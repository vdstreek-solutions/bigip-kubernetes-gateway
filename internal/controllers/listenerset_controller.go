/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"time"

	"github.com/f5devcentral/bigip-kubernetes-gateway/internal/pkg"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
)

// ListenerSetReconciler mirrors HttpRouteReconciler's shape: a ListenerSet contributes no
// resources of its own to AS3 directly, it only ever changes what a Gateway's OWN
// parseGateway sees once merged in (see cache.go's AttachedListenerSets and parser.go's
// listener-merging) -- so, same as HTTPRoute, all this Reconcile does is keep the cache current
// and trigger a redeploy for whichever GatewayClass the ListenerSet's parent Gateway belongs to.
type ListenerSetReconciler struct {
	ObjectType client.Object
	Client     client.Client
}

func (r *ListenerSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lctx := pkg.NewContext()
	slog := utils.LogFromContext(lctx)
	if !pkg.ActiveSIGs.SyncedAtStart {
		<-time.After(100 * time.Millisecond)
		return ctrl.Result{Requeue: true}, nil
	}

	var obj gatewayapi.ListenerSet

	slog.Debugf("handling " + req.NamespacedName.String())
	if err := r.Client.Get(ctx, req.NamespacedName, &obj); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// delete resources
			ls := pkg.ActiveSIGs.GetListenerSet(req.NamespacedName.String())
			cls := []string{}
			if gw := pkg.ActiveSIGs.GatewayRefOfListenerSet(ls); gw != nil {
				cls = append(cls, string(gw.Spec.GatewayClassName))
			}
			pkg.ActiveSIGs.UnsetListenerSet(req.NamespacedName.String())
			return ctrl.Result{}, pkg.DeployForEvent(lctx, cls)
		}
		return ctrl.Result{}, err
	}

	// upsert resources -- collect the class from both the old and new parentRef, same reasoning
	// as HttpRouteReconciler: if a ListenerSet's parentRef itself changed (moved to point at a
	// different Gateway), both the old and new parent Gateway's class need a redeploy, not just
	// whichever one it ends up pointing at now.
	nls := obj.DeepCopy()
	ols := pkg.ActiveSIGs.GetListenerSet(req.NamespacedName.String())
	cls := []string{}
	if gw := pkg.ActiveSIGs.GatewayRefOfListenerSet(ols); gw != nil {
		cls = append(cls, string(gw.Spec.GatewayClassName))
	}
	if gw := pkg.ActiveSIGs.GatewayRefOfListenerSet(nls); gw != nil {
		cls = append(cls, string(gw.Spec.GatewayClassName))
	}
	cls = utils.Unified(cls)
	pkg.ActiveSIGs.SetListenerSet(&obj)
	return ctrl.Result{}, pkg.DeployForEvent(lctx, cls)
}

func (r *ListenerSetReconciler) GetResObject() client.Object {
	return r.ObjectType
}
