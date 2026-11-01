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

	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/f5devcentral/bigip-kubernetes-gateway/internal/pkg"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
)

type GatewayReconciler struct {
	ObjectType client.Object
	Client     client.Client
	// LogLevel   string
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Adc object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *GatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lctx := pkg.NewContext()
	slog := utils.LogFromContext(lctx)
	if !pkg.ActiveSIGs.SyncedAtStart {
		<-time.After(100 * time.Millisecond)
		return ctrl.Result{Requeue: true}, nil
	}
	var obj gatewayapi.Gateway

	slog.Debugf("handling " + req.NamespacedName.String())
	if err := r.Client.Get(ctx, req.NamespacedName, &obj); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// delete resources
			gw := pkg.ActiveSIGs.GetGateway(req.NamespacedName.String())
			cls := string(gw.Spec.GatewayClassName)
			pkg.ActiveSIGs.UnsetGateway(req.NamespacedName.String())
			if err := pkg.DeployForEvent(lctx, []string{cls}); err != nil {
				return ctrl.Result{}, err
			} else {
				return ctrl.Result{}, nil
			}
		} else {
			return ctrl.Result{}, err
		}
	} else {
		key := req.NamespacedName.String()
		// This controller's own Reconcile fires for EVERY Gateway object cluster-wide, not just
		// ones using our GatewayClass (there's no field selector on the watch) -- confirmed live:
		// without this check, the address-auto-assignment block below ran for Cilium's unrelated
		// "private"/"public" Gateways too (they also leave spec.addresses empty, relying on their
		// own LoadBalancer-based assignment instead), repeatedly overwriting their
		// status.addresses with an address from OUR pool. Cilium's own controller kept correcting
		// it back, and each correction is itself a status change that re-triggers our Reconcile --
		// a self-sustaining fight, thousands of reconciles/sec, on a shared Gateway that serves
		// many unrelated production apps. Skip entirely for a GatewayClass we don't control.
		gwc := pkg.ActiveSIGs.GetGatewayClass(string(obj.Spec.GatewayClassName))
		weOwnThisClass := gwc != nil && gwc.Spec.ControllerName == gatewayapi.GatewayController(pkg.ActiveSIGs.ControllerName)

		// No spec.addresses is a valid, documented Gateway API pattern (see
		// GatewayStatus.Addresses' own doc comment: "no addresses are specified,
		// all addresses are dynamically assigned") -- this project never
		// implemented that half, hence this block. Only acts when
		// --address-pool-cidr is actually configured; otherwise behavior is
		// unchanged from before (no addresses -> no Virtual Server, see parser.go).
		if weOwnThisClass && len(obj.Spec.Addresses) == 0 {
			// already assigned in a previous reconcile (or before a controller restart) --
			// re-register rather than reallocate, so restarts don't change a Gateway's address
			// out from under it. Falls through to a fresh allocation below if the existing
			// address is no longer within the configured range (RegisterExistingAddress
			// returns false in that case) -- see its doc comment.
			hasValidExisting := len(obj.Status.Addresses) > 0 &&
				pkg.RegisterExistingAddress(key, obj.Status.Addresses[0].Value)
			if !hasValidExisting && pkg.AddressPoolConfigured() {
				ip, err := pkg.AllocateAddress(key)
				if err != nil {
					slog.Errorf("failed to allocate address for %s: %s", key, err.Error())
					return ctrl.Result{}, err
				}
				addrType := gatewayapi.IPAddressType
				if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
					var latest gatewayapi.Gateway
					if err := r.Client.Get(ctx, req.NamespacedName, &latest); err != nil {
						return err
					}
					latest.Status.Addresses = []gatewayapi.GatewayStatusAddress{
						{Type: &addrType, Value: ip},
					}
					if err := r.Client.Status().Update(ctx, &latest); err != nil {
						return err
					}
					obj.Status.Addresses = latest.Status.Addresses
					return nil
				}); err != nil {
					slog.Errorf("failed to write allocated address %s to %s status: %s", ip, key, err.Error())
					return ctrl.Result{}, err
				}
			}
		}

		// upsert resources
		pkg.ActiveSIGs.SetGateway(&obj)

		gw := pkg.ActiveSIGs.GetGateway(req.NamespacedName.String())
		cls := []string{string(obj.Spec.GatewayClassName)}
		if gw != nil {
			cls = append(cls, string(gw.Spec.GatewayClassName))
		}
		cls = utils.Unified(cls)
		if err := pkg.DeployForEvent(lctx, cls); err != nil {
			return ctrl.Result{}, err
		} else {
			return ctrl.Result{}, nil
		}
	}
}

func (r *GatewayReconciler) GetResObject() client.Object {
	return r.ObjectType
}
