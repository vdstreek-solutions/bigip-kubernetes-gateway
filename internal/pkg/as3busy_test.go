package pkg

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/f5devcentral/f5-bigip-rest-go/utils"
)

// What HandleRequest returns when another AS3 deploy is running on the BIG-IP, built the same way
// the library builds it (assertBigipResp20X's RetryErrorf, re-wrapped by HandleRequest with %s).
var errAS3Busy = fmt.Errorf("failed to do deployment to https://10.193.253.51:443: %s",
	utils.RetryErrorf("%d, %s", 503,
		`{"code":503,"message":"Error: Configuration operation in progress on device localhost, please try again in 2 minutes"}`).Error())

var quickBackoff = backoff{initial: time.Millisecond, max: 4 * time.Millisecond, budget: 200 * time.Millisecond}

func Test_deployWhenAS3Free_waitsOutBusy(t *testing.T) {
	calls := 0
	err := deployWhenAS3Free(context.Background(), quickBackoff, func() error {
		calls++
		if calls < 3 {
			return errAS3Busy
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("want success on 3rd call, got err=%v after %d calls", err, calls)
	}
}

func Test_deployWhenAS3Free_otherErrorsNotRetried(t *testing.T) {
	calls := 0
	invalid := errors.New("failed to do deployment to https://10.193.253.51: 422, declaration is invalid")
	err := deployWhenAS3Free(context.Background(), quickBackoff, func() error {
		calls++
		return invalid
	})
	if err != invalid || calls != 1 {
		t.Fatalf("want the 422 back after 1 call, got err=%v after %d calls", err, calls)
	}
}

func Test_deployWhenAS3Free_givesUpAfterBudget(t *testing.T) {
	calls := 0
	err := deployWhenAS3Free(context.Background(), quickBackoff, func() error {
		calls++
		return errAS3Busy
	})
	if !isAS3Busy(err) || calls < 2 {
		t.Fatalf("want a wrapped busy error after several calls, got err=%v after %d calls", err, calls)
	}
}

func Test_deployWhenAS3Free_stopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	slow := backoff{initial: time.Hour, max: time.Hour, budget: 10 * time.Hour}
	err := deployWhenAS3Free(ctx, slow, func() error {
		calls++
		return errAS3Busy
	})
	if !isAS3Busy(err) || calls != 1 {
		t.Fatalf("want to stop after 1 call on a cancelled context, got err=%v after %d calls", err, calls)
	}
}
