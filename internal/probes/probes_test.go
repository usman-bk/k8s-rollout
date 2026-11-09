package probes_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/usmanvirk/k8s-rollout/internal/lifecycle"
	"github.com/usmanvirk/k8s-rollout/internal/probes"
)

func get(t *testing.T, mux *http.ServeMux, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("the probe at %s returned %q, which is not json: %v", path, rec.Body, err)
	}
	return rec, body
}

func newLifecycle() *lifecycle.Lifecycle {
	return lifecycle.New(lifecycle.Timings{
		Drain: time.Second, Shutdown: 2 * time.Second, ReadyAfter: time.Second,
	})
}

func TestStartingPod(t *testing.T) {
	l := newLifecycle()
	mux := probes.Handler(l)

	if rec, _ := get(t, mux, "/healthz"); rec.Code != 200 {
		t.Errorf("liveness during startup is %d, and a restart would not help", rec.Code)
	}
	if rec, _ := get(t, mux, "/readyz"); rec.Code != 503 {
		t.Errorf("readiness during startup is %d, so traffic arrives before the pod can serve it", rec.Code)
	}
	if rec, _ := get(t, mux, "/startupz"); rec.Code != 503 {
		t.Errorf("startup probe during startup is %d", rec.Code)
	}
}

func TestServingPod(t *testing.T) {
	l := newLifecycle()
	l.Ready()
	mux := probes.Handler(l)

	for _, path := range []string{"/healthz", "/readyz", "/startupz"} {
		rec, body := get(t, mux, path)
		if rec.Code != 200 {
			t.Errorf("%s is %d on a serving pod", path, rec.Code)
		}
		if body["state"] != "serving" {
			t.Errorf("%s reports state %v", path, body["state"])
		}
	}
}

// TestDrainingPodIsNotReadyAndStillLive is the combination a rollout
// depends on.
func TestDrainingPodIsNotReadyAndStillLive(t *testing.T) {
	l := newLifecycle()
	l.Ready()
	l.Drain()
	mux := probes.Handler(l)

	if rec, _ := get(t, mux, "/readyz"); rec.Code != 503 {
		t.Errorf("a draining pod reports ready %d, so it stays in the Service and keeps getting new traffic forever", rec.Code)
	}
	if rec, _ := get(t, mux, "/healthz"); rec.Code != 200 {
		t.Errorf("a draining pod reports not live %d, which asks the kubelet to kill it mid request", rec.Code)
	}
}

func TestAFailingDependencyAffectsReadinessOnly(t *testing.T) {
	l := newLifecycle()
	l.Ready()
	l.AddCheck("postgres", func(context.Context) error { return errors.New("connection refused") })
	mux := probes.Handler(l)

	rec, body := get(t, mux, "/readyz")
	if rec.Code != 503 {
		t.Fatalf("readiness is %d with the database down", rec.Code)
	}
	reason, _ := body["reason"].(string)
	if reason == "" {
		t.Fatal("the probe does not say which dependency failed")
	}

	// And liveness is untouched, which is what stops a database outage
	// restarting every pod in the cluster.
	if rec, _ := get(t, mux, "/healthz"); rec.Code != 200 {
		t.Fatalf("liveness is %d because a dependency is down, which would restart every pod", rec.Code)
	}
}

// TestProbesAreNotCacheable. Anything caching a probe response reports a
// pod as ready long after it stopped being so.
func TestProbesAreNotCacheable(t *testing.T) {
	l := newLifecycle()
	l.Ready()
	mux := probes.Handler(l)

	for _, path := range []string{"/healthz", "/readyz", "/startupz"} {
		rec, _ := get(t, mux, path)
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s does not forbid caching: %q", path, rec.Header().Get("Cache-Control"))
		}
	}
}

func TestProbesReportInFlight(t *testing.T) {
	l := newLifecycle()
	l.Ready()
	done := l.Begin()
	defer done()

	_, body := get(t, probes.Handler(l), "/readyz")
	if n, _ := body["in_flight"].(float64); n != 1 {
		t.Fatalf("in_flight is %v, and the number is what tells an operator whether a slow shutdown was one request or a thousand", body["in_flight"])
	}
}

// TestASlowDependencyDoesNotHangTheProbe. The kubelet has its own timeout,
// and hitting it gives a failure with no reason attached.
func TestASlowDependencyDoesNotHangTheProbe(t *testing.T) {
	l := newLifecycle()
	l.Ready()
	l.AddCheck("slow", func(ctx context.Context) error {
		select {
		case <-time.After(30 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	start := time.Now()
	rec, body := get(t, probes.Handler(l), "/readyz")
	elapsed := time.Since(start)

	if elapsed > probes.Timeout+time.Second {
		t.Fatalf("the probe took %s", elapsed)
	}
	if rec.Code != 503 {
		t.Fatalf("a probe that timed out returned %d", rec.Code)
	}
	if reason, _ := body["reason"].(string); reason == "" {
		t.Fatal("the timed out probe gives no reason")
	}
}
