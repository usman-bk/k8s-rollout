// Package probes serves the three endpoints the kubelet calls.
package probes

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/usmanvirk/k8s-rollout/internal/lifecycle"
)

// Timeout bounds a probe.
//
// Shorter than the probe's own timeoutSeconds in the manifest, so a slow
// dependency produces a clean "not ready" with a reason rather than the
// kubelet timing out with nothing to log.
const Timeout = 2 * time.Second

type body struct {
	Status   string `json:"status"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
	InFlight int64  `json:"in_flight"`
}

// Handler returns the probe endpoints.
//
// Three separate ones, because they answer three different questions and
// the kubelet does different things with the answers.
func Handler(l *lifecycle.Lifecycle) *http.ServeMux {
	mux := http.NewServeMux()

	// Liveness: is this process stuck? The only response to it failing is
	// a restart, so it checks nothing but the process. A dependency here
	// turns a database outage into a restart loop across every pod.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), Timeout)
		defer cancel()
		if err := l.Live(ctx); err != nil {
			write(w, http.StatusServiceUnavailable, l, err.Error())
			return
		}
		write(w, http.StatusOK, l, "")
	})

	// Readiness: should this pod be in the Service? This is the one that
	// matters during a rollout.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), Timeout)
		defer cancel()
		if err := l.Readiness(ctx); err != nil {
			write(w, http.StatusServiceUnavailable, l, err.Error())
			return
		}
		write(w, http.StatusOK, l, "")
	})

	// Startup: has the process finished starting? Its job is to let a slow
	// start take as long as it needs without forcing the liveness probe to
	// have a long initialDelaySeconds, which would also delay detecting a
	// hang for the whole life of the pod.
	mux.HandleFunc("GET /startupz", func(w http.ResponseWriter, r *http.Request) {
		if l.State() == lifecycle.Starting {
			write(w, http.StatusServiceUnavailable, l, "still starting")
			return
		}
		write(w, http.StatusOK, l, "")
	})

	return mux
}

func write(w http.ResponseWriter, code int, l *lifecycle.Lifecycle, reason string) {
	status := "ok"
	if code != http.StatusOK {
		status = "unavailable"
	}
	w.Header().Set("Content-Type", "application/json")
	// A probe response must never be cached by anything between the
	// kubelet and the pod, or a pod reports ready long after it stopped
	// being so.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body{
		Status: status, State: l.State().String(),
		Reason: reason, InFlight: l.InFlight(),
	})
}
