package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Serve runs a server through the whole lifecycle and returns when it has
// finished draining.
//
// The order of what happens on SIGTERM is the entire point of this
// package, and it is not the order anybody writes first:
//
//  1. Fail readiness. The endpoints controller starts removing this pod
//     from the Service.
//  2. Keep serving for Drain. The removal has not reached every kube-proxy
//     and every ingress yet, so new connections are still arriving and
//     they are arriving correctly.
//  3. Stop the listener and let in-flight requests finish, up to Shutdown.
//  4. Exit.
//
// Doing (3) first is what drops requests during a rollout, and it is what
// a plain signal.NotifyContext and srv.Shutdown gives you.
func Serve(ctx context.Context, srv *http.Server, l *Lifecycle, log *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr,
			"drain", l.Timings.Drain.String(),
			"shutdown", l.Timings.Shutdown.String(),
			"grace_period_needed", l.Timings.GracePeriod().String())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// 1 and 2.
	log.Info("signal received, failing readiness and continuing to serve",
		"drain", l.Timings.Drain.String(), "in_flight", l.InFlight())
	l.Drain()

	drain := time.NewTimer(l.Timings.Drain)
	defer drain.Stop()
	<-drain.C

	// 3.
	log.Info("draining finished, closing the listener", "in_flight", l.InFlight())
	shutCtx, cancel := context.WithTimeout(context.Background(), l.Timings.Shutdown)
	defer cancel()

	err := srv.Shutdown(shutCtx)
	l.Stop()

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		// Worth saying loudly. Requests were cut off, which means either
		// Shutdown is too short or something is holding a connection open
		// that should not be.
		log.Error("shutdown timed out with requests still running",
			"in_flight", l.InFlight(), "shutdown", l.Timings.Shutdown.String())
	case err != nil:
		log.Error("shutdown", "err", err)
	default:
		log.Info("stopped cleanly", "in_flight", l.InFlight())
	}
	return err
}

// Track wraps a handler so in-flight requests are counted.
//
// Counted for the logs and the tests rather than for correctness, since
// http.Server.Shutdown does the waiting. The number is what tells an
// operator whether a shutdown timeout was one slow request or a thousand.
func Track(l *Lifecycle, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done := l.Begin()
		defer done()
		next.ServeHTTP(w, r)
	})
}
