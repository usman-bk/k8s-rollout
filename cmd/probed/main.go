// Command probed is a service that survives a rolling deploy.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/usmanvirk/k8s-rollout/internal/lifecycle"
	"github.com/usmanvirk/k8s-rollout/internal/probes"
)

func main() {
	var (
		addr     = flag.String("addr", ":8080", "listen address")
		drain    = flag.Duration("drain", envDur("DRAIN", 5*time.Second), "how long to keep serving after the signal, while the endpoint removal propagates")
		shutdown = flag.Duration("shutdown", envDur("SHUTDOWN", 20*time.Second), "how long in flight requests get once the listener closes")
		startup  = flag.Duration("startup-work", 0, "simulated startup work, to watch the startup probe")
		depDown  = flag.Bool("dependency-down", false, "make the readiness dependency fail")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	t := lifecycle.Timings{Drain: *drain, Shutdown: *shutdown, ReadyAfter: 30 * time.Second}
	l := lifecycle.New(t)

	// Printed at startup so the number the manifest needs is not something
	// anybody has to work out. There is a test comparing the two, and this
	// is the same number from the other side.
	log.Info("lifecycle",
		"drain", t.Drain.String(), "shutdown", t.Shutdown.String(),
		"terminationGracePeriodSeconds_needed", int(t.GracePeriod().Seconds()))

	// A readiness dependency. Liveness never sees it.
	l.AddCheck("database", func(ctx context.Context) error {
		if *depDown {
			return errors.New("connection refused")
		}
		return nil
	})

	// Something that stops taking new work when the drain starts, while
	// the HTTP side finishes what it already has.
	l.OnDrain(func() { log.Info("queue consumer stopped taking new work") })

	mux := http.NewServeMux()
	mux.Handle("/healthz", probes.Handler(l))
	mux.Handle("/readyz", probes.Handler(l))
	mux.Handle("/startupz", probes.Handler(l))
	mux.HandleFunc("GET /work", func(w http.ResponseWriter, r *http.Request) {
		// Slow enough to still be running when a shutdown begins, which
		// is the case worth being able to produce by hand.
		d := 200 * time.Millisecond
		if v := r.URL.Query().Get("ms"); v != "" {
			if ms, err := time.ParseDuration(v + "ms"); err == nil {
				d = ms
			}
		}
		select {
		case <-time.After(d):
			fmt.Fprintf(w, "worked for %s\n", d)
		case <-r.Context().Done():
		}
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           lifecycle.Track(l, mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Longer than the slowest handler, or the server cuts a response
		// off before the shutdown ever gets a chance to drain it.
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if *startup > 0 {
			log.Info("starting up", "work", startup.String())
			time.Sleep(*startup)
		}
		l.Ready()
		log.Info("ready")
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := lifecycle.Serve(ctx, srv, l, log); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func envDur(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
