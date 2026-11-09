package lifecycle_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/usmanvirk/k8s-rollout/internal/lifecycle"
	"github.com/usmanvirk/k8s-rollout/internal/probes"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func fast() lifecycle.Timings {
	// The same shape as production, scaled down so a test is a test and
	// not a wait.
	return lifecycle.Timings{
		Drain:      300 * time.Millisecond,
		Shutdown:   2 * time.Second,
		ReadyAfter: time.Second,
	}
}

func TestStateMachine(t *testing.T) {
	l := lifecycle.New(fast())
	if l.State() != lifecycle.Starting {
		t.Fatalf("a new lifecycle is %s", l.State())
	}
	if err := l.Readiness(context.Background()); err == nil {
		t.Fatal("a starting pod reported ready")
	}

	l.Ready()
	if l.State() != lifecycle.Serving {
		t.Fatalf("after Ready the state is %s", l.State())
	}
	if err := l.Readiness(context.Background()); err != nil {
		t.Fatalf("a serving pod with no checks is not ready: %v", err)
	}

	l.Drain()
	if l.State() != lifecycle.Draining {
		t.Fatalf("after Drain the state is %s", l.State())
	}
	if err := l.Readiness(context.Background()); err == nil {
		t.Fatal("a draining pod still reports ready, so it stays in the Service")
	}
}

// TestADrainingPodIsStillLive. Failing liveness while draining asks the
// kubelet to kill a pod that is deliberately finishing work.
func TestADrainingPodIsStillLive(t *testing.T) {
	l := lifecycle.New(fast())
	l.Ready()
	l.Drain()
	if err := l.Live(context.Background()); err != nil {
		t.Fatalf("a draining pod reported not live, which asks for a SIGKILL mid request: %v", err)
	}
}

// TestLivenessIgnoresDependencies is the rule that stops a database
// outage becoming a restart loop across every pod.
func TestLivenessIgnoresDependencies(t *testing.T) {
	l := lifecycle.New(fast())
	l.Ready()
	l.AddCheck("database", func(context.Context) error { return errors.New("down") })

	if err := l.Live(context.Background()); err != nil {
		t.Fatalf("liveness failed because a dependency is down: %v", err)
	}
	if err := l.Readiness(context.Background()); err == nil {
		t.Fatal("readiness passed with the database down")
	}
}

func TestReadinessNamesTheFailingDependency(t *testing.T) {
	l := lifecycle.New(fast())
	l.Ready()
	l.AddCheck("postgres", func(context.Context) error { return errors.New("connection refused") })

	err := l.Readiness(context.Background())
	if err == nil {
		t.Fatal("no error")
	}
	// A probe that says only "not ready" sends somebody to the logs. One
	// that names the dependency does not.
	if msg := err.Error(); !strings.Contains(msg, "postgres") || !strings.Contains(msg, "connection refused") {
		t.Fatalf("the reason is %q", msg)
	}
}

func TestDrainHooksRunOnce(t *testing.T) {
	l := lifecycle.New(fast())
	l.Ready()

	var n atomic.Int64
	l.OnDrain(func() { n.Add(1) })
	l.OnDrain(func() { n.Add(10) })

	l.Drain()
	l.Drain() // a second signal, which happens
	if got := n.Load(); got != 11 {
		t.Fatalf("hooks ran to %d, want 11 exactly once", got)
	}
}

func TestGracePeriodCoversTheWholeSequence(t *testing.T) {
	ti := lifecycle.Timings{Drain: 5 * time.Second, Shutdown: 20 * time.Second}
	got := ti.GracePeriod()
	if got < ti.Drain+ti.Shutdown {
		t.Fatalf("the grace period %s is shorter than draining plus shutdown, so SIGKILL arrives mid request", got)
	}
}

func TestInFlightCounting(t *testing.T) {
	l := lifecycle.New(fast())
	done1 := l.Begin()
	done2 := l.Begin()
	if l.InFlight() != 2 {
		t.Fatalf("in flight is %d", l.InFlight())
	}
	done1()
	done1() // a double call, which a deferred wrapper can produce
	if l.InFlight() != 1 {
		t.Fatalf("in flight is %d after one request finished twice", l.InFlight())
	}
	done2()
	if l.InFlight() != 0 {
		t.Fatalf("in flight is %d", l.InFlight())
	}
}

// listen returns a server on a real port, because the behaviour under test
// is about connections and a httptest recorder has none.
func listen(t *testing.T, l *lifecycle.Lifecycle, handler http.Handler) (*http.Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: lifecycle.Track(l, handler)}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return srv, "http://" + ln.Addr().String()
}

// TestTrafficArrivingDuringTheDrainIsStillServed is the whole package.
//
// When a pod is deleted, SIGTERM and the endpoint removal happen at the
// same moment and neither waits for the other. For a few seconds the pod
// is still being sent new connections by kube-proxies that have not heard
// yet. A service that exits promptly on SIGTERM refuses them, and that is
// what 502s during a rollout are.
func TestTrafficArrivingDuringTheDrainIsStillServed(t *testing.T) {
	l := lifecycle.New(fast())
	l.Ready()

	var served atomic.Int64
	_, base := listen(t, l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	// The signal arrives.
	l.Drain()

	// Readiness now fails, which is what removes the pod from the Service.
	if err := l.Readiness(context.Background()); err == nil {
		t.Fatal("readiness still passes after the signal")
	}

	// And requests keep being answered, because the removal has not
	// reached everybody yet.
	client := &http.Client{Timeout: time.Second}
	for i := 0; i < 20; i++ {
		resp, err := client.Get(base + "/")
		if err != nil {
			t.Fatalf("request %d during the drain failed: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d during the drain got %d", i, resp.StatusCode)
		}
	}
	if served.Load() != 20 {
		t.Fatalf("%d requests were served during the drain, want 20", served.Load())
	}
}

// TestServeDrainsBeforeClosingTheListener checks the ordering end to end:
// readiness must fail before the listener closes, not after.
func TestServeFailsReadinessBeforeClosingTheListener(t *testing.T) {
	l := lifecycle.New(fast())
	l.Ready()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()

	mux := http.NewServeMux()
	mux.Handle("/", probes.Handler(l))
	srv := &http.Server{Handler: lifecycle.Track(l, mux)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// Serve expects to own the listener, so mimic what it does around
		// an already open one.
		go func() { _ = srv.Serve(ln) }()
		<-ctx.Done()
		l.Drain()
		time.Sleep(l.Timings.Drain)
		shutCtx, c := context.WithTimeout(context.Background(), l.Timings.Shutdown)
		defer c()
		done <- srv.Shutdown(shutCtx)
	}()

	client := &http.Client{Timeout: time.Second}

	resp, err := client.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz before the signal is %d", resp.StatusCode)
	}

	cancel()
	time.Sleep(50 * time.Millisecond) // inside the drain window

	resp, err = client.Get(base + "/readyz")
	if err != nil {
		t.Fatalf("readyz during the drain was refused, so the listener closed too early: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz during the drain is %d, want 503 so the pod leaves the Service", resp.StatusCode)
	}

	if err := <-done; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestAnInFlightRequestFinishes. A long request started before the
// shutdown has to complete, not be cut off.
func TestAnInFlightRequestFinishes(t *testing.T) {
	l := lifecycle.New(fast())
	l.Ready()

	release := make(chan struct{})
	finished := make(chan struct{})
	srv, base := listen(t, l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("done"))
		close(finished)
	}))

	body := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(base + "/slow")
		if err != nil {
			errCh <- err
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		body <- string(b)
	}()

	// Wait for it to be in the handler.
	deadline := time.Now().Add(2 * time.Second)
	for l.InFlight() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if l.InFlight() == 0 {
		t.Fatal("the request never reached the handler")
	}

	shutDone := make(chan error, 1)
	go func() {
		l.Drain()
		shutCtx, cancel := context.WithTimeout(context.Background(), l.Timings.Shutdown)
		defer cancel()
		shutDone <- srv.Shutdown(shutCtx)
	}()

	time.Sleep(100 * time.Millisecond)
	close(release)

	select {
	case got := <-body:
		if got != "done" {
			t.Fatalf("the in flight request returned %q", got)
		}
	case err := <-errCh:
		t.Fatalf("the in flight request was cut off: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the in flight request never finished")
	}

	if err := <-shutDone; err != nil {
		t.Fatalf("shutdown reported %v", err)
	}
	<-finished
}

// TestNoRequestIsDroppedThroughAWholeRollout is the closest this gets to
// the real thing: constant traffic, and the pod is drained underneath it.
func TestNoRequestIsDroppedThroughAWholeRollout(t *testing.T) {
	l := lifecycle.New(fast())
	l.Ready()

	srv, base := listen(t, l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	var ok, failed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Traffic, as a load balancer that has not yet heard would send it.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 2 * time.Second}
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := client.Get(base + "/")
				if err != nil {
					failed.Add(1)
					return
				}
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					ok.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	// The rollout: fail readiness, keep serving, then close.
	l.Drain()
	time.Sleep(l.Timings.Drain)
	close(stop)
	wg.Wait()

	// The client's transport can leave a dialled connection that never carried
	// a request. Shutdown waits five seconds on a new connection like that.
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	shutCtx, cancel := context.WithTimeout(context.Background(), l.Timings.Shutdown)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if failed.Load() != 0 {
		t.Fatalf("%d of %d requests failed during the rollout", failed.Load(), ok.Load()+failed.Load())
	}
	if ok.Load() < 50 {
		t.Fatalf("only %d requests were served, so this did not exercise much", ok.Load())
	}
	t.Logf("%d requests served across the drain, none dropped", ok.Load())
}
