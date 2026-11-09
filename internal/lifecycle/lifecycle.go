// Package lifecycle is the part of a service that decides when it is ready
// for traffic and when it has finished with the traffic it has.
//
// A rolling deploy drops requests for one reason above all others, and it
// is not a slow shutdown. When a pod is deleted, two things happen at the
// same moment and neither waits for the other: the kubelet sends SIGTERM,
// and the endpoints controller begins removing the pod from the Service.
// The second one is propagated to every kube-proxy and every ingress in the
// cluster, which takes a moment, and during that moment the pod is still
// being sent new connections.
//
// A service that exits promptly on SIGTERM is therefore refusing requests
// that were routed to it correctly. The fix is counter-intuitive: on
// SIGTERM, fail the readiness probe and then keep serving for a few seconds
// before shutting anything down.
package lifecycle

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// State is what the probes report.
type State int32

const (
	// Starting means the process is up and not yet ready. Liveness passes,
	// readiness fails.
	Starting State = iota
	// Serving is the normal state.
	Serving
	// Draining means SIGTERM arrived. Readiness fails so the pod is taken
	// out of the Service, and the server keeps answering.
	Draining
	// Stopped means the server has shut down.
	Stopped
)

func (s State) String() string {
	switch s {
	case Starting:
		return "starting"
	case Serving:
		return "serving"
	case Draining:
		return "draining"
	default:
		return "stopped"
	}
}

// Timings are the three numbers a rollout turns on.
type Timings struct {
	// Drain is how long to keep serving after readiness starts failing.
	//
	// It has to cover the time for the endpoint removal to reach every
	// kube-proxy and every ingress, which is a cluster property rather
	// than an application one. Five seconds is the usual starting point
	// and the right value is measured, not guessed: it is the thing to
	// raise when a rollout still shows 502s.
	Drain time.Duration

	// Shutdown is how long in-flight requests get to finish once the
	// listener is closed.
	//
	// It must be comfortably shorter than terminationGracePeriodSeconds,
	// because when that expires the kubelet sends SIGKILL and whatever is
	// still running is cut off mid-response. Drain plus Shutdown plus a
	// margin is the grace period the manifest needs.
	Shutdown time.Duration

	// ReadyAfter is how long startup work is allowed to take before the
	// startup probe gives up.
	ReadyAfter time.Duration
}

func DefaultTimings() Timings {
	return Timings{Drain: 5 * time.Second, Shutdown: 20 * time.Second, ReadyAfter: 30 * time.Second}
}

// GracePeriod is what terminationGracePeriodSeconds has to be, at least.
//
// Worth computing rather than writing a number in the manifest and hoping
// it is still right: the two drift, and the symptom of them drifting is
// requests cut off during a deploy.
func (t Timings) GracePeriod() time.Duration {
	return t.Drain + t.Shutdown + 5*time.Second
}

// Lifecycle tracks the state and the in-flight requests.
type Lifecycle struct {
	state    atomic.Int32
	inFlight atomic.Int64
	Timings  Timings

	// Checks are the dependencies readiness depends on, by name.
	//
	// Readiness and liveness take different ones, and conflating them is
	// the second commonest way to turn a dependency outage into an
	// outage: if liveness checks the database, then the database going
	// away restarts every pod, which loses all the warm state and does
	// not bring the database back.
	mu     sync.RWMutex
	checks map[string]Check

	onDrain []func()
}

// Check reports whether a dependency is usable.
type Check func(context.Context) error

func New(t Timings) *Lifecycle {
	l := &Lifecycle{Timings: t, checks: map[string]Check{}}
	l.state.Store(int32(Starting))
	return l
}

func (l *Lifecycle) State() State { return State(l.state.Load()) }

// Ready marks the service as able to take traffic.
func (l *Lifecycle) Ready() {
	l.state.CompareAndSwap(int32(Starting), int32(Serving))
}

// AddCheck registers a readiness dependency.
func (l *Lifecycle) AddCheck(name string, c Check) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.checks[name] = c
}

// OnDrain registers something to run when draining begins, such as
// stopping a queue consumer so it takes no new work while the HTTP side
// finishes what it has.
func (l *Lifecycle) OnDrain(f func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onDrain = append(l.onDrain, f)
}

// Begin records a request starting, and returns the function to call when
// it finishes.
func (l *Lifecycle) Begin() func() {
	l.inFlight.Add(1)
	var once sync.Once
	return func() { once.Do(func() { l.inFlight.Add(-1) }) }
}

func (l *Lifecycle) InFlight() int64 { return l.inFlight.Load() }

// Drain moves to draining and runs the registered hooks.
func (l *Lifecycle) Drain() {
	if !l.state.CompareAndSwap(int32(Serving), int32(Draining)) &&
		!l.state.CompareAndSwap(int32(Starting), int32(Draining)) {
		return
	}
	l.mu.RLock()
	hooks := make([]func(), len(l.onDrain))
	copy(hooks, l.onDrain)
	l.mu.RUnlock()
	for _, f := range hooks {
		f()
	}
}

func (l *Lifecycle) Stop() { l.state.Store(int32(Stopped)) }

// Live reports whether the process should keep running.
//
// It checks nothing but the process itself. Liveness answers "is this
// stuck", and the only correct response to it failing is a restart, so a
// dependency must never appear here: a database outage would then restart
// every pod in a loop, which does not help the database and loses
// everything warm.
//
// A draining pod is still live. Failing liveness during a drain asks the
// kubelet to kill it while it is deliberately finishing work.
func (l *Lifecycle) Live(context.Context) error { return nil }

// Ready reports whether this pod should receive traffic.
type NotReadyError struct {
	State  State
	Reason string
}

func (e NotReadyError) Error() string {
	if e.Reason == "" {
		return "not ready: " + e.State.String()
	}
	return "not ready: " + e.Reason
}

// Readiness runs the dependency checks, unless the pod is starting or
// draining, in which case the answer is already no.
func (l *Lifecycle) Readiness(ctx context.Context) error {
	switch s := l.State(); s {
	case Starting:
		return NotReadyError{State: s}
	case Draining, Stopped:
		// The important one. Failing readiness here is what takes this pod
		// out of the Service, and it has to happen before the server stops
		// answering, not after.
		return NotReadyError{State: s}
	}

	l.mu.RLock()
	checks := make(map[string]Check, len(l.checks))
	for k, v := range l.checks {
		checks[k] = v
	}
	l.mu.RUnlock()

	for name, c := range checks {
		if err := c(ctx); err != nil {
			return NotReadyError{State: Serving, Reason: name + ": " + err.Error()}
		}
	}
	return nil
}
