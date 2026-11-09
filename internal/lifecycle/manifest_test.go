package lifecycle_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/usmanvirk/k8s-rollout/internal/lifecycle"
)

// The manifest and the code have to agree, and nothing makes them. The
// usual way this breaks is that somebody shortens the grace period, or
// lengthens the drain, and the two are in different files reviewed by
// different people. The symptom is requests cut off mid response during a
// deploy, weeks later, with nothing in the diff that looks related.
//
// Parsed with a regular expression rather than a YAML library, because
// pulling in a dependency to read four numbers out of one file is a worse
// trade than a regex with a test behind it.
func manifest(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "deploy", "deployment.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(b)
}

func field(t *testing.T, src, name string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(name) + `:\s*"?([^"\s#]+)"?`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("%s is not in the manifest", name)
	}
	return m[1]
}

// envDuration reads one of the container's environment values. They are
// written as inline mappings, so a line anchored match for "value:" finds
// nothing and a match for the wrong one finds the first.
func envDuration(t *testing.T, src, name string) time.Duration {
	t.Helper()
	re := regexp.MustCompile(`name:\s*` + regexp.QuoteMeta(name) + `\s*,\s*value:\s*"([^"]+)"`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("%s is not in the manifest", name)
	}
	d, err := time.ParseDuration(m[1])
	if err != nil {
		t.Fatalf("%s is %q, which is not a duration: %v", name, m[1], err)
	}
	return d
}

func TestTheGracePeriodCoversTheApplicationsSequence(t *testing.T) {
	src := manifest(t)

	grace, err := strconv.Atoi(field(t, src, "terminationGracePeriodSeconds"))
	if err != nil {
		t.Fatal(err)
	}
	drain := envDuration(t, src, "DRAIN")
	shutdown := envDuration(t, src, "SHUTDOWN")

	ti := lifecycle.Timings{Drain: drain, Shutdown: shutdown}
	needed := ti.GracePeriod()

	if time.Duration(grace)*time.Second < needed {
		t.Fatalf("terminationGracePeriodSeconds is %ds, and the application needs %s "+
			"(a %s drain plus a %s shutdown plus a margin). SIGKILL would arrive mid request.",
			grace, needed, drain, shutdown)
	}
}

// TestMaxUnavailableIsZero. With anything above zero a rollout removes
// capacity before it adds any, which on three replicas is losing a third
// of the fleet during every deploy.
func TestMaxUnavailableIsZero(t *testing.T) {
	if got := field(t, manifest(t), "maxUnavailable"); got != "0" {
		t.Fatalf("maxUnavailable is %s, so a rollout takes capacity away before it adds any", got)
	}
}

// TestLivenessAndReadinessUseDifferentPaths. Pointing liveness at the
// readiness endpoint is the single commonest way to turn a dependency
// outage into a cluster wide restart loop.
func TestLivenessAndReadinessUseDifferentPaths(t *testing.T) {
	src := manifest(t)

	live := regexp.MustCompile(`livenessProbe:\s*\n\s*httpGet:\s*\{\s*path:\s*([^,\s]+)`)
	ready := regexp.MustCompile(`readinessProbe:\s*\n\s*httpGet:\s*\{\s*path:\s*([^,\s]+)`)

	lm, rm := live.FindStringSubmatch(src), ready.FindStringSubmatch(src)
	if lm == nil || rm == nil {
		t.Fatalf("could not find both probes: liveness=%v readiness=%v", lm, rm)
	}
	if lm[1] == rm[1] {
		t.Fatalf("liveness and readiness both point at %s, so a dependency outage restarts every pod", lm[1])
	}
	if lm[1] != "/healthz" || rm[1] != "/readyz" {
		t.Fatalf("liveness is %s and readiness is %s, which is not what the code serves", lm[1], rm[1])
	}
}

// TestProbePathsExist. A probe pointing at a path the service does not
// serve fails every time, and for liveness that is a restart loop.
func TestEveryProbePathIsServed(t *testing.T) {
	src := manifest(t)
	re := regexp.MustCompile(`httpGet:\s*\{\s*path:\s*([^,\s]+)`)
	found := re.FindAllStringSubmatch(src, -1)
	if len(found) != 3 {
		t.Fatalf("%d probes in the manifest, want 3", len(found))
	}

	served := map[string]bool{"/healthz": true, "/readyz": true, "/startupz": true}
	for _, m := range found {
		if !served[m[1]] {
			t.Errorf("a probe points at %s, which the service does not serve", m[1])
		}
	}
}

func TestTheLivenessProbeToleratesABlip(t *testing.T) {
	src := manifest(t)
	re := regexp.MustCompile(`livenessProbe:[\s\S]{0,300}?failureThreshold:\s*(\d+)`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatal("the liveness probe has no failureThreshold")
	}
	n, _ := strconv.Atoi(m[1])
	if n < 3 {
		t.Fatalf("liveness restarts after %d failed probe(s). One failure is a blip, and restarting on a blip turns a busy afternoon into an outage.", n)
	}
}

// TestThereIsNoCPULimit. A CPU limit throttles the container at its quota
// even on an idle node, and the symptom is p99 latency nobody can explain.
func TestThereIsNoCPULimit(t *testing.T) {
	src := manifest(t)
	re := regexp.MustCompile(`limits:\s*\{([^}]*)\}`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatal("no resource limits at all, so one pod can take the node's memory")
	}
	if regexp.MustCompile(`\bcpu\s*:`).MatchString(m[1]) {
		t.Fatalf("there is a CPU limit: %s", m[1])
	}
	if !regexp.MustCompile(`\bmemory\s*:`).MatchString(m[1]) {
		t.Fatalf("there is no memory limit: %s", m[1])
	}
}

func TestThereIsADisruptionBudget(t *testing.T) {
	// The deployment strategy says nothing about a node drain, and without
	// a budget draining a node can evict every replica at once.
	if !regexp.MustCompile(`kind:\s*PodDisruptionBudget`).MatchString(manifest(t)) {
		t.Fatal("no PodDisruptionBudget, so a node drain can take every replica")
	}
}
