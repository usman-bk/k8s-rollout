# k8s-rollout

_Built with AI assistance._

Why a rolling deploy returns 502s, and the shutdown sequence that stops it.

Go 1.23, no dependencies outside the standard library.

```
internal/lifecycle/lifecycle.go  the states, and what readiness means in each
internal/lifecycle/server.go     the shutdown order
internal/probes/                 the three endpoints the kubelet calls
deploy/deployment.yaml           the manifest, with a test holding it to the code
cmd/probed/
```

## The thing that is counter-intuitive

When a pod is deleted, two things happen at the same moment and **neither
waits for the other**:

- the kubelet sends SIGTERM
- the endpoints controller starts removing the pod from the Service

The second has to reach every kube-proxy and every ingress in the cluster,
which takes a moment. During that moment the pod is still being sent new
connections, and those connections are being routed correctly.

So a service that exits promptly on SIGTERM is refusing requests that were
sent to it on purpose. That is what 502s during a rollout are, and it is
exactly what `signal.NotifyContext` plus `srv.Shutdown` gives you.

The fix reads wrong until you see why: **on SIGTERM, fail readiness and
then keep serving.**

    1. fail readiness          the pod starts leaving the Service
    2. keep serving, 5s        the removal has not reached everybody yet
    3. close the listener      in-flight requests finish, up to 20s
    4. exit

Here it is happening:

    === while serving ===
      readyz:   {"status":"ok","state":"serving"}
      healthz:  {"status":"ok","state":"serving"}

    === start a 4 second request, then send SIGTERM ===
    === during the drain ===
      readyz:   {"status":"unavailable","state":"draining","in_flight":2}
      healthz:  {"status":"ok","state":"draining","in_flight":2}
      new work: HTTP 200
      in-flight request returned: worked for 4s

Readiness is already failing. Liveness is not. New work is still answered.
The four second request that was running when the signal arrived finished.

## Three probes, three questions

Conflating them is the second way a rollout goes wrong, and the worse one,
because it turns a dependency outage into a cluster-wide restart loop.

| | question | what failing means | checks dependencies |
|---|---|---|---|
| `/startupz` | has it finished starting? | wait longer, then restart | no |
| `/healthz` | is it stuck? | **restart it** | **never** |
| `/readyz` | should it get traffic? | take it out of the Service | yes |

**Liveness must not check the database.** The only response to liveness
failing is a restart, and restarting every pod does not bring a database
back; it loses all the warm state and adds a thundering herd to a system
that is already struggling. There is a test asserting liveness passes while
a dependency is down, and another asserting the manifest does not point
both probes at the same path.

**A draining pod is still live.** Failing liveness during a drain asks the
kubelet to kill a pod that is deliberately finishing work.

**The startup probe exists so liveness can be impatient.** Without it, a
slow start forces a long `initialDelaySeconds` on liveness, which also
delays detecting a hang for the whole life of the pod.

## The manifest is tested against the code

`terminationGracePeriodSeconds` has to be longer than drain plus shutdown.
When it expires the kubelet sends SIGKILL and whatever is running is cut
off mid-response.

Nothing makes those two agree. They are in different files, reviewed by
different people, and the symptom of them drifting is requests cut off
during a deploy weeks later, with nothing in the diff that looks related.
So the two are compared, in a test:

    terminationGracePeriodSeconds is 15s, and the application needs 30s
    (a 5s drain plus a 20s shutdown plus a margin). SIGKILL would arrive
    mid request.

Four more hold the manifest to things that are easy to get wrong:

- `maxUnavailable: 0`, or a rollout removes capacity before it adds any,
  which on three replicas is losing a third of the fleet every deploy.
- liveness and readiness on different paths.
- every probe path is one the service actually serves.
- the liveness `failureThreshold` is at least 3. One failed probe is a
  blip, and restarting on a blip turns a busy afternoon into an outage.
- no CPU limit. A CPU limit throttles the container at its quota even on
  an idle node, and the symptom is p99 latency nobody can explain. Memory
  is limited, because that failure mode takes the node with it.

Parsed with a regular expression rather than a YAML library: pulling in a
dependency to read four numbers out of one file is the worse trade.

## Watching a rollout

    go run ./cmd/probed -drain 3s -shutdown 10s

    curl localhost:8080/readyz
    curl "localhost:8080/work?ms=4000" &   # then send SIGTERM and watch

    go run ./cmd/probed -dependency-down   # readyz fails, healthz does not
    go run ./cmd/probed -startup-work 10s  # watch the startup probe

The binary prints the grace period the manifest needs, so nobody has to
work it out:

    "terminationGracePeriodSeconds_needed": 30

## What is tested, and what is not

    go test ./...     25 cases, no cluster

No cluster, and that is a real limit rather than a convenience: the timing
this package is built around is a property of kube-proxy propagation, and
nothing here measures it. What the tests do cover is the ordering and the
states, which is the part that is in the code.

`TestNoRequestIsDroppedThroughAWholeRollout` runs eight clients in a loop
against a real listener, drains the pod underneath them, and requires zero
failures.

The manifest tests were checked by breaking the manifest three ways and
confirming three failures.

## Not covered

**No cluster test.** The drain window is a guess until it is measured
against a real cluster: five seconds is a starting point, and the thing to
raise when a rollout still shows 502s. `doc/drain-timing.md` has how to
measure it and why the number is usually larger than people expect.

**Nothing about connection reuse.** A client holding a keep-alive
connection to a draining pod keeps using it, and `Shutdown` closes idle
ones but waits for active ones. A long-lived connection pool needs more
than this.

**No gRPC.** gRPC keeps long-lived connections and needs `GOAWAY`, which
is a different shutdown entirely.
