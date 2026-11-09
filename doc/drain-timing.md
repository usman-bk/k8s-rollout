# Where the five seconds came from

A note to myself, because I have now worked this out twice.

The drain window is not a property of the application. It is how long
kube-proxy and every ingress controller in front of it take to stop sending
new connections after the endpoint is marked not-ready. The pod has already
had its SIGTERM by then and is still being given work.

Five seconds is a starting point taken from the behaviour of a cluster I had
access to, not a measurement of yours. The way to find yours:

1. Run a client in a loop against the service, one request every 50ms, and
   count failures.
2. Roll the deployment.
3. If anything 502s, raise the drain and roll again.

Two things that make the number larger than people expect. An ingress that
watches endpoints through its own cache adds its own propagation delay on top
of kube-proxy. And a node under load syncs iptables more slowly, so a number
measured on an idle cluster is a floor.

The number that is NOT a guess is the grace period, because the binary works
it out and prints it, and `manifest_test.go` fails if the manifest disagrees.
Drain plus shutdown plus a margin. Get the drain wrong and the grace period
is wrong with it, which is why this note exists at all.
