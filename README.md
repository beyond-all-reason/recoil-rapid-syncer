# Spring RTS Rapid syncer

A small stateless utility that can mirror content of a Spring RTS
[Rapid](https://springrts.com/wiki/Rapid) repository to some other hosting.

Currently it supports [Bunny Edge Storage](https://bunny.net/edge-storage/) as a
target destination for syncing. Binary runs a HTTP server with `/sync` endpoint
that performs mirroring when triggered. This allows to deploy it as a cheap
[Google Cloud Run](https://cloud.google.com/run) service triggered by the [Cloud
Scheduler](https://cloud.google.com/scheduler) periodically.

### TODO
- [ ] More automated tests
- [ ] Refactor destinations and allow at least local storage one for testing
- [ ] Write down the GCP project deployment spec in terraform/pulimi.
