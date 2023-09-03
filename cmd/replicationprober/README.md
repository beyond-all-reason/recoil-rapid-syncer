# Replication Prober

Replication Prober is a tool for testing replication of data in Bunny CDN.

- `RedirectProber`: tests current status of HTTP Redirects for the
  `{repo}/version.gz` file that are set up using
  https://github.com/p2004a/bar-repos-bunny-replication-lag-mitigation.
- `StorageReplicationProber`: writes a special canary file to Bunny Edge Storage
  and measures after what time the it's available in the configured replication
  zones.

See `config` struct in `main.go` for the ENV configuration options.
