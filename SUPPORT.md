# Support matrix

The primary production distribution is an immutable container image. The
image is built for Linux `amd64` and `arm64` and is intended to run with
PostgreSQL 16 or a compatible managed PostgreSQL service.

| Surface | Support level | Notes |
| --- | --- | --- |
| Linux container | Supported | `amd64` and `arm64`; production target |
| Windows amd64/arm64 | Development only | Go build/test validation; no server release artifact |
| Go module | Development only | Contributor and CI interface; use the patched Go 1.25 toolchain |

The core packages are intended to remain platform-neutral. OS sandboxing and
filesystem permission behavior are adapter/deployment concerns; Windows does
not claim POSIX mode-bit enforcement. PostgreSQL 16 is the supported durable
integration-test database. The alpha release does not promise a stable
application API or database migration rollback; take a backup before an
upgrade.

Issues and feature requests should include the version, OS/architecture, Go
version, reproduction steps, and relevant non-sensitive logs. Never attach
credentials or contents of `.credentials`.
