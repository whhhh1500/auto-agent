# Container release checklist

Harness Core is released as an immutable container image, not as a set of
platform binaries. The tag workflow builds Linux `amd64` and `arm64` OCI
artifacts with `v0.1.0-alpha.1`-style version metadata, emits an image archive
checksum and an SPDX SBOM, and uploads them for review. It deliberately does
not push to a registry or claim a signature when registry/OIDC/key material is
not configured.

Before tagging:

1. Confirm governance files and the changelog are updated.
2. Run the clean-checkout smoke script and the full Go CI gates, including
   PostgreSQL (not skipped), race, Windows, vulnerability, and coverage jobs.
3. Create the reviewed `v*` tag from a clean checkout. Verify the immutable
   commit tag and `org.opencontainers.image.revision` image label.
4. Review the OCI archive checksum, SBOM, and build metadata. Configure the
   registry, provenance, and cosign policy before enabling publication.
5. Deploy the exact image digest to staging; check `/healthz` and `/readyz`,
   then exercise an authenticated HTTP/SSE request.
6. Back up PostgreSQL and the master key before production migration. Follow
   [deployment and recovery](docs/deployment.md) for rolling upgrades.

Go module builds and local binaries remain developer verification interfaces;
they are not production release artifacts. Do not publish from a worktree
without version-control ownership and an explicit release decision.

## Local release smoke (optional)

The local release check uses the same image metadata arguments as the tag
workflow (`VERSION`, `COMMIT`, and `BUILD_DATE`), then invokes the retained
container smoke script. On Unix-like systems:

```sh
version=v0.1.0-alpha.1
commit=local
build_date=local
image="harness-core:release-smoke-${version}"
docker build --tag "$image" \
  --build-arg VERSION="$version" \
  --build-arg COMMIT="$commit" \
  --build-arg BUILD_DATE="$build_date" .
bash scripts/container-smoke.sh "$image"
```

PowerShell equivalent:

```powershell
$version = 'v0.1.0-alpha.1'
$commit = 'local'
$buildDate = 'local'
$image = "harness-core:release-smoke-$version"
docker build --tag $image --build-arg "VERSION=$version" --build-arg "COMMIT=$commit" --build-arg "BUILD_DATE=$buildDate" .
& ./scripts/container-smoke.ps1 -Image $image
```
