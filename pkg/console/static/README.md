# Vendored assets

| File | Upstream | Version | SHA-256 |
|---|---|---|---|
| `assets/alpine.min.js` | https://cdn.jsdelivr.net/npm/alpinejs@3.14.9/dist/cdn.min.js | 3.14.9 (MIT) | `3ed1eed252488921df65e363d6715deb04d7f92aaedb9e52199fdf73cb1e0ad3` |

Update procedure:

```sh
curl -sL "https://cdn.jsdelivr.net/npm/alpinejs@<exact-version>/dist/cdn.min.js" -o alpine.min.js
sha256sum alpine.min.js   # update the table above in the same commit
```

Rules: pin exact versions (never `@3` ranges), review the diff of a vendored
file before committing, and keep this table in sync. The console loads
assets only from this directory — no CDN references at runtime.
