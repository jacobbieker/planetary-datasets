# Credential rotation

Consolidating the data pipelines turned up several credentials hardcoded in source. The new
code reads everything from the environment (see `.env.example`), but that only stops the
bleeding — the values below still exist in files and in local git history, so they need
rotating at the provider.

Verified 2026-09-27. Re-check before acting; branches move.

## Exposure summary

There are **two** distinct S3 credential pairs, with very different exposure. Scanning only
for the `AKIA` prefix misses the second one, which is the urgent one.

| Credential | Where it is | Public? | Action |
|---|---|---|---|
| **source.coop key `SC11A9…` + secret** | 35 occurrences; **live on `origin/main` at HEAD** in `dags/assets/icechunky/{g2ka,himawari,iasi}.py` and `dags/assets/nwp/geos.py` | **YES — published** | **Revoke immediately.** |
| AWS access key `AKIAWCQM…` + secret | 97 occurrences across 51 `.py` files; history of the **local-only** `mars-provider` branch (`f440d4a`, `ed47f05`) | No | Rotate; do not push `mars-provider` as-is. |
| Copernicus Marine username + password | 7 `pb/` download scripts (untracked) | No | Rotate. |
| Destination Earth PAT | `destinE.py:3` (untracked) | No | Rotate. |
| ESA VirES token | `one_offs/vires_download.py:4` (untracked) | No | Rotate. |
| Earthdata / GPM PPS username + password | `pb/corra*.py` (untracked, partly commented out) | No | Rotate. |

### The source.coop key is published — revoke it first

It is in the default branch of a public repository and has been since mid-2025:

```
git log origin/main --oneline -S'SC11A9…'
b63fc1c 2025-08-07 Dump a ton of updates to files and such
b735134 2025-07-29 Remove unused portion
5da9ab7 2025-07-15 Add GOES/Himawari Icechunk creation
```

It is still present in four files at `origin/main` HEAD, and every `consolidate/*` branch
inherits a copy in `dags/assets/icechunk/himawari.py` (scrubbed on this branch).

Assume it is compromised. Revoke it in the Source Cooperative console before anything else;
deleting the literals does not undo publication, and rewriting history on a public repo does
not recall clones or forks.

### The AWS key is not public

Unreachable from `origin/main`, from `origin/dags/refactor`, and from every pushed
`consolidate/*` branch:

```
git log origin/main origin/dags/refactor --oneline -S'AKIAWCQM…'   # 0 commits
```

It lives only in the history of `mars-provider`, which has never been pushed, and in
untracked working-tree files. **The live risk is pushing `mars-provider`**, which would
publish it. Rotate first, or rewrite those two commits before pushing.

Rotating is still right even though nothing is public: the value was pasted into 51 files
over months, and there is no way to be confident it was never shared elsewhere.

### Nothing else is published

The table above is not just a list of what turned up during the migration. `origin/main`
was scanned independently for any remaining plaintext secret:

```
# any assignment of a password / username / token / api_key / secret literal
git grep -nE "(password|passwd|pwd|username|token|api_key|secret)\s*=\s*[\"'][^\"']{6,}[\"']" \
    origin/main -- '*.py' | grep -viE "os\.environ|getenv|\{\{|your_|example"
# -> 0 hits
```

The source.coop pair is missed by that pattern because it is assigned to `access_key_id` /
`secret_access_key`, which is why scanning for one shape of credential is not enough. Both
shapes were checked; only the source.coop pair is reachable from a pushed ref.

## Revoking the source.coop key

1. In the Source Cooperative console, revoke `SC11A9…` for the `bkr` repository.
2. Issue a replacement and put it in `.env` (gitignored) as `AWS_ACCESS_KEY_ID` /
   `AWS_SECRET_ACCESS_KEY`, or configure a profile and set `AWS_PROFILE`. The consolidated
   providers read both via `planetary_datasets/config.py`.
3. Check what the old key could reach. It was a write credential for `bkr`, so review the
   store history for writes you did not make:
   ```
   pixi run python -c "
   from planetary_datasets.config import get_config
   repo = get_config().icechunk_repo('geo/himawari_1km.icechunk')
   for s in list(repo.ancestry(branch='main'))[:20]: print(s.written_at, s.message[:80])"
   ```
4. Remove the remaining literals from `origin/main` (`dags/assets/icechunky/g2ka.py`,
   `himawari.py`, `iasi.py`, `dags/assets/nwp/geos.py`). This does not undo publication, but
   it stops the value spreading further.

## Rotating the AWS key

1. In IAM, create a replacement access key for the identity that writes to
   `us-west-2.opendata.source.coop`.
2. Put it in `.env` (gitignored) as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or
   configure a named profile and set `AWS_PROFILE`. The consolidated providers read both;
   see `planetary_datasets/config.py`.
3. Verify a write still works against a scratch prefix:
   ```
   ICECHUNK_PREFIX=rotation-check pixi run python -c "
   from planetary_datasets.config import get_config
   print(get_config().icechunk_repo('smoke.icechunk'))"
   ```
4. Deactivate, then delete, the old key in IAM.
5. If `mars-provider` is to be kept, rewrite `f440d4a` and `ed47f05` (or squash the branch)
   before pushing it anywhere.

## Where the values used to live

Kept for auditing; every one of these is replaced by a `get_config()` lookup in the
consolidated providers.

- AWS key/secret: `hawaii.py`, `icon_ruc.py`, `tiny.py`, `plot_icing.py`,
  `planetary_datasets/providers/gfs.py`, `planetary_datasets/provider/silam/download_dust.py`,
  `dags/assets/nwp/geso1.py`, `geso2.py`, `geos.py`, `metoffice_analysis.py`,
  `metoffice_uk_analysis.py`, `dags/assets/icechunky/mrms.py`, `iasi.py`, `imerg_final.py`,
  `dags/assets/virt/virtualize_goes_mcmpf.py`, `pb/metoffice_ocean*.py`, `pb/w_m_icechunk.py`,
  `pb/gmgsi.py`, the `one_offs/meteofrance*` scripts, and others.
- Copernicus Marine: `pb/cop_marine.py`, `pb/d_cop_marine{3,4,5,6}.py`,
  `pb/download_cop_marine_2.py`, `pb/download_coperinicus_marine.py`.
- Earthdata / PPS: `pb/corra.py`, `pb/corra2.py`, `pb/corra_gpm.py`.

## Checking it stays clean

```
# No AWS key literals in tracked files
git grep -nE 'AKIA[A-Z0-9]{16}' -- '*.py'

# Nothing reachable from a pushed ref
git log origin/main --oneline -S'AKIA' -- '*.py'
```
