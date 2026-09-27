# Credential rotation

Consolidating the data pipelines turned up several credentials hardcoded in source. The new
code reads everything from the environment (see `.env.example`), but that only stops the
bleeding — the values below still exist in files and in local git history, so they need
rotating at the provider.

Verified 2026-09-27. Re-check before acting; branches move.

## Exposure summary

| Credential | Where it is | Reachable from a pushed branch? | Action |
|---|---|---|---|
| AWS access key `AKIAWCQM…` + secret | 97 occurrences across 51 `.py` files; history of the **local-only** `mars-provider` branch (commits `f440d4a`, `ed47f05`) | **No** | Rotate. Do not push `mars-provider` as-is. |
| Copernicus Marine username + password | 7 `pb/` download scripts (untracked) | No | Rotate. |
| Destination Earth PAT | `destinE.py:3` (untracked) | No | Rotate. |
| ESA VirES token | `one_offs/vires_download.py:4` (untracked) | No | Rotate. |
| Earthdata / GPM PPS username + password | `pb/corra*.py` (untracked, partly commented out) | No | Rotate. |

### The AWS key is not currently public

It is unreachable from `origin/main`, from `origin/dags/refactor`, and from every pushed
`consolidate/*` branch:

```
git log origin/main origin/dags/refactor --oneline -S'AKIAWCQM…'   # 0 commits
```

It lives only in the history of `mars-provider`, which has never been pushed, and in
untracked working-tree files.

**The live risk is pushing `mars-provider`.** Doing so would publish the key in a public
repository. Rotate first, or rewrite those two commits before pushing.

Rotating is still the right call even though nothing is public: the value was pasted into
51 files over months, and there is no way to be confident it was never shared elsewhere.

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
