"""Central configuration loaded from the environment and an optional ``.env`` file.

Every provider and Dagster asset reads configuration from here rather than hardcoding
credentials, bucket names or machine-specific paths. Real environment variables always win
over values in ``.env`` so the same code runs unchanged under Dagster, in CI and from a
shell.

Typical use::

    from planetary_datasets.config import get_config

    cfg = get_config()
    repo = cfg.icechunk_repo("bkr/dmi/hawaii_nams.icechunk")

Setting ``ICECHUNK_LOCAL_PATH`` redirects every store to a local filesystem directory, which
is how tests and end-to-end checks avoid writing to the public bucket.
"""

from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass, field
from functools import lru_cache

from dotenv import load_dotenv

from planetary_datasets.common.paths import UnsafePath, default_scratch_dir, safe_join

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

DEFAULT_BUCKET = "us-west-2.opendata.source.coop"
DEFAULT_REGION = "us-west-2"


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if value is not None:
        value = value.strip()
    return value or None


def _env_path(name: str, default: str) -> pathlib.Path:
    return pathlib.Path(_env(name) or default).expanduser()


def _env_bool(name: str, default: bool | None = None) -> bool | None:
    raw = _env(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


class MissingCredential(RuntimeError):
    """Raised when a provider needs a credential that is not configured."""


@dataclass(frozen=True)
class Credentials:
    """Credentials for the third-party services the providers talk to.

    Values are ``None`` when unset; call :meth:`require` to fail loudly at the point of use
    rather than sending an anonymous request and getting an opaque error back.
    """

    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_profile: str | None = None
    copernicusmarine_username: str | None = None
    copernicusmarine_password: str | None = None
    earthdata_username: str | None = None
    earthdata_password: str | None = None
    gpm_pps_username: str | None = None
    gpm_pps_password: str | None = None
    cdsapi_url: str | None = None
    cdsapi_key: str | None = None
    eumetsat_consumer_key: str | None = None
    eumetsat_consumer_secret: str | None = None
    ecmwf_api_url: str | None = None
    ecmwf_api_key: str | None = None
    ecmwf_api_email: str | None = None
    hf_token: str | None = None
    destine_pat: str | None = None
    vires_token: str | None = None

    def require(self, *names: str) -> tuple[str, ...]:
        """Return the named credentials, raising if any are unset."""
        missing = [n for n in names if getattr(self, n) is None]
        if missing:
            raise MissingCredential(
                "Missing required credentials: "
                + ", ".join(sorted(n.upper() for n in missing))
                + ". Set them in .env or the environment (see .env.example)."
            )
        return tuple(getattr(self, n) for n in names)


@dataclass(frozen=True)
class Config:
    """Resolved configuration for a process."""

    bucket: str = DEFAULT_BUCKET
    prefix: str = ""
    region: str = DEFAULT_REGION
    endpoint_url: str | None = None
    force_path_style: bool | None = None
    allow_http: bool = False
    data_dir: pathlib.Path = field(default_factory=lambda: pathlib.Path("data"))
    scratch_dir: pathlib.Path = field(default_factory=default_scratch_dir)
    icechunk_local_path: pathlib.Path | None = None
    hf_repo_id: str | None = None
    memory_fraction: float = 0.8
    memory_ceiling_gb: float | None = None
    credentials: Credentials = field(default_factory=Credentials)

    @property
    def use_local_store(self) -> bool:
        """True when stores should be written to the local filesystem instead of S3."""
        return self.icechunk_local_path is not None

    def full_prefix(self, prefix: str) -> str:
        """Apply the configured ``ICECHUNK_PREFIX`` to a store prefix.

        Every path-producing method routes through this. Resolving the prefix in more than
        one place is how a staging prefix ended up being reported but not written to.
        """
        prefix = prefix.strip("/")
        if any(part == ".." for part in prefix.split("/")):
            raise UnsafePath(f"store prefix {prefix!r} may not contain '..'")
        if self.prefix:
            prefix = f"{self.prefix.strip('/')}/{prefix}"
        return prefix

    def local_store_path(self, prefix: str) -> pathlib.Path:
        """Resolve a store prefix to a directory under the configured local root.

        Raises :class:`~planetary_datasets.common.paths.UnsafePath` if the prefix would
        escape that root.
        """
        if not self.use_local_store:
            raise ValueError("no local store configured; set ICECHUNK_LOCAL_PATH")
        return safe_join(self.icechunk_local_path, self.full_prefix(prefix))

    def store_path(self, prefix: str) -> str:
        """Resolve a store prefix to a full path.

        ``prefix`` is the logical location of a store, e.g. ``bkr/dmi/hawaii_nams.icechunk``.
        It is returned as an ``s3://`` URI, or as a local directory when
        ``ICECHUNK_LOCAL_PATH`` is set.
        """
        if self.use_local_store:
            return str(self.local_store_path(prefix))
        return f"s3://{self.bucket}/{self.full_prefix(prefix)}"

    def icechunk_storage(self, prefix: str):
        """Build an ``icechunk`` storage object for a store prefix."""
        import icechunk

        if self.use_local_store:
            path = self.local_store_path(prefix)
            path.mkdir(parents=True, exist_ok=True)
            return icechunk.local_filesystem_storage(str(path))

        creds = self.credentials
        kwargs = {
            "bucket": self.bucket,
            "prefix": self.full_prefix(prefix),
            "region": self.region,
        }
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
            # A custom endpoint almost always needs path-style addressing, and a bucket
            # name containing dots cannot be addressed virtual-host style over TLS at all.
            kwargs["force_path_style"] = (
                self.force_path_style if self.force_path_style is not None else True
            )
            if self.allow_http:
                kwargs["allow_http"] = True
        elif self.force_path_style:
            kwargs["force_path_style"] = True
        if creds.aws_profile:
            # icechunk has no profile argument; the AWS SDK resolves AWS_PROFILE from the
            # environment when credentials are sourced from there. An explicit profile is
            # what writing to source.coop needs.
            os.environ["AWS_PROFILE"] = creds.aws_profile
            return icechunk.s3_storage(**kwargs, from_env=True)
        if creds.aws_access_key_id and creds.aws_secret_access_key:
            return icechunk.s3_storage(
                **kwargs,
                access_key_id=creds.aws_access_key_id,
                secret_access_key=creds.aws_secret_access_key,
            )
        return icechunk.s3_storage(**kwargs, from_env=True)

    def icechunk_repo(self, prefix: str):
        """Open or create the icechunk repository for a store prefix."""
        import icechunk

        return icechunk.Repository.open_or_create(self.icechunk_storage(prefix))


def load_config(env_file: str | os.PathLike | None = None, override: bool = False) -> Config:
    """Read configuration from ``.env`` and the environment.

    Args:
        env_file: Path to the dotenv file. Defaults to ``.env`` at the repository root.
        override: When True, values in the dotenv file take precedence over real
            environment variables. Defaults to False so the environment always wins.
    """
    path = pathlib.Path(env_file) if env_file is not None else REPO_ROOT / ".env"
    if path.is_file():
        load_dotenv(path, override=override)

    local_path = _env("ICECHUNK_LOCAL_PATH")
    ceiling = _env("MEMORY_CEILING_GB")

    return Config(
        bucket=_env("ICECHUNK_BUCKET", DEFAULT_BUCKET) or DEFAULT_BUCKET,
        prefix=_env("ICECHUNK_PREFIX", "") or "",
        region=_env("AWS_REGION", DEFAULT_REGION) or DEFAULT_REGION,
        endpoint_url=_env("ICECHUNK_ENDPOINT_URL"),
        force_path_style=_env_bool("ICECHUNK_FORCE_PATH_STYLE"),
        allow_http=bool(_env_bool("ICECHUNK_ALLOW_HTTP", False)),
        data_dir=_env_path("PLANETARY_DATASETS_DATA_DIR", str(REPO_ROOT / "data")),
        scratch_dir=(
            pathlib.Path(_env("PLANETARY_DATASETS_SCRATCH_DIR")).expanduser()
            if _env("PLANETARY_DATASETS_SCRATCH_DIR")
            else default_scratch_dir()
        ),
        icechunk_local_path=pathlib.Path(local_path).expanduser() if local_path else None,
        hf_repo_id=_env("HF_REPO_ID"),
        memory_fraction=_env_float("MEMORY_FRACTION", 0.8),
        memory_ceiling_gb=float(ceiling) if ceiling else None,
        credentials=Credentials(
            aws_access_key_id=_env("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY"),
            aws_profile=_env("AWS_PROFILE"),
            copernicusmarine_username=_env("COPERNICUSMARINE_SERVICE_USERNAME"),
            copernicusmarine_password=_env("COPERNICUSMARINE_SERVICE_PASSWORD"),
            earthdata_username=_env("EARTHDATA_USERNAME"),
            earthdata_password=_env("EARTHDATA_PASSWORD"),
            gpm_pps_username=_env("GPM_PPS_USERNAME"),
            gpm_pps_password=_env("GPM_PPS_PASSWORD"),
            cdsapi_url=_env("CDSAPI_URL"),
            cdsapi_key=_env("CDSAPI_KEY"),
            eumetsat_consumer_key=_env("EUMETSAT_CONSUMER_KEY"),
            eumetsat_consumer_secret=_env("EUMETSAT_CONSUMER_SECRET"),
            ecmwf_api_url=_env("ECMWF_API_URL"),
            ecmwf_api_key=_env("ECMWF_API_KEY"),
            ecmwf_api_email=_env("ECMWF_API_EMAIL"),
            hf_token=_env("HF_TOKEN"),
            destine_pat=_env("DESTINE_PAT"),
            vires_token=_env("VIRES_TOKEN"),
        ),
    )


@lru_cache(maxsize=1)
def get_config() -> Config:
    """Return the process-wide configuration, loading it on first use."""
    return load_config()


def reset_config_cache() -> None:
    """Clear the cached configuration. Intended for tests that change the environment."""
    get_config.cache_clear()
