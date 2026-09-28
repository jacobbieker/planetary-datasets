"""Publishing finished stores to the Hugging Face Hub.

Replaces the one-off ``upload_hf.py`` scripts that each hardcoded a repo id, a
local path and — in a couple of cases — a token. The destination repo comes
from ``HF_REPO_ID`` and the token from ``HF_TOKEN``, both via the shared
config, so nothing secret or machine-specific is committed.
"""

from __future__ import annotations

import pathlib

from loguru import logger

from planetary_datasets.config import Config, get_config


def upload_folder(
    folder: str | pathlib.Path,
    repo_id: str | None = None,
    *,
    repo_type: str = "dataset",
    path_in_repo: str | None = None,
    config: Config | None = None,
    dry_run: bool = False,
) -> str:
    """Upload a local directory — typically a Zarr store — to the Hub.

    Uses ``upload_large_folder``, which resumes and parallelises, because these
    stores run to hundreds of thousands of chunk files.

    Args:
        folder: Local directory to upload.
        repo_id: Destination repo. Defaults to ``HF_REPO_ID``.
        repo_type: Hub repo type; these are datasets, not models.
        path_in_repo: Subdirectory within the repo. Defaults to the repo root.
        config: Override configuration.
        dry_run: Validate the inputs and credentials, then stop without
            uploading. Used by the end-to-end checks, which must never write to
            the Hub.

    Returns:
        The resolved ``repo_id``.

    Raises:
        MissingCredential: when ``HF_TOKEN`` is not configured.
        ValueError: when no repo id is given or configured.
        FileNotFoundError: when ``folder`` does not exist.
    """
    cfg = config if config is not None else get_config()
    folder = pathlib.Path(folder).expanduser()

    target = repo_id or cfg.hf_repo_id
    if not target:
        raise ValueError(
            "No Hugging Face repo id. Pass repo_id, or set HF_REPO_ID in .env or the "
            "environment."
        )
    if not folder.is_dir():
        raise FileNotFoundError(f"{folder} is not a directory")

    (token,) = cfg.credentials.require("hf_token")

    if dry_run:
        logger.info(f"dry run: would upload {folder} to {repo_type} {target}")
        return target

    from huggingface_hub import HfApi

    api = HfApi(token=token)
    api.create_repo(repo_id=target, repo_type=repo_type, exist_ok=True)
    kwargs = {"path_in_repo": path_in_repo} if path_in_repo else {}
    api.upload_large_folder(
        folder_path=str(folder),
        repo_id=target,
        repo_type=repo_type,
        **kwargs,
    )
    logger.info(f"uploaded {folder} to {repo_type} {target}")
    return target
