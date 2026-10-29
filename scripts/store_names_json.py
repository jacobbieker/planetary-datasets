"""Write a store's variable renames as a flat JSON file, for satellite-consumer.

    python scripts/store_names_json.py PREFIX OUT.json

satellite-consumer reads the file named by ``SATCONS_RENAME_JSON`` and renames its
variables before writing, so the stores it fills keep the names fixed in
``planetary_datasets/naming/store_names.json``. A store with no renames gets ``{}``.
"""

from __future__ import annotations

import json
import pathlib
import sys

from planetary_datasets.common.naming import store_names


def main(prefix: str, out: str) -> None:
    pathlib.Path(out).write_text(json.dumps(store_names(prefix), indent=1, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:3])
