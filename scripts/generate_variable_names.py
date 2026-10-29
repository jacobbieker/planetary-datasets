"""Fix every in-scope store's variable names, for review, from a metadata scan.

    python scripts/generate_variable_names.py SCAN.json SCOPE.json

SCAN.json maps store prefix -> {array path: {dims, long_name, standard_name, GRIB_*}};
SCOPE.json lists the stores to rename ("box" and "static"). Writes
``planetary_datasets/naming/store_names.json`` (old -> new, changed names only) and
``plans/variable_naming_review.md``.
"""

from __future__ import annotations

import collections
import json
import pathlib
import re
import sys

from planetary_datasets.common.naming import (
    coordinate_rename,
    geopotential_rename,
    level_qualifier,
    propose,
    snake,
    unit_conversion,
)

ROOT = pathlib.Path(__file__).resolve().parents[1]
#: Stores whose variables carry a product prefix, named after it.
PREFIXED = {
    "bkr/polar/earthcare.icechunk": None,
    "bkr/polar/pace_l2.icechunk": None,
    "bkr/polar/pace_virtual.icechunk": None,
    "bkr/polar/aeolus.icechunk": None,
}


def product_prefixes() -> dict[str, list[str]]:
    from planetary_datasets.providers.polar import aeolus, earthcare, pace, pace_virtual

    return {
        "bkr/polar/earthcare.icechunk": [p.prefix for p in earthcare.PRODUCTS.values()],
        "bkr/polar/pace_l2.icechunk": list(pace.PRODUCTS),
        "bkr/polar/pace_virtual.icechunk": list(pace_virtual.PRODUCTS),
        "bkr/polar/aeolus.icechunk": list(aeolus.LEVELS),
    }


def name_store(store: str, arrays: dict, prefixes: list[str]) -> tuple[dict, list]:
    proposals = {}
    rows = []
    for path, info in arrays.items():
        if path == "__error__":
            continue
        group, _, leaf = path.rpartition("/")
        prefix = next(
            (p for p in sorted(prefixes, key=len, reverse=True) if leaf.startswith(p + "_")), ""
        )
        rest = leaf[len(prefix) + 1 :] if prefix else leaf
        dims = [
            d[len(prefix) + 1 :] if prefix and d.startswith(prefix + "_") else d
            for d in info.get("dims", [])
        ]
        new, rule = propose(rest, info, dims, store)
        heights_as_geopotential = geopotential_rename(new, info.get("units"))
        if heights_as_geopotential and rule != "coordinate":
            new, rule = heights_as_geopotential, rule + "+geopotential"
        new = f"{prefix}_{new}" if prefix else new
        full = f"{group}/{new}" if group else new
        proposals[path] = (full, rule)
    # Two variables must not end up with one name. First what the source names say about
    # their levels tells them apart; failing that the later ones keep their tidied source
    # name, and failing that their source name.
    for round_ in range(3):
        taken = collections.defaultdict(list)
        for old, (new, _) in proposals.items():
            taken[new].append(old)
        clashes = {new: olds for new, olds in taken.items() if len(olds) > 1}
        if not clashes:
            break
        for new, olds in clashes.items():
            for old in olds:
                leaf = old.rsplit("/", 1)[-1]
                group = old.rpartition("/")[0]
                if round_ == 0:
                    qualifier = level_qualifier(leaf)
                    candidate = f"{new}_{qualifier}" if qualifier else new
                elif round_ == 1:
                    candidate = (f"{group}/" if group else "") + snake(leaf)
                else:
                    candidate = old
                proposals[old] = (candidate, "clash" if candidate != new else proposals[old][1])
    conversions = {}
    for old, (new, rule) in sorted(proposals.items()):
        units = arrays[old].get("units")
        converted = unit_conversion(new, units) if rule not in ("coordinate", "kept") else None
        if converted:
            # Stored as plain numbers, so xarray decodes in float64 whatever the stored dtype:
            # a float16 pressure in hPa times 100 would overflow float16.
            converted = (float(converted[0]), float(converted[1]), converted[2])
        if converted:
            conversions[new] = {
                "scale_factor": converted[0],
                "add_offset": converted[1],
                "units": converted[2],
                "from_units": units,
            }
            rule = f"{rule}+units"
        rows.append(
            (
                old,
                new,
                rule,
                arrays[old].get("long_name"),
                f"{units} -> {converted[2]}" if converted else units,
            )
        )
    dims = {}
    for path, info in arrays.items():
        if path == "__error__":
            continue
        group = path.rpartition("/")[0]
        for dim in info.get("dims", []):
            key = f"{group}/{dim}" if group else dim
            if dim not in dims:
                renamed = coordinate_rename(dim, arrays.get(key, {}), [dim])
                # Never onto a name the store already uses for something else.
                existing = {p.rsplit("/", 1)[-1] for p in arrays} | {
                    d for a in arrays.values() if isinstance(a, dict) for d in a.get("dims", [])
                }
                if renamed and renamed not in existing:
                    dims[dim] = renamed
    return {o: n for o, (n, _) in proposals.items() if o != n}, rows, conversions, dims


def family(store: str) -> str:
    leaf = store.rsplit("/", 1)[-1]
    leaf = re.sub(r"(_\d{6,8}(T\d+)?|_\d+|_backfill_\d+|_\d{4}-\d\d-\d\d)?\.icechunk$", "", leaf)
    leaf = re.sub(r"_C\d\d$", "_C*", leaf)
    return store.rsplit("/", 1)[0] + "/" + leaf


def main(scan_path: str, scope_path: str) -> None:
    scan = json.loads(pathlib.Path(scan_path).read_text())
    scope = json.loads(pathlib.Path(scope_path).read_text())
    prefixes = product_prefixes()
    stores = sorted(set(scope["box"]) | set(scope["static"]))
    mapping: dict[str, dict[str, str]] = {}
    units_out: dict[str, dict] = {}
    dims_out: dict[str, dict] = {}
    report = collections.defaultdict(list)
    for store in stores:
        arrays = scan.get(store, {})
        if not arrays or "__error__" in arrays:
            continue
        renamed, rows, conversions, dims = name_store(store, arrays, prefixes.get(store, []))
        if dims:
            dims_out[store] = dims
        if renamed:
            mapping[store] = renamed
        if conversions:
            units_out[store] = conversions
        report[family(store)].append((store, rows))
    out = ROOT / "planetary_datasets" / "naming" / "store_names.json"
    out.write_text(json.dumps(mapping, indent=1, sort_keys=True) + "\n")
    (out.parent / "store_units.json").write_text(
        json.dumps(units_out, indent=1, sort_keys=True) + "\n"
    )
    (out.parent / "store_dims.json").write_text(
        json.dumps(dims_out, indent=1, sort_keys=True) + "\n"
    )
    lines = [
        "# Variable naming review",
        "",
        f"{len(mapping)} stores renamed, "
        f"{sum(len(m) for m in mapping.values())} variables. One table per store family "
        "(its first store); `kept` and `clash` rows need a look.",
        "",
    ]
    rules = collections.Counter(r[2] for fam in report.values() for _, rows in fam for r in rows)
    lines += ["Rules: " + ", ".join(f"{k} {v}" for k, v in rules.most_common()), ""]
    for fam, entries in sorted(report.items()):
        store, rows = entries[0]
        changed = [r for r in rows if r[0] != r[1] or r[2] in ("kept", "clash")]
        if not changed:
            continue
        lines += [
            f"## {fam} ({len(entries)} store(s))",
            "",
            "| old | new | rule | long_name | units |",
            "|---|---|---|---|---|",
        ]
        for old, new, rule, long_name, units in changed:
            lines.append(
                f"| `{old}` | `{new}` | {rule} | {str(long_name or '')[:60]} | {units or ''} |"
            )
        lines.append("")
    (ROOT / "plans").mkdir(exist_ok=True)
    text = "\n".join(lines) + "\n"
    # Some source long names carry undecodable bytes.
    text = text.encode("utf-8", "replace").decode("utf-8")
    (ROOT / "plans" / "variable_naming_review.md").write_text(text)
    print(
        f"{len(mapping)} stores, {sum(len(m) for m in mapping.values())} renames, "
        f"{sum(len(u) for u in units_out.values())} unit conversions, "
        f"{sum(len(d) for d in dims_out.values())} dimension renames in {len(dims_out)} stores; "
        f"rules {dict(rules)}"
    )


if __name__ == "__main__":
    main(*sys.argv[1:3])
