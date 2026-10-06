"""Generate docs/data-dictionary.md from the compiled dbt manifest.

    dbt docs generate --project-dir transform --profiles-dir transform
    python -m pipelines.generate_data_dictionary
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

LOGGER = logging.getLogger("data-dictionary")

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = REPO_ROOT / "transform" / "target" / "manifest.json"
OUTPUT = REPO_ROOT / "docs" / "data-dictionary.md"


def main() -> int:
    if not MANIFEST.exists():
        LOGGER.error("manifest not found - run `dbt docs generate` first")
        return 1
    manifest = json.loads(MANIFEST.read_text())
    catalog_path = MANIFEST.parent / "catalog.json"
    catalog = json.loads(catalog_path.read_text())["nodes"] if catalog_path.exists() else {}
    nodes = manifest["nodes"]

    lines = [
        "# Data Dictionary",
        "",
        "Generated from the dbt project (`transform/`) - staging views over the silver",
        "Parquet zone and gold marts in `data/gold/bharat.duckdb`. Regenerate with",
        "`dbt docs generate` + `python -m pipelines.generate_data_dictionary`.",
        "",
    ]

    sections = [
        ("Staging (schema `silver` in the DuckDB warehouse)", "staging"),
        ("Gold marts (schema `gold`)", "marts"),
    ]
    for section_title, folder in sections:
        lines.append(f"## {section_title}")
        lines.append("")
        models = sorted(
            (node for node in nodes.values()
             if node.get("resource_type") == "model" and f"models/{folder}/" in node.get("original_file_path", "")),
            key=lambda node: node["name"],
        )
        for node in models:
            lines.append(f"### {node['schema']}.{node['name']}")
            description = (node.get("description") or "").strip()
            if description:
                lines.append(f"{description}")
            lines.append("")
            lines.append("| Column | Type | Description |")
            lines.append("|---|---|---|")
            catalog_columns = catalog.get(node["unique_id"], {}).get("columns", {})
            documented = node.get("columns", {})
            all_columns = sorted(
                set(catalog_columns) | set(documented),
                key=lambda name: list(catalog_columns).index(name)
                if name in catalog_columns else len(catalog_columns),
            )
            for name in all_columns:
                col_type = catalog_columns.get(name, {}).get("type") or documented.get(name, {}).get("data_type") or ""
                col_desc = (documented.get(name, {}).get("description") or "").strip()
                lines.append(f"| {name} | {col_type} | {col_desc} |")
            if not all_columns:
                lines.append("| (no columns found in catalog) | | |")
            lines.append("")

    OUTPUT.write_text("\n".join(lines) + "\n")
    LOGGER.info("wrote %s", OUTPUT)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
