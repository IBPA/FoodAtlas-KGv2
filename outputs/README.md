# Build outputs (`outputs/`)

This directory holds **generated** artifacts from the KG pipeline. Most of it is **not committed** to Git.

## `outputs/kg/` — knowledge graph export

Working KG tables live here. You can **copy in** existing `entities.tsv`, `triplets.tsv`, and related files from another machine or release without rerunning the pipeline, or generate them from the **repository root** (see the root `README.md` and `scripts/README.md`):

| File | Role |
|------|------|
| `entities.tsv` | Entity table |
| `triplets.tsv` | Graph edges |
| `relationships.tsv` | Relationship id → name |
| `metadata_contains.tsv` | Metadata for contains edges |
| `lookup_table_food.tsv`, `lookup_table_chemical.tsv` | Lookup tables |
| … | Additional tables as the pipeline progresses |

**First-time layout:** `./scripts/0_run_kg_init.sh` starts by creating the empty TSV scaffold (via `food_atlas.kg.initialization.create_empty_files`). Later stages fill and extend these files.

### Git and `outputs/kg/`

- `outputs/kg/.gitignore` ignores **new** generated files under this folder (`*` with exceptions), so typical build products such as `entities.tsv` and `triplets.tsv` are **not pushed** to GitHub.
- Tracked exceptions include `outputs/kg/.gitignore` itself and, where applicable, seed files under `outputs/kg/initialization/` that ship with the repo for reproducible builds.

Tools such as `manuscript-repro/` read whatever is present under `outputs/kg/`; they do not rebuild the graph for you.

## Other folders under `outputs/`

See each subfolder’s `.gitignore` and the root `README` for data-processing and additional-analysis workspaces.
