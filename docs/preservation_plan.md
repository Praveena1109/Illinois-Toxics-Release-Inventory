# Preservation and Dissemination Plan

## Purpose

This document explains how the curated Illinois TRI dataset is prepared for longer-term access, integrity checking, citation, and reuse.

## Preservation Goals

The preservation goal is to ensure that future users can:

1. Find the dataset.
2. Understand the dataset.
3. Verify file integrity.
4. Reproduce key curation steps.
5. Cite the dataset.
6. Reuse the dataset responsibly.

## Preservation Package Contents

The preservation package includes:

| Category | Files |
|---|---|
| Curated data | `outputs/*.csv` |
| Documentation | `README.md`, `docs/data_user_guide.md`, `docs/quality_dimensions.md` |
| Metadata | `metadata/dataset_metadata.json`, `docs/data_dictionary.csv`, `docs/data_dictionary.json` |
| Provenance | `docs/acquisition_log.csv`, `docs/curation_log.md`, `docs/curation_log.json`, `docs/provenance.md`, `docs/structuring_provenance.json` |
| Fixity | `docs/fixity_manifest.csv`, `docs/fixity_manifest.json` |
| Workflow | `scripts/*.py`, `requirements.txt`, `environment.yml` |
| Citation and rights | `CITATION.cff`, `LICENSE` |

## Format Choices

| Format | Used For | Preservation Rationale |
|---|---|---|
| CSV | Data outputs | Open, widely supported, non-proprietary, easy to inspect |
| JSON | Metadata and provenance | Machine-readable, structured, common in repositories |
| Markdown | Documentation | Human-readable plain text, version-control friendly |
| Python | Workflow scripts | Reproducible processing and automation |
| YAML | Environment file and citation metadata | Structured, readable, supported by GitHub and conda |

## Fixity Strategy

Fixity is implemented using file checksums. Run:

```bash
python scripts/06_fixity_manifest.py
```

This creates:

```text
docs/fixity_manifest.csv
docs/fixity_manifest.json
```

The manifest records:

- file path
- file size
- MD5 checksum
- SHA-256 checksum
- timestamp generated

MD5 is included because many repositories display MD5 checksums. SHA-256 is included as a stronger integrity check.

## Repository Strategy

### GitHub

GitHub is used for:

- version control
- transparent workflow inspection
- public access to scripts and documentation
- issue tracking or future updates
- linking code to archived dataset package

GitHub alone is not treated as the preservation repository because it is optimized for collaboration and version control, not long-term archival preservation.

### Zenodo

Zenodo Sandbox is used for:

- archival packaging
- DOI support
- file-level checksums
- citation support
- long-term access practice

For this course project, a Zenodo Sandbox record was created. Because Sandbox DOIs do not resolve permanently.

## Versioning Plan

Current version:

```text
Version 1.0.0
Coverage: 2010–2024
```

Future updates should use semantic versioning:

| Version Type | Example | When Used |
|---|---|---|
| Patch | 1.0.1 | Typo fixes, metadata clarification, no data change |
| Minor | 1.1.0 | Added documentation, new derived output, same source years |
| Major | 2.0.0 | Added new reporting years, changed workflow logic, changed output schema |

## Dissemination Plan

The final submission will be disseminated through:

1. GitHub repository link in the final report.
2. Supplementary ZIP or repository link submitted with the report.
3. Zenodo Sandbox record for metadata and preservation practice.

## Rights and Licensing

The underlying TRI data are public regulatory data from the U.S. EPA. The curated dataset, documentation, and workflow scripts are derivative curation artifacts.

Licensing:

- Documentation and curated dataset: CC BY 4.0

## Preservation Risks and Mitigations

| Risk | Mitigation |
|---|---|
| File corruption | Checksums in fixity manifest |
| Loss of context | README, data dictionary, user guide, metadata JSON |
| Workflow irreproducibility | Ordered scripts, requirements, environment file |
| Repository link rot | Zenodo archival deposit and DOI |
| Misinterpretation of TRI data | Known limitations and data user guide |
| Version confusion | Version field in metadata and CITATION.cff |

