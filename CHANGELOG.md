# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases and associated Docker images are published on
[GitHub](https://github.com/GenomicDataInfrastructure/gdi-mini-node).


## [Unreleased]

### Added

### Changed

### Fixed


## 0.9.0

Initial version to demonstrate a **FAIR Data Point**, **aggregated Beacon**, and
**sensitive Beacon** in a single app solution. This is an early release that
aims to match the current API specification but is not yet 100% compliant nor
verified. This release is considered stable for early evaluation and testing.

### Features

#### FAIR Data Point

- FAIR Data Point v1.2,  GDI metadata v1.1
- configurable properties, templates, and dataset-metadata files.
- initial templates for FAIRDataPoint, MetadataService, Catalog, Dataset
  (including Distribution). 


#### Aggregated Beacon

- Beacon API specification v2.2
- `POST /g_variants` – allele frequencies per cohort
- `POST /datasets` – list of available datasets
- data from `{DATASET_ID}/{ASSEMBLY_ID}/allele-freq-chr*.parquet` files
- configurable metadata and security (including OIDC, GA4GH Passport & Visas)


#### Sensitive Beacon

- Beacon API specification v2.2
- `POST /individuals` – for counting individuals
- data from `{DATASET_ID}/{ASSEMBLY_ID}/individuals-chr*.parquet` files
- configurable metadata and security (including OIDC, GA4GH Passport & Visas)


#### General

- Landing page that reflects the current state of data in the app instance. 
- `GET /health` endpoint for health-checks.
- Optional Basic authentication for the landing page.
- Optional data-syncing from an S3 storage.
- Inspection of data-directory for dataset metadata and Beacon Parquet files.
- Optional data syncing from an S3 storage into the local data directory.
- Discovered dataset details are stored in memory for quick access.
- README instructions to start with deployment.
- Initial CONTRIBUTING guide.
