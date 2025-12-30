GDI MINI-NODE
=============

This is a minimal single-app solution to run GDI Node services.

This application covers following GDI APIs:
- FAIR Data Point
- Aggregated Beacon v2 API (`/datasets`, `/g_variants`)
- Sensitive Beacon v2 API (`/individuals`)

It loads data from disk (`./data/` directory), so it has no external
dependencies. However, it also supports
* loading data from an MinIO S3 storage;
* Basic authentication;
* JWT-based user authentication (including GA4GH Visa checking) by specifying an
  OIDC service in the configuration of the target Beacon.

The primary clients for these services are:

* [gdi-userportal-dataset-discovery-service](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service)
  for Beacon endpoints;
* [gdi-userportal-ckanext-fairdatapoint](https://github.com/GenomicDataInfrastructure/gdi-userportal-ckanext-fairdatapoint)
  for FAIR Data Point endpoints.

This project releases its software as Docker images on
[GitHub packages](https://github.com/GenomicDataInfrastructure/gdi-mini-node/pkgs/container/gdi-mini-node).

If you want to share feedback, please
[report an issue](https://github.com/GenomicDataInfrastructure/gdi-mini-node/issues)
or
[start a new discussion](https://github.com/GenomicDataInfrastructure/gdi-mini-node/discussions).

This software is ready for early testing by the GDI members who want to evaluate
alternative solutions to existing FDP and Beacon solutions that heavily depend
on the Mongo database. On contrast, this gdi-mini-node solution runs as a single
application that mostly consumes disk space for its data, which are compressed
Parquet files. This project also includes scripts for generating the Parquet
files from VCFs.

Future developments to this project depend on general interest and individual
contributions. This code belongs to public domain for the sake of the European
Genomic  Data Infrastructure project.


FAIR Data Point
---------------

Implementation under the `mini_node.fdp` module follows standards:

1. [FAIR Data Point](https://specs.fairdatapoint.org/fdp-specs-v1.2.html) v1.2:
   * defines the main entry-point and catalogue navigation,
   * adds requirements for Profiles and SHACLs,
   * requires RDF Turtle (default) and JSON-LD response formats.
2. [GDI metadata v1.1](https://github.com/GenomicDataInfrastructure/gdi-metadata):
   * defines the common metadata model for the GDI catalogues and datasets.
3. [The Profiles Vocabulary](https://www.w3.org/TR/dx-prof/) _18 December 2019_
   * defines vocabulary for referencing data model properties (especially SHACL files),
   * there are endpoints for viewing item-specific profiles.
4. [Shapes Constraint Language (SHACL)](https://www.w3.org/TR/shacl/) _20 July 2017_
   * defines vocabulary for specifying data-model properties and restrictions;
   * `mini_node.fdp.service.shacl` module contains SHACL files;
   * the API includes `.../valid` endpoints for quickly validating current
     entity against its SHACL specification (that is also referenced in its
     profile).

The FDP solution is configurable via multiple files:

1. [config/fdp.yaml](config/fdp.yaml) contains the general attribute values and
   the catalogues (not recursive)
2. [mini_node/fdp/service/shacl/](mini_node/fdp/service/shacl/) contains the
   SHACL files for each entity; they are exposed via API and used for validation
   when the `.../valid` endpoint is called.
3. [mini_node/fdp/service/templates.yaml](mini_node/fdp/service/templates.yaml)
   contains the entity templates, which, combined with metadata, are rendered to
   RDF Graphs and then to the desired output format; that resource paths for the
   FDP are also defined here.
4. [data/*/metadata.yaml](data/) files specify values for `dcat:Dataset`
   entities, which are provided as input to the templates; on validation
   failure (see [config.py](mini_node/fdp/config.py)), the dataset does not
   become visible; the metadata also contains a reference to the catalogue where
   the dataset will be listed.

From the list of configurable files, the SHACL and template files are updated by
developers, _fdp.yaml_ is managed by system deployer, and _metadata.yaml_ files
are either edited manually or loaded into the directory from another
system.

**NOTE**: it is possible to disable the FDP module just by omitting the
_fdp.yaml_ configuration file.


The Beacon APIs
---------------

The solution adapts the Beacon v2 specification only partially, just enough to
comply with the expectations from the GDI User Portal. First of all, the Beacon
specification specifies framework endpoints (all using the `GET` method):

* `/` and `/info` – JSON describing the Beacon;
* `/service-info` – another JSON that follows the GA€GH Service Info spec;
* `/map` – JSON describing available endpoints;
* `/configuration` – JSON describing entry-types, maturity, and security;
* `/entry_types` – JSON describing entry-types;
* `/filtering_terms` – JSON describing supported filtering terms;

These endpoints are the same for all Beacons, except their output JSON is
expected to reflect the purpose and the configuration of the service.

Additional endpoints for the regular Beacon are used by the GDI User Portal to
query for the number of matching individuals:

* `POST /individuals`

This implementation closely aligns with the
[client code](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/5dbf3403bd9751b4c91bb90f9a417c6201b6027f/src/main/java/io/github/genomicdatainfrastructure/discovery/datasets/infrastructure/beacon/persistence/BeaconDatasetIdsCollector.java#L46),
meanwhile it does not actually return any records (which are not used by the
client).

Additional endpoints for the aggregated-data Beacon are used by the GDI User
Portal to find available cohorts from datasets and allele-frequencies for a
variant:

* `POST /datasets`
* `POST /g_variants`

Also in here, this application closely aligns with the
[client code](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/5dbf3403bd9751b4c91bb90f9a417c6201b6027f/src/main/java/io/github/genomicdatainfrastructure/discovery/datasets/infrastructure/beacon/persistence/GVariantsRepository.java#L39),
and just returns the JSON properties actually used by the client.

In summary, this implementation aims for minimal data as requested by the GDI
User Portal, and does not implement the standard Beacon data model. The benefit
is the smaller footprint of the data required for the service. And in this case,
the service relies on [Parquet](https://parquet.apache.org) files stored under
`./data/{datasetId}/{assemblyId}/`, where `assemblyId` is restricted to `GRCh37`
and `GRCh38` (indicates the reference genome that was used for the source VCF
file). These Parquet files are essentially tables that the software uses for
retrieving data for the Beacon responses:

1. input request specifies values for: assembly, chromosome, position,
   reference, alternative, variant type (e.g. `SNP`);
2. the system distinguishes files (paths stored in memory) first by assembly,
   then `chr{CHR}.{GROUP}` string where `GROUP` is an integer obtained from
   integer-division `position // 1_000_000` to indicate position-range;
3. next, the system reads the list of matching files (one per dataset) by going
   to the matching line (POS-REF-ALT-VARIANT_TYPE), and reading response data
   from additional columns;
4. finally, the system composes a Beacon-specific JSON response from the
   collected results.

Since there are two distinguished Beacon types, the set of Parquet files used
for search varies:

* aggregated Beacon uses files with prefix: `allele-freq-`;
* regular Beacon uses files with prefix: `individuals-`;

Since the regular and aggregated-data Beacons are roughly the same, it made
sense to combine the functionality into one app. However, by omitting
corresponding configuration files, it's possible to disable one or both of them.

Beacon configuration is specified in following files:

* [config/beacon-common.yaml](config/beacon-common.yaml) specifies common
  properties for both Beacons (most notably the organisation's info, which every
  deployer needs to update to match their own situation);
* [config/beacon-aggregated.yaml](config/beacon-aggregated.yaml) specifies
  service-level properties for the _aggregated-data Beacon_, including
  authentication requirements; 
* [config/beacon-sensitive.yaml](config/beacon-sensitive.yaml) specifies
  service-level properties for the _regular/sensitive Beacon_, including
  authentication requirements; 

Regarding user-authentication, the Beacon endpoints are public by default.
However, it is possible to activate user authorisation using service-specific
configuration files:

* Basic-authentication – it's possible to define one or more user-credentials.
* OIDC-verifier - specify an OIDC service with client-credentials to enable
  validation of incoming Bearer tokens; includes support for checking GA4GH
  Visas (currently no restriction to dataset specific searches, not sure if it's
  required); this app does not support OIDC authentication flow (expected to be
  covered by the GDI User Portal).


General Configuration
---------------------

There is also another configuration file: [app.yaml](config/app.yaml). It is
always required, and is used for configuring general app features:

* logging,
* the location of the data directory,
* optional Basic authentication for the root path, and
* optional data-syncing with an external S3-service.

Regarding the latter, this mini-node software can be deployed as an independent
public GDI Node API that downloads its data from an S3 bucket. This approach
enables data-production from other systems, as well as S3 file history. Whether
to combine the deployed mini-node software with a persistent volume, is left to
the deployers to decide. The volume may reduce network traffic, especially on
restarts, but on the other hand it duplicates the required disk space.


Getting Started
---------------

There are many ways to run the software. In either way, your entrypoint to the
application is at http://localhost:8080 in the following examples.

```shell
# Via Docker Compose
docker compose build
docker compose up

# Via Docker
docker build -t mininode .
docker run --rm -it -p 127.0.0.1:8080:8000 \
  -v "$PWD/config:/app/config" \
  -v "$PWD/data:/app/data" \
  mininode

# Via Poetry – first install dependencies (into virtual environment)
poetry install
# Runs in fastapi in "dev" mode for fast-reloading changes:
poetry run fastapi --port 8080 dev mini_node
```

The GitHub repo also includes a public Docker image under the packages.

Notes about the Docker image:
* it runs as a user without any permissions (`nobody:nogroup` or `65534:65534`);
* it does not need to write to its image, so it can run read-only;
* it requires 150 MB of RAM at minimum; ensure at least 500 MB.


Data Preparation
----------------

Datasets are laid out under the data-directory as subdirectories where the
directory name is the ID of the dataset. Each such directory is expected to
contain a valid **metadata.yaml** file to be considered valid and active.
The YAML file describes the properties that will be exposed in the FAIR Data
Point and also by the Beacon datasets endpoint.

Next to the YAML file, a directory with the name of a genome assembly (GRCh37,
GRCh38) is expected to contain Parquet files for one of the Beacon services.

For aggregated Beacon, the files are named with this pattern:
`allele-freq-chr{1-22,X,Y,M}.{0,1,2,3,...}.parquet`, and they contain columns:
 1. `POS` (int32) - VCF position - 1;
 2. `REF` – REF-value from VCF;
 3. `ALT` – ALT-value from VCF;
 4. `VT` (string) - variant-type from VCF, typically "SNP";
 5. `POPULATION` (string) – label for the population (2-letter country code,
    optionally a gender suffix after an underscore);
 6. `AF` (float64) - allele-frequency value;
 7. `AC` (int32) – allele-count value from VCF;
 8. `AC_HET` (int32) – heterozygous allele-count value from VCF;
 9. `AC_HOM` (int32) – homozygous allele-count value from VCF;
10. `AC_HEMI` (int32) – hemizygous allele-count value from VCF;
11. `AN` (int32) – allele-number value from VCF.

The software uses these files to support User Portal allele frequency search for
a specific variant (within given assembly).

For sensitive Beacon, the files have name with this pattern:
`individuals-chr{1-22,X,Y,M}.{0,1,2,3,...}.parquet`, and they contain columns:

1. `POS` (int32) - VCF position - 1;
2. `REF` – REF-value from VCF;
3. `ALT` – ALT-value from VCF;
4. `VT` (string) - variant-type from VCF, typically "SNP";
5. `INDIVIDUALS` (string) – references to individuals by numbers 0, 1, 2, 3,
   ..., also possibly hyphen-separated ranges (e.g. "4,11,22-43,50").

For sensitive Beacon, there must also be a file called `individuals.parquet`
about the properties of the individuals: 

1. `INDEX` (int32) - individual identified by a number (0, 1, 2, 3, ...)
2. `SEX` (string) – either `M` (male) or `F` (female) reflecting individual's
   sex, often assigned at birth;
3. `AGE` (string) – ISO 8601 Period (e.g. "P27Y3M14D") reflecting individual's
   age when the genetic sample was taken (year-precision might be sufficient).

The software uses these files to support User Portal dataset discovery
(individual counting) via filtering facets: sex, age, and genomic variant.

Therefore, to register a new dataset:

1. Create a new directory **under the data directory**.
   * The directory name will be used as DATASET ID.
   * This software does not enforce any convention for IDs – it's up to you.
2. Copy an existing **metadata.yaml** into the new directory.
   * Review and update the YAML file.
   * `catalog_id` will affect in which [catalogue](./config/fdp.yaml) the
     dataset will be listed. 
3. Create a nested directory for storing the Parquet files.
   * Depending on the reference genome, pick either **GRCh37** or **GRCh38** as
     the name for the directory.
   * Technically, a dataset can have both reference genome directories, too.
4. Create Parquet files for the dataset to be discoverable through a Beacon.
   * Details about running the script are given below.

**NOTE**: if you enable S3 data-syncing in the mini-node configuration, you
would  create the dataset directory with the content on a computer/server where
you have the input VCF files and Python script for generating the Parquet files.
Once the directory is ready, you would upload it to the S3 bucket where the
mini-node instance automatically detects and downloads them. Of course, this
dataset directory preparation and upload to S3 can also be automated using an
additional software (which is currently out of the scope of this solution). The
mini-node instance would require access to the S3 bucket with this policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:ListenBucketNotification"
      ],
      "Resource": "arn:aws:s3:::BUCKET_NAME"
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::BUCKET_NAME/*"
    }
  ]
}
```


### Generating Allele-Frequency Files from VCFs

The main overview about the process of exposing allele-frequency info via Beacon
API is well described here:
https://docs.google.com/document/d/1LLzp6zZT3fSM1XxOXHuRqwJje1v726Z2/edit.

The input comes from one or more (e.g. per chromosome) VCF files containing
allele-frequency annotations (producing these files is not covered here). As
searching from VCF files is quite slow, the mini-node solution extracts minimal
required data from the VCFs and stores them in Parquet files, which serve as
lookup tables for serving Beacon API response. To generate the Parquet files,
launch the script as follows:

```shell
poetry run python3 parquet-writer allele-freq \
  -d data/DATASET_ID/GRCH37/ 
  -c CY \
  /some/dir/af_data_chr*.vcf.gz
```

* `-d` flag is used to specify the destination directory (in this case:
  `data/DATASET_ID/GRCH37/`). Without it, the destination defaults to current
  directory.
* `-c` flag is required to specify two-letter country code for cohort; if the
  VCF already contains INFO fields with a country code, it must be the same
  country code.
* There can be many VCF files as long as their chromosome-position pairs don't
  overlap. The script processes multiple files in parallel.
* Depending on the size of the VCF files, the process may take tens of minutes
  or even hours. It does not require a lot of memory, but it is CPU intensive.

The destination directory will contain files
**individuals-chr{1-22,X,Y}.{0,1,2,3,...}.parquet** based on the VCF data.


### Generating Individual-Based Files from VCFs

Finding datasets by individual count is used by the GDI User Portal when
filtering datasets
([left-side filters](https://portal.dev.gdi.lu/datasets?page=1) that are visible
for authenticated users). More precisely, the actual client is
[gdi-userportal-dataset-discovery-service](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/feat/g-variant-does-not-require-auth/src/main/java/io/github/genomicdatainfrastructure/discovery/datasets/infrastructure/beacon/persistence/BeaconDatasetIdsCollector.java),
which collects responses via Beacon Network and forwards extracted information
to the frontend of the GDI User Portal.

The endpoint needs to be able to filter individuals by
* genomic variant
* sex
* age-range

The whole list of Beacon filters is provided
[here](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/resources/META-INF/resources/beacon-filters.json).
However, the current implementation does not support filtering by diseases, so
these requests will get zero counts.

For preparing the data, the following input is required:
* VCF(s) with genomic variants and samples (expecting only one sample per
  individual);
* a CSV-file containing columns (the column-names on the first line):
  - `SAMPLE` – corresponding sample code in the VCF,
  - `SEX` – either `M` (male) or `F` (female),
  - `AGE` – individual's age when the specimen was taken; expressed as ISO 8601
    period (e.g. "P49Y" – "49 years", "P23Y9M" – "23 years, 9 months");
    precision of the value is not enforced by the system.

Once the files are prepared, this script produces several Parquet files:

```shell
poetry run python3 parquet-writer individuals
  -d data/DATASET_ID/GRCH37/ \
  -i /some/dir/individuals.csv \
  /some/dir/ind_chr*.vcf.gz
```

* `-d` flag is used to specify the destination directory (in this case:
  `data/DATASET_ID/GRCH37/`). Without it, the destination defaults to current
  directory.
* `-i` flag is used to specify the input CSV about individuals; this file is
  mandatory; using semi-comma as the field-value separator is recommended.
* There can be many VCF files as long as their chromosome-position pairs don't
  overlap. The script processes multiple files in parallel.
* Depending on the size of the VCF files, the process may take tens of minutes
  or even hours. It does not require a lot of memory, but it is CPU intensive.

The destination directory will contain files:
1. **individuals.parquet** – based on the CSV data;
2. **individuals-chr{1-22,X,Y}.{0,1,2,3,...}.parquet** – based on the VCF data.

As the metadata requires some statistics about the individuals (count, min/max
age), these can be obtained using this command:

```shell
poetry run python3 parquet-writer summary -i /some/dir/individuals.parquet
```
