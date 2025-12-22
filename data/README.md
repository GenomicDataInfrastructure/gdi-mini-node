# Sample Data And Beacon Queries

The directory with this README-file is the default location for the datasets as
specified in **config/app.yaml** (`data_dir`). This directory is included in the
Docker Compose deployment, however, the data is not part of the Docker image.

This directory also contains two sample datasets for a quick start. The sample
data uses a  section of
[chromosome 3 from the 1000 genomes project](https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr3.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz)
to cover the famous `3-45864731-T-C` variant query used a sample input for the
[allele frequency query](https://portal.dev.gdi.lu/allele-frequency).
The VCF file (as well as the dataset of individuals) contains 2504 samples, and
`AF`, `AC`, and `AN` annotations about the allele-frequencies. 

Parquet files for these datasets were generated using the Parquet writer:

```shell
poetry run python3 parquet-writer allele-freq -c DEMO path/to/vcf
poetry run python3 parquet-writer individuals -i RANDOM path/to/vcf
```

The `RANDOM` input value triggered the `parquet-writer` to generate random `SEX`
and `AGE` values for each individual. So these will be different with each
generation.

Beacon request examples below do not cover authentication. If you have
authentication  enabled, just provide the `Bearer` token to the `AUTHORIZATION`
header. In case of `Basic` authentication, the credentials can be provided as
`curl -u "username:password" <other-curl-arguments...>`.

## General Instructions

The minimal Beacon API does not support GET-requests for data-endpoints, only
the POST method can be used. Therefore, JSON as the request body is expected.
For more convenient testing, save the request JSON to file that you refer from
the command-line. In addition, every request must also send the `Content-Type`
header to let the server know that the uploaded content is JSON. Therefore, if
the payload is saved in file `request.json`, the request can be invoked using
`curl`:

```shell
curl -s -H "Content-Type: application/json" -d @request.json \
  http://localhost:<port-number>/beacon/<some-endoint>
```

Due to the payload from a file, the POST method is automatically used here.
Also note that the `@` in front of the filename is mandatory as it informs curl
that the value is to be taken from the file.

For formatting the received JSON, additional command-line program called `jq`
can be used after the curl command: _/curl .../_ ` | jq`.

Typically, the Beacon query has the following structure:

```json
{
  "meta": {
    "apiVersion": "2.0",
    "requestedSchemas": [
      {
        "entityType": "genomicVariation",
        "schema": "https://raw.githubusercontent.com/ga4gh-beacon/beacon-v2/main/models/json/beacon-v2-default-model/genomicVariations/defaultSchema.json"
      }
    ]
  },
  "query": {
    "testMode": false,
    "requestedGranularity": "record",
    "includeResultsetResponses": "HIT",
    "filters": [],
    "requestParameters": {},
    "pagination": {
      "limit": 10,
      "skip": 0
    }
  }
}
```

which can be shortened, if the defaults are fine, to the following:

```json
{
  "meta": {
    "apiVersion": "2.0"
  },
  "query": {
    "filters": [],
    "requestParameters": {}
  }
}
```

Good to know:
* `meta.requestedSchemas` is never evaluated by the server;
* due to server-side configuration, `requestedGranularity` defaults to `record`.
* if you want any results, don't assign different values to these parameters:
  * `query.testMode=false`
  * `query.includeResultsetResponses='HIT'`
* be aware that unsupported filters will also trigger empty responses.

JSON examples below follow the minimal required format.


## Allele Frequency Beacon

Regarding this GDI feature, there are 2 endpoints for aggregated data:

* `POST /beacon/aggregated/v2/g_variants`
* `POST /beacon/aggregated/v2/datasets`

None of these endpoints support the GET method, as
[the client uses only POST](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/openapi/beacon/gvariants.yaml).
(See also how the
[server-side client request](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/java/io/github/genomicdatainfrastructure/discovery/datasets/infrastructure/beacon/persistence/BeaconGVariantsRequestMapper.java)
and the
[frontend-side parameters](https://github.com/GenomicDataInfrastructure/gdi-userportal-frontend/blob/fc01aa8db789cabf5b46048141c9d3a13d8d05c1/src/app/allele-frequency/page.tsx#L45)
are constructed.)

For retrieving allele-frequencies per dataset, use this input:

```json
{
  "meta": {
    "apiVersion": "2.0"
  },
  "query": {
    "requestParameters": {
      "assemblyId": "GRCh38",
      "referenceName": "22",
      "start": "40016503",
      "referenceBases": "T",
      "alternateBases": "A"
    }
  }
}
```

Notes:
* `query.requestParameters.variantType` defaults to `SNP` as the most common
  use-case; sometimes `INDEL` can also be found in the data.
* the value of `query.requestParameters.end` is ignored. 
* don't use other Beacon API genomic-variant parameters (e.g. `geneId`) as these
  will trigger an empty response.
* there will be an empty response when `requestParameters` is fully omitted or
  only part of the shown parameters is present.

The response will look like (major noisy parts removed):

```json
{
  "meta": {
    "beaconId": "org.example.aggregated.beacon.v2",
    "apiVersion": "v2.2.0",
    "receivedRequestSummary": {},
    "returnedSchemas": [
      {
        "entityType": "genomicVariant",
        "schemaUrl": "https://raw.githubusercontent.com/ga4gh-beacon/beacon-v2/v2.2.0/models/json/beacon-v2-default-model/genomicVariations/defaultSchema.json"
      }
    ],
    "returnedGranularity": "record"
  },
  "responseSummary": {
    "exists": true,
    "numTotalResults": 1
  },
  "response": {
    "resultSets": [{
      "id": "GDIDEMO20251201001D",
      "setType": "dataset",
      "exists": true,
      "resultsCount": 1,
      "results": [{
        "frequencyInPopulations": [{
          "source": "The Genome of Europe",
          "sourceReference": "https://genomeofeurope.eu/",
          "numberOfPopulations": 1,
          "populations": [{
            "population": "DEMO",
            "alleleFrequency": 0.000199681002413854,
            "alleleCount": 1,
            "alleleNumber": 5008
          }]
        }]
      }]
    }]
  }
}
```

Currently, the `/datasets` endpoint has not been implemented on the client-side,
but technically the data can be requested with this payload: 

```json
{
  "meta": {
    "apiVersion": "2.0"
  },
  "query": {
    "pagination": {
      "limit": 100000
    }
  }
}
```

The shortened response appears following:

```json
{
  "meta": {},
  "responseSummary": {
    "exists": true,
    "numTotalResults": 1
  },
  "response": {
    "collections": [
      {
        "id": "GDIDEMO20251201001D",
        "name": "Minimal aggregated data from the 1000 Genomes project",
        "description": "This dataset covers only a part of chromosome 3 and is provided only for technical testing.",
        "createDateTime": "2025-12-01T13:31:27Z",
        "updateDateTime": "2025-12-01T13:33:27Z"
      }
    ]
  }
}
```

This endpoint does not take any parameters nor filters. Providing them will not
affect the response (this might change in the future once the client-side
implementation is ready).

Note that the returned dataset information is the same as in the FDP.

```shell
curl http://localhost:8080/fairdp/dataset/GDIDEMO20251201001D
```

## Individuals (Count)

For retrieving the count of matching individuals, there is only one endpoint in
the sensitive Beacon:

* `POST /beacon/sensitive/v2/individuals`

It supports filtering by
* `sex` (filter) values: `NCIT:C16576` (male) and `NCIT:C16576` (female)
* `diseases.ageOfOnset.iso8601duration` (filter) values (ISO 8601 period) and operator (`<`, `>`, `<=`, `>=`, `=`, `!`, )
* genomic variant (request-parameters)

The [full list of possible filters](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/resources/META-INF/resources/beacon-filters.json)
has more options. If there is a need to support them, the current mini-node
implementation has to be extended (take more Parquet files for input).

A full example of the request for finding the number of individuals that match
ALL of the following criteria:
* males (`sex` = `NCIT:C16576`)
* age over 80 years (`diseases.ageOfOnset.iso8601duration` `>=` `P80Y`)
* specific variant (`GRCh38` – `22:40016504T>A` – note that Beacon API uses
  `<position value> - 1`)

```json
{
  "meta": {
    "apiVersion": "2.0"
  },
  "query": {
    "filters": [{
      "id": "sex",
      "value": "NCIT:C16576",
      "scope": "individual"
    }, {
      "id": "diseases.ageOfOnset.iso8601duration",
      "value": "P80Y",
      "operator": ">=",
      "scope": "individual"
    }],
    "requestParameters": {
      "assemblyId": "GRCh38",
      "referenceName": "22",
      "start": "40016503",
      "referenceBases": "T",
      "alternateBases": "A"
    },
    "pagination": {
      "limit": 100
    }
  }
}
```

The following is a shortened-version of a typical response, which lists
result-sets with the count of individuals only per matching dataset:

```json
{
  "meta": {},
  "responseSummary": {
    "exists": true,
    "numTotalResults": 1
  },
  "response": {
    "resultSets": [
      {
        "id": "GDIDEMO20251202001D",
        "setType": "dataset",
        "exists": true,
        "resultsCount": 2504,
        "results": []
      }
    ]
  }
}
```

Notes:
* it is possible to omit all filters and request-parameters to get the total
  counts per dataset;
* for each filter, don't forget to specify `"scope": "individual"` (required);
* other filters than sex and age are not supported and will lead to an empty
  result;
* variant-parameters are validated as with allele-frequency requests, except
  that they can be totally omitted here;
* although, the response is at `record`-level, it always returns an empty list
  at `response.resultSets[].results`
  ([the client code does not use it](https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/java/io/github/genomicdatainfrastructure/discovery/datasets/infrastructure/beacon/persistence/BeaconDatasetIdsCollector.java))
* the response omits datasets where `response.resultSets[].resultsCount == 0`.


## Closing Notes

This document was written at the end of 2025 and was based on the current
understanding of the required node-provided endpoints. This is not something
that can be considered the final version that will be required by October 2026.
Perhaps the referenced source code (in GitHub) can be the main source of truth
about the latest API requirements.
