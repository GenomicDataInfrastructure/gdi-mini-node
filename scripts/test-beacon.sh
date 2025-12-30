#!/usr/bin/env bash
set -eu

# Simple test-script for verifying Beacon API endpoints.

# The base_url can be provided as a command-line argument, here is the default:
base_url="${1:-http://localhost:8000/beacon/aggregated/v2}"
#base_url="http://localhost:8000/beacon/sensitive/v2"

function get_url() {
    full_url="$base_url$1"
    echo "Calling: GET $full_url"
    curl -s "$full_url" | jq
    read -p "Press enter to continue"
    echo
}

function post_url() {
    full_url="$base_url$1"
    echo "Calling: POST $full_url"
    curl -s -H "Content-Type: application/json" -d @request.json "$full_url" | jq
    read -p "Press enter to continue"
    echo
}

get_url "/"
get_url "/info"
get_url "/service-info"
get_url "/map"
get_url "/configuration"
get_url "/entry_types"
get_url "/filtering_terms"


if [[ $base_url == *"/aggregated/"* ]]; then
  cat > request.json <<EOF
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
EOF
  post_url "/datasets"

  cat > request.json <<EOF
{
  "meta": {
    "apiVersion": "2.0"
  },
  "query": {
    "requestParameters": {
      "assemblyId": "GRCh37",
      "referenceName": "3",
      "start": "45864731",
      "referenceBases": "T",
      "alternateBases": "C"
    },
    "pagination": {
      "limit": 100
    }
  }
}
EOF
  post_url "/g_variants"

else  # Sensitive Beacon

  cat > request.json <<EOF
{
  "meta": {
    "apiVersion": "2.0"
  },
  "query": {
    "filters": [{
      "id": "sex",
      "value": "NCIT:C16576",
      "scope": "individual"
    }],
    "requestParameters": {
      "assemblyId": "GRCh37",
      "referenceName": "3",
      "start": "40015127",
      "referenceBases": "G",
      "alternateBases": "A"
    },
    "pagination": {
      "limit": 100
    }
  }
}
EOF
  post_url "/individuals"

fi

echo "Testing completed!"
