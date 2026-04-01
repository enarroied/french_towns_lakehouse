#!/bin/bash
# Deploy all Prefect flows

set -e

echo "Deploying Prefect flows..."

# Staging flows
echo "Deploying staging flows..."
prefect deployment build \
    flows/staging/staging_current_geography.py:staging_current_geography \
    --name "staging-geography" \
    --apply

prefect deployment build \
    flows/staging/staging_current_demographics.py:staging_current_demographics \
    --name "staging-demographics" \
    --apply

prefect deployment build \
    flows/staging/staging_current_labels.py:staging_current_labels \
    --name "staging-labels" \
    --apply

# Transformation flows
echo "Deploying transformation flows..."
prefect deployment build \
    flows/transformation/transformation_current_dim_geography.py:transformation_current_dim_geography \
    --name "transformation-dim-geography" \
    --apply

prefect deployment build \
    flows/transformation/transformation_current_fact_demographics.py:transformation_current_fact_demographics \
    --name "transformation-fact-demographics" \
    --apply

prefect deployment build \
    flows/transformation/transformation_current_labels.py:transformation_current_labels \
    --name "transformation-labels" \
    --apply

echo "All flows deployed! View them at http://localhost:4200/deployments"
