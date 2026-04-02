#!/bin/bash
# Deploy all Prefect flows (Updated for Prefect 3.0+)

set -e

echo "Deploying Prefect flows..."

# Staging flows
echo "Deploying staging flows..."
prefect deploy ../flows/staging/staging_current_geography.py:staging_current_geography --name "staging-geography"
prefect deploy ../flows/staging/staging_current_demographics.py:staging_current_demographics --name "staging-demographics"
prefect deploy ../flows/staging/staging_current_labels.py:staging_current_labels --name "staging-labels"

# Transformation flows
echo "Deploying transformation flows..."
prefect deploy ../flows/transformation/transformation_current_dim_geography.py:transformation_current_dim_geography --name "transformation-dim-geography"
prefect deploy ../flows/transformation/transformation_current_fact_demographics.py:transformation_current_fact_demographics --name "transformation-fact-demographics"
prefect deploy ../flows/transformation/transformation_current_labels.py:transformation_current_labels --name "transformation-labels"

# Test Pipeline
prefect deploy ../flows/french_towns_pipeline.py:french_towns_pipeline --name "french_towns_pipeline"

echo "All flows deployed! View them at http://localhost:4200/deployments"
