## Workflows Overview

### Continuous Integration (CI) Pipeline

The CI pipeline is triggered on push events to the `dev` branch. It ensures that the code is properly built, tested, and meets quality standards.

#### Workflow File: `ci.yml`

#### Trigger
- On push to the `dev` branch

#### Jobs
- **build-and-deploy**
  - Runs on: `ubuntu-latest`
  - Steps:
    - Checkout the repository
    - Set up Python 3.12
    - Install `uv`
    - Create a virtual environment using `uv`
    - Install dependencies
    - Run type checking with `mypy`
    - Run linting with `ruff`
    - Run security analysis with `Bandit`
    - Authenticate to Google Cloud
    - Set up Google Cloud SDK

### Continuous Deployment (CD) Pipeline

The CD pipeline is triggered on push events to the `main` branch. It handles the deployment of cloud functions and infrastructure setup using Terraform.

#### Workflow File: `cd.yml`

#### Trigger
- On push to the `main` branch

#### Jobs
- **build-and-deploy**
  - Runs on: `ubuntu-latest`
  - Steps:
    - Checkout the repository
    - Set up Python 3.12
    - Install `uv`
    - Create a virtual environment using `uv`
    - Install dependencies
    - Run type checking with `mypy`
    - Run linting with `ruff`
    - Run security analysis with `Bandit`
    - Authenticate to Google Cloud
    - Set up Google Cloud SDK
    - Run tests with environment variables
    - Prepare and upload cloud functions to Google Cloud Storage
    - Set up Terraform
    - Initialize Terraform
    - Import Google Cloud resources into Terraform
    - Plan and apply Terraform changes

## Secrets

Both workflows rely on several secrets for authentication and configuration. These secrets should be configured in the repository settings:
- `GCP_SA_KEY`
- `GCP_PROJECT_ID`
- `FUNCTIONS_BUCKET_NAME`
- `DEFAULT_SERVICE_ACCOUNT`
- `DISCORD_WEBHOOK_URL`
- `BUCKET_NAME`
- `API_FOOTBALL_KEY`
- `GOOGLE_MAPS_API_KEY`
- `REDDIT_CLIENT_ID`
- `REDDIT_CLIENT_SECRET`
- `DATAFORM_REPOSITORY`
- `DATAFORM_WORKSPACE`

## Cloud Functions

The CD pipeline deploys multiple cloud functions for different purposes, including data validation, fetching and processing league, match, weather, and standings data, Reddit data processing, and syncing data to Firestore.

## Terraform Resources

Terraform is used to manage the infrastructure, more details [here](https://github.com/peter115342/soccer-tracker-DE-project/tree/main/terraform)

--------------------------------------------------------

For more details, refer to the main [README](https://github.com/peter115342/soccer-tracker-DE-project/blob/main/README.md) of the project.
