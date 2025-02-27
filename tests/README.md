
## Running the Tests

To run the test suite, follow these steps:

1. **Install Dependencies**: Ensure that all dependencies from pyproject.toml are installed by running:
   ```bash
   uv pip install -e .
   ```

2. **Change To Tests Directory**: 
   ```bash
   cd tests
   ```
2. **Run Tests**: Execute the tests using pytest:
   ```bash
   pytest
   ```

## Test Coverage

The test suite covers the following areas:

- **Data Extraction**: Tests for data fetching from various APIs (e.g., Football-data.org, Open-Meteo, Reddit).
- **Data Transformation**: Tests for data processing and transformation using Dataform.
- **Data Storage**: Tests for data storage in GCS and BigQuery.
- **Serving Layer**: Tests for loading Data to Firestore.

## Continuous Integration

The project uses GitHub Actions for continuous integration and deployment. The CI and CD pipelines are defined in the `.github/workflows` directory and include workflows for running tests as well.

---------------------------------------------------------------------------
For more details, refer to the main [README](https://github.com/peter115342/soccer-tracker-DE-project/blob/main/README.md) of the project.
