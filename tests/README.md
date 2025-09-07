# Tests

This directory contains the automated test suite for the FlexibleHours project.

## Test Structure

- `test_processFile_Local_AI.py`: Main test file that covers all functionalities of the `src/processFile_Local_AI.py` script

## Running Tests

To run all tests, you can use the main script:

```bash
./run_tests.sh
```

Or run directly with pytest:

```bash
python -m pytest tests/ -v
```

### Running Tests with Coverage

To run tests with code coverage collection:

```bash
./run_tests.sh --coverage
```

Or using coverage commands directly:

```bash
coverage run -m pytest tests/
coverage report
coverage html
```

## Test Description

### Unit Tests

Unit tests verify the correct operation of individual functions in the main script:

- `test_condense_description_*`: Tests the long description condensation function
- `test_build_flexibility_prompt`: Verifies the correct creation of the prompt for the Ollama API
- `test_safe_parse_json_*`: Tests the safe JSON parsing function
- `test_validate_response_*`: Verifies API response validation
- `test_yesno_to_dummy`: Tests the conversion of YES/NO values to 1/0

### Batch Processing Tests

These tests verify the correct operation of the batch saving system:

- `test_load_existing_batches_*`: Tests loading existing batches for processing resume
- `test_save_batches_*`: Verifies correct batch saving

### Integration Tests

These tests verify the complete job analysis process:

- `test_process_job_postings_resume`: Tests resuming processing from existing batches
- `test_process_job_postings_resume_incomplete`: Verifies correct resume after an interruption

## Mocks and Stubs

Tests use mocks to simulate Ollama API calls and isolate test units. This allows:

- Running tests quickly and deterministically
- Testing specific scenarios without depending on external services
- Verifying code behavior in case of API failures

## Contributing to Tests

When adding new features to the project, make sure to include appropriate tests covering the new use cases. Follow the existing pattern in the current tests.
