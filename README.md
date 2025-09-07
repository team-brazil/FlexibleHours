# FlexibleHours

Project for analyzing flexibility in job postings using local artificial intelligence.

## Description

This project analyzes job postings to identify unwanted flexibility requirements (such as variable shifts, on-call duties, etc.) and desired flexibility (such as flexible hours chosen by the employee). It uses a local language model (Ollama) to process job descriptions.

## Features

- Processing of CSV and XLSX files containing job postings
- Flexibility analysis using local AI (Ollama)
- Job classification based on predefined criteria
- Generation of reports in Excel format with conditional coloring
- Resume processing functionality from interruption points
- Batch saving to prevent data loss in case of interruption

## Requirements

- Python 3.6+
- Ollama (with qwen3:8b model or similar)
- Pandas
- OpenPyXL
- httpx
- tqdm

## Installation

1. Clone the repository:

   ```
   git clone <repository-url>
   cd FlexibleHours
   ```
2. Create a virtual environment and activate it:

   ```
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:

   ```
   pip install -r requirements.txt
   ```
4. Install Ollama and download the required model:

   ```
   # Follow instructions at https://ollama.ai to install Ollama
   ollama pull qwen3:8b
   ```

## Usage

1. Make sure Ollama is running:

   ```
   ollama serve
   ```
2. Run the main script:

   ```
   ./run_process.sh
   ```

   Or directly with Python:

   ```
   python src/processFile_Local_AI.py
   ```

## Project Structure

```
FlexibleHours/
├── src/
│   └── processFile_Local_AI.py     # Main script
├── input/
│   ├── 1000_unit_lightcast_sample.csv  # Sample file
│   └── us_postings_sample.xlsx         # Sample file
├── output/
│   └── results/                        # Processing results
├── logs/                               # Log files
├── tests/                              # Automated tests
├── requirements.txt                    # Project dependencies
├── requirements-dev.txt                # Development dependencies
├── run_process.sh                      # Script to run processing
├── run_tests.sh                        # Script to run tests
└── README.md                           # This file
```

## Tests

The project includes a comprehensive suite of automated tests. To run the tests:

```
./run_tests.sh
```

### Code Coverage

The project is configured to generate code coverage reports. The coverage configuration is defined in the `.coveragerc` file.

To run tests with coverage collection, use:

```
./run_tests.sh --coverage
```

This command will:

1. Run tests with coverage collection
2. Generate a text report in the terminal
3. Generate an HTML report in the `htmlcov/` directory

Alternatively, you can use coverage commands directly:

```
coverage run -m pytest tests/
coverage report
coverage html
```

Coverage files (`.coverage`, `htmlcov/`) are not versioned and are included in `.gitignore`.

For more information about tests, see [tests/README.md](tests/README.md).

## Configuration

The main settings are at the beginning of the `src/processFile_Local_AI.py` file:

- `INPUT_DIR_NAME_FILE`: Path to the input file
- `OLLAMA_URL`: Ollama server URL
- `MODEL_NAME`: Name of the model to be used
- `OUTPUT_PATH`: Output directory
- `BATCH_SIZE`: Number of records per batch

## Script Operation

### Main Script (`src/processFile_Local_AI.py`)

The main script performs the following operations:

1. **File Processing**: Reads CSV or XLSX files containing job postings
2. **Flexibility Analysis**: Uses the Ollama API to analyze job descriptions and classify them by flexibility
3. **Processing Resume**: Allows resuming processing from the previous interruption point
4. **Batch Saving**: Saves intermediate results in batches to prevent data loss
5. **Report Generation**: Creates reports in Excel format with conditional coloring

### Test Script (`tests/test_processFile_Local_AI.py`)

The test script includes:

1. **Unit Tests**: Tests all main functions of the script
2. **Integration Tests**: Tests the complete job analysis process
3. **Resume Tests**: Verifies the processing resume functionality
4. **Mocks**: Uses mocks to simulate Ollama API calls

## License

Distributed under the MIT license. See `LICENSE` for more information.
