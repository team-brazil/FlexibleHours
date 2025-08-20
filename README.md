# Job Posting Flexibility Analysis Project

## 1. Overview

This project utilizes a local Large Language Model (LLM), Llama 3, to analyze job descriptions. The goal is to classify schedule flexibility into two main categories:

* **Undesirable Flexibility:** The schedule is controlled by the company to meet its needs (e.g., rotating shifts, "as needed" work, mandatory on-call).
* **Desirable Flexibility:** The employee has autonomy to set their own work schedule.

The system is designed to be robust, processing data in batches and ensuring that the results are auditable and easy to analyze.

## 2. Prerequisites

Before you begin, ensure you have the following software installed:

* **Python 3.8+**
* **Ollama:** To run the LLM locally. Follow the installation instructions on the [official Ollama website](https://ollama.com/).

## 3. Environment Setup

Follow these steps to set up the project environment on your local machine.

### a. Install the AI Model

After installing Ollama, open your terminal and run the following command to download the Llama 3 8B model used in this project:

```bash
ollama pull llama3:8b
```

### b. Clone the Repository (if applicable)

If the project is in a git repository, clone it. Otherwise, just ensure you have the project folder.

### c. Create a Virtual Environment

It is a best practice to use a virtual environment to isolate project dependencies.

```bash
# Create the virtual environment
python -m venv venv

# Activate the environment (on macOS/Linux)
source venv/bin/activate

# Activate the environment (on Windows)
.\venv\Scripts\activate
```

### d. Install Dependencies

Create a file named `requirements.txt` in the project's root directory with the following content:

```
pandas
openpyxl
httpx
tqdm
```

Then, install these dependencies using pip:

```bash
pip install -r requirements.txt
```

## 4. Folder Structure

The project expects the following directory structure:

```
/flexibility-analysis-project
|
├── input/
│   └── us_postings_sample.xlsx  <-- PLACE YOUR INPUT FILE HERE
|
├── output/
│   └── results/                 <-- RESULTS WILL BE SAVED HERE
|
├── venv/
├── processFile_Local_AI.py      <-- MAIN SCRIPT
└── README.md
```

## 5. How to Run the Analysis

1.  **Place your data file:** Add the Excel file (`.xlsx`) you want to analyze into the `input/` folder. Ensure the filename matches the one configured in the `INPUT_DIR_NAME_FILE` variable inside the `processFile_Local_AI.py` script.
2.  **Start the Ollama Service:** Make sure the Ollama application is running on your machine. The script needs it to communicate with the AI model.
3.  **Run the script:** With your virtual environment activated, navigate to the project folder in your terminal and execute the main script:

    ```bash
    python processFile_Local_AI.py
    ```

4.  **Track the progress:** A progress bar will be displayed in the terminal. The script will save batch files (`batch_temp_*.xlsx`) and a log file (`process_log_*.txt`) in the `output/results/` folder.

## 6. Output

At the end of the process, you will find the following files in the `output/results/` folder:

* `Job_postings_processed_llama3:8b.xlsx`: The final, consolidated file with all results. The rows are color-coded for easy identification:
    * **Red:** Classified as "Undesirable Flexibility".
    * **Green:** Not classified as "Undesirable Flexibility".
* `process_log_*.txt`: A detailed log file with information about the execution, including warnings and errors.

## 7. Next Steps: Improving the Prompt

The main task now is to refine the prompt sent to the AI to improve classification accuracy. The process is iterative:

1.  **Run the analysis** on a sample set.
2.  **Manually review** the output Excel file, focusing on the `undesired_quote` and `reasoning` columns to understand the AI's decisions.
3.  **Identify common errors:** Look for patterns in mistakes (e.g., false positives, false negatives, misinterpretations of certain terms).
4.  **Adjust the prompt:** Modify the `build_flexibility_prompt` function in the `processFile_Local_AI.py` script. You can add clearer examples, refine definitions, or include explicit instructions to avoid the identified errors.
5.  **Repeat the cycle** until the accuracy rate is satisfactory.
