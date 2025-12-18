import os
import time
import json
import glob
import httpx
import logging
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import pyarrow.parquet as pq


# === CONFIGURATION ===
# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Build paths relative to the script directory
INPUT_DIR = os.path.join(SCRIPT_DIR, "..", "input")
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen3:8b"
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "output", "results")
LOG_PATH = os.path.join(SCRIPT_DIR, "..", "logs")
# FINAL_FILE_PATH will be determined per file
temperature = 0

NUM_PREDICT = 200
MAX_RETRIES = 2
RETRY_SLEEP = 3
BATCH_SIZE = 20   # Save every N records
BATCH_SAVE_DIR = os.path.join(OUTPUT_PATH, "batch_temp")
os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

LOG_FILE = os.path.join(LOG_PATH, f"process_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE)
        # Removed StreamHandler so logs don't appear in terminal
    ]
)


def read_file_adaptive(file_path, **kwargs):
    """
    Reads input files adaptively, automatically detecting the file format
    and using the appropriate pandas function to load the data as a dataframe.
    
    Parameters:
    - file_path: Path to the file to be read
    - **kwargs: Additional parameters to pass to specific reading functions
    
    Returns:
    - pandas DataFrame with the file data
    
    Supports formats:
    - CSV (.csv)
    - Excel (.xls, .xlsx, .xlsm, .xlsb, .odf, .ods, .odt)
    - JSON (.json)
    - Parquet (.parquet, .parq)
    - Pickle (.pkl, .pickle)
    - TSV (.tsv) - as CSV with tab as delimiter
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Get file extension
    file_extension = os.path.splitext(file_path)[1].lower()
    
    # Mapping of extensions to reading functions
    readers = {
        '.csv': pd.read_csv,
        '.tsv': lambda path, **args: pd.read_csv(path, sep='\t', **args),
        '.xlsx': pd.read_excel,
        '.xls': pd.read_excel,
        '.xlsm': pd.read_excel,
        '.xlsb': pd.read_excel,
        '.odf': pd.read_excel,
        '.ods': pd.read_excel,
        '.odt': pd.read_excel,
        '.json': pd.read_json,
        '.parquet': lambda path, **args: pd.read_parquet(path, **args),
        '.parq': lambda path, **args: pd.read_parquet(path, **args),
        '.pkl': pd.read_pickle,
        '.pickle': pd.read_pickle,
    }
    
    # Check if extension is supported
    if file_extension not in readers:
        # Try content detection if extension is unknown
        try:
            return _detect_and_read_by_content(file_path, **kwargs)
        except:
            raise ValueError(f"Unsupported file format: {file_extension}")
    
    # Get the appropriate reading function
    reader_func = readers[file_extension]
    
    try:
        # Call the reading function with additional parameters
        df = reader_func(file_path, **kwargs)
        logging.info(f"File {file_path} read successfully. Format: {file_extension}, Shape: {df.shape}")
        return df
    except Exception as e:
        # Error handling for corrupted files or reading problems
        logging.error(f"Error reading file {file_path}: {str(e)}")
        raise e


def _detect_and_read_by_content(file_path, **kwargs):
    """
    Helper function for content-based file format detection.
    """
    # Try reading the first bytes for format detection
    with open(file_path, 'rb') as f:
        header = f.read(1024)  # Read the first 1024 bytes
    
    # Try to detect JSON format
    header_str = header.decode('utf-8', errors='ignore').strip()
    if header_str.startswith('{') or header_str.startswith('['):
        return pd.read_json(file_path, **kwargs)
    
    # Try to detect CSV/TSV format by common patterns
    if ',' in header_str or '\t' in header_str:
        # Probably CSV or TSV, try reading as CSV by default
        return pd.read_csv(file_path, **kwargs)
    
    # If format cannot be detected, raise exception
    raise ValueError(f"File format not automatically detected: {file_path}")


# ----------- condense_description (kept) ------------
def condense_description(description, window=3, min_length=2000):
    """
    Only condense text if it is really long.
    Keeps lines with keywords and surrounding context.
    """
    if description is None or len(description) < min_length:
        return description if description is not None else ""

    keywords = [
        "schedule", "flexible", "flexibility", "shift", "weekend", "weekends",
        "holiday", "holidays", "night", "nights", "irregular", "on-call",
        "availability", "as needed", "rotating", "rotational", "hours", "hourly",
        "mandatory", "split shift", "split-shift", "variable", "overtime", "call-in",
        "unpredictable", "as required", "required to", "vary", "subject to", "full availability",
        "prn"
    ]
    lines = description.split('\n')
    lines = [l for l in lines if l.strip() != ""]
    keyword_matches = set()
    for i, line in enumerate(lines):
        if any(kw in line.lower() for kw in keywords):
            for j in range(max(0, i - window), min(len(lines), i + window + 1)):
                keyword_matches.add(j)
    if not keyword_matches:
        return description
    return "\n".join(lines[i] for i in sorted(keyword_matches))


# ----------- IMPROVED JSON PROMPT ------------
def build_flexibility_prompt(description):
    return f"""
You are an expert HR analyst specializing in evaluating job posting flexibility requirements. Your task is to analyze the job description and classify the flexibility aspects.

RESPONSE FORMAT:
Respond ONLY in the following exact JSON format:
{{
  "undesired_flexibility": "YES" or "NO",
  "undesired_quote": "exact quote or 'N/A'",
  "desired_flexibility": "YES" or "NO",
  "desired_quote": "exact quote or 'N/A'",
  "reasoning": "Your step-by-step reasoning, maximum 200 characters"
}}

CLASSIFICATION CRITERIA:

Undesired Flexibility:
- Mark as "YES" ONLY if there is a direct phrase proving the employer can unpredictably change work hours
- Examples include: "schedule may vary", "rotating shifts", "on-call required", "as needed", "PRN", "must be available for different shifts", "subject to change", "open availability required", "weekend/holiday work required"
- Demanded flexible work schedule is considered undesired flexibility, as it is open to the employer deciding working hours, with words like "must" and "necessary" hinting at that. Examples include "must be available to work flexible schedule", "must have a flexible schedule", "flexible schedule needed", "necessary flexible schedule"
- Fixed schedules like "weekend coverage" or "night shift" are NOT undesired flexibility
- The quote must be the exact phrase that justifies the classification

Desired Flexibility:
- Mark as "YES" ONLY if the employee can clearly choose when to work
- Examples include: "flexible schedule", "choose your hours", "work when you want", "set your own schedule"
- The quote must be the exact phrase that justifies the classification

Instructions:
1. Read the job description carefully
2. Identify phrases related to work schedule flexibility
3. Classify according to the criteria above
4. Provide exact quotes to support your classifications
5. Explain your reasoning step-by-step
6. Respond ONLY in the specified JSON format

EXAMPLE RESPONSES:
Example 1:
{{
  "undesired_flexibility": "YES",
  "undesired_quote": "Schedule may vary based on business needs",
  "desired_flexibility": "NO",
  "desired_quote": "N/A",
  "reasoning": "Found phrase indicating unpredictable schedule changes"
}}

Example 2:
{{
  "undesired_flexibility": "NO",
  "undesired_quote": "N/A",
  "desired_flexibility": "YES",
  "desired_quote": "Flexible work schedule available",
  "reasoning": "Employee can choose their work hours"
}}

Job Description:
{description}
"""


# ----------- OLLAMA API CALL -----------
def call_ollama_api(prompt, max_retries=MAX_RETRIES, retry_sleep=RETRY_SLEEP):
    for attempt in range(max_retries):
        try:
            response = httpx.post(
                OLLAMA_URL,
                json={
                    "model": MODEL_NAME,
                    "prompt": prompt,
                    "temperature": temperature,
                    "stream": False,
                    "repeat_penalty": 1.2,  # Helps reduce repetition
                    "top_k": 50,  # Limits token selection for more focused output
                    "top_p": 0.9  # Nucleus sampling for better quality
                },
                timeout=120
            )
            if response.status_code == 200:
                return response.json()["response"]
            else:
                logging.warning(f"Status {response.status_code}: {response.text}")
        except Exception as e:
            logging.error(f"Error calling Ollama: {e}")
        time.sleep(retry_sleep)
    return None


# ----------- SAFE JSON PARSING -----------
def safe_parse_json(llm_output):
    """
    Strip everything before first '{' and after last '}', and try to parse.
    """
    if not llm_output:
        logging.warning("Empty JSON input")
        return None
        
    import re
    match = re.search(r'(\{[\s\S]+\})', llm_output)
    if match:
        json_str = match.group(1)
        # Check if it's just empty braces
        if json_str.strip() == "{}":
            return {}
            
        try:
            # Try parsing with double quotes
            return json.loads(json_str)
        except json.JSONDecodeError:
            # Fallback: try to fix common issues
            json_str = json_str.replace("'", '"')
            # Fix trailing commas
            json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
            try:
                return json.loads(json_str)
            except Exception:
                pass
    logging.warning(f"Could not parse JSON:\n{llm_output}")
    return None

# --------- RESPONSE VALIDATION ---------

def validate_response(parsed_response):
    """
    Validate that the response has the expected structure and values.
    """
    if not parsed_response:
        return False
    
    # Check required keys
    required_keys = ["undesired_flexibility", "undesired_quote", "desired_flexibility", "desired_quote", "reasoning"]
    if not all(key in parsed_response for key in required_keys):
        return False
    
    # Check that flexibility values are either "YES" or "NO"
    flexibility_values = [parsed_response["undesired_flexibility"], parsed_response["desired_flexibility"]]
    if not all(val in ["YES", "NO"] for val in flexibility_values):
        return False
    
    return True

# ----------- YES/NO TO DUMMY -----------
def yesno_to_dummy(val):
    if isinstance(val, str):
        val = val.strip().upper()
        if val == "YES":
            return 1
        if val == "NO":
            return 0
    # For any None, empty, N/A, parse error: always return 0
    return 0


def clear_results_folder(path=OUTPUT_PATH, pattern="*.xlsx"):
    files = glob.glob(os.path.join(path, pattern))
    for f in files:
        try:
            os.remove(f)
        except Exception as e:
            print(f"Could not remove {f}: {e}")


def load_existing_batches(save_path_prefix):
    """
    Load existing batch files to resume processing.
    Returns the list of processed results and the highest processed index.
    """
    # Find all batch files
    batch_files = glob.glob(f"{save_path_prefix}_*.xlsx")
    
    # Sort by batch number
    def extract_batch_number(filename):
        try:
            return int(filename.split('_')[-1].split('.')[0])
        except:
            return -1
    
    batch_files.sort(key=extract_batch_number)
    
    # Load only the last batch file, as it contains all processed records up to that point
    results = []
    last_processed_index = -1
    
    if batch_files:
        # Get the last batch file (highest batch number)
        last_batch_file = batch_files[-1]
        try:
            df = pd.read_excel(last_batch_file)
            # Convert DataFrame to list of dictionaries
            results = df.to_dict('records')
            # The last processed index is the number of records in the last batch minus 1
            last_processed_index = len(results) - 1
            logging.info(f"Loaded {len(results)} existing records from {last_batch_file}, resuming from index {last_processed_index + 1}")
        except Exception as e:
            logging.warning(f"Could not load batch file {last_batch_file}: {e}")
            results = []
            last_processed_index = -1
    
    return results, last_processed_index


def load_all_processed_batches(save_path_prefix, exclude_final=False):
    """
    Load all processed batch files and combine them into a single dataframe.
    This function is used for generating the final output file.
    Returns a combined dataframe of all processed batches.
    """
    # Find all batch files
    batch_files = glob.glob(f"{save_path_prefix}_*.xlsx")
    
    # If exclude_final is True, filter out the final batch file
    if exclude_final:
        batch_files = [f for f in batch_files if not f.endswith("_final.xlsx")]
    
    # Sort by batch number
    def extract_batch_number(filename):
        try:
            # Handle the special case of "_final.xlsx" files
            if filename.endswith("_final.xlsx"):
                # Return a very large number to ensure final files are sorted last
                return float('inf')
            return int(filename.split('_')[-1].split('.')[0])
        except:
            return -1
    
    batch_files.sort(key=extract_batch_number)
    
    # Load all batch files and combine them, keeping only unique records
    all_records = []
    seen_titles = set()
    
    for batch_file in batch_files:
        try:
            df = pd.read_excel(batch_file)
            # Iterate through records and add only unique ones
            for _, record in df.iterrows():
                title = record.get("Title", "")
                # Extract the row index from the title if it exists
                if " (Row_" in title:
                    # Use the row index as the unique identifier
                    row_index = title.split(" (Row_")[-1].rstrip(")")
                    unique_key = row_index
                else:
                    # Use the title as the unique identifier
                    unique_key = title
                
                # Add the record if we haven't seen this key before
                if unique_key not in seen_titles:
                    seen_titles.add(unique_key)
                    all_records.append(record.to_dict())
            logging.info(f"Loaded batch file: {batch_file}")
        except Exception as e:
            logging.warning(f"Could not load batch file {batch_file}: {e}")
    
    # Create a dataframe from the unique records
    if all_records:
        combined_df = pd.DataFrame(all_records)
        logging.info(f"Combined batch files into a single dataframe with {len(combined_df)} unique records")
        return combined_df
    else:
        logging.warning("No batch files found to combine")
        return pd.DataFrame()


# ----------- SAVE BATCHES -----------
def save_batches(results, batch_size, save_path_prefix):
    if len(results) % batch_size == 0 and len(results) > 0:
        batch_number = len(results) // batch_size
        df = pd.DataFrame(results)
        save_path = f"{save_path_prefix}_{batch_number}.xlsx"
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_excel(save_path, index=False)
        logging.info(f"Batch saved: {save_path}")


# ----------- EXCEL HIGHLIGHTING FUNCTION -----------
def color_excel(path, col="undesired_flexibility"):
    try:
        wb = load_workbook(path)
        ws = wb.active
    except FileNotFoundError:
        print(f"File not found: {path}")
        return
    except Exception as e:
        print(f"Error loading workbook: {e}")
        return
    red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")   # Red
    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid") # Green

    # Find column index by name
    col_idx = None
    for idx, cell in enumerate(ws[1], 1):
        if cell.value == col:
            col_idx = idx
            break

    if col_idx is None:
        print(f"Column '{col}' not found in {path}")
        return

    for row in ws.iter_rows(min_row=2, min_col=col_idx, max_col=col_idx):
        for cell in row:
            if cell.value == 1:
                cell.fill = red   # 1 = undesirable (red)
            elif cell.value == 0:
                cell.fill = green # 0 = not undesirable (green)
    wb.save(path)
    print(f"Colors applied in {path}")


# ----------- MAIN PIPELINE -----------
def process_job_postings(input_path):
    filename = os.path.basename(input_path)
    filename_no_ext = os.path.splitext(filename)[0]
    # Handle .tar.gz or .csv.gz double extensions if needed, though simple splitext handles the last one.
    if filename.endswith(".csv.gz"):
        filename_no_ext = filename[:-7]
    
    # Create a path-friendly string from the input path (excluding the filename)
    input_dir_path = os.path.dirname(input_path)
    # Get relative path from the main directory to include folder structure in output name
    relative_path = os.path.relpath(input_dir_path, start=os.path.dirname(SCRIPT_DIR))
    # Replace path separators with underscores to create a safe filename
    path_structure = relative_path.replace(os.sep, '_').replace('/', '_')
    
    logging.info(f"Processing file: {input_path}")
    
    # Unique batch prefix for this file, including folder structure in the name
    if relative_path != ".":
        batch_save_prefix = os.path.join(BATCH_SAVE_DIR, f"{path_structure}_{filename_no_ext}_batch")
        final_file_path = os.path.join(OUTPUT_PATH, f"{path_structure}_{filename_no_ext}_processed_{MODEL_NAME}.xlsx")
    else:
        batch_save_prefix = os.path.join(BATCH_SAVE_DIR, f"{filename_no_ext}_batch")
        final_file_path = os.path.join(OUTPUT_PATH, f"{filename_no_ext}_processed_{MODEL_NAME}.xlsx")

    # Create output directories if they don't exist
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(BATCH_SAVE_DIR, exist_ok=True)
    
    # Check if there are existing batch files to resume from
    existing_results, last_processed_index = load_existing_batches(batch_save_prefix)
    
    if existing_results:
        logging.info(f"Resuming from existing batches. Last processed index: {last_processed_index}")
        results = existing_results
        resume_from = last_processed_index + 1
    else:
        logging.info("Starting fresh processing")
        results = []
        resume_from = 0
    
    # Use the adaptive function to read the file
    try:
        df = read_file_adaptive(input_path)
    except Exception as e:
        logging.error(f"Error reading input file {input_path}: {str(e)}")
        return
    
    df.columns = [column.upper() for column in df.columns]
    
    # Process rows starting from resume_from index
    pbar = tqdm(total=len(df), initial=0)
    pbar.update(resume_from)  # Set initial position
    
    for idx, row in df.iterrows():
        # Skip already processed rows
        if idx < resume_from:
            continue
            
        # Update progress bar description with current row index
        # pbar.set_description(f"Processing row {idx}")
        pbar.set_postfix({"Total": len(df)})
            
        # Log the row being processed
        if idx % 10 == 0 or idx == resume_from:
            logging.info(f"Processing row {idx}/{len(df)}")
            
        desc = row.get("BODY")
        short_desc = condense_description(desc)
        prompt = build_flexibility_prompt(short_desc)
        
        logging.info(f"Calling Ollama API for row {idx}")
        validated = False
        while not validated:
            response = call_ollama_api(prompt)
            parsed = safe_parse_json(response) if response else None
            validated = validate_response(parsed)
            
        logging.info(f"Finished processing row {idx}")
        
        # Update progress bar
        pbar.update(1)
        
        if parsed:
            undesired_val = yesno_to_dummy(parsed.get("undesired_flexibility"))
            desired_val = yesno_to_dummy(parsed.get("desired_flexibility"))
            undesired_quote = parsed.get("undesired_quote")
            desired_quote = parsed.get("desired_quote")
            reasoning = parsed.get("reasoning")
        else:
            undesired_val = 0
            desired_val = 0
            undesired_quote = ""
            desired_quote = ""
            reasoning = "PARSING ERROR"

        # Include original index in the title for tracking
        title = row.get("TITLE_NAME", "")
        record = {
            "Title": f"{title} (Row_{idx})",
            "Body": desc,
            "llama_raw_response": response,
            "undesired_flexibility": undesired_val,
            "undesired_quote": undesired_quote,
            "desired_flexibility": desired_val,
            "desired_quote": desired_quote,
            "reasoning": reasoning
        }
        results.append(record)
        save_batches(results, BATCH_SIZE, batch_save_prefix)
        
    pbar.close()

    # Save remaining records at the end as a regular batch
    if len(results) % BATCH_SIZE != 0:
        # Save the final batch with all results
        df_final = pd.DataFrame(results)
        save_path = f"{batch_save_prefix}_{(len(results) // BATCH_SIZE) + 1}.xlsx"
        df_final.to_excel(save_path, index=False)
        logging.info(f"Final regular batch saved: {save_path}")
    
    # Save a separate "final" batch file
    df_final_all = pd.DataFrame(results)
    save_path_final = f"{batch_save_prefix}_final.xlsx"
    df_final_all.to_excel(save_path_final, index=False)
    logging.info(f"Final batch saved: {save_path_final}")

    # Save the full output by loading all processed batches (excluding the final batch file)
    df_final = load_all_processed_batches(batch_save_prefix, exclude_final=True)
    if not df_final.empty:
        # Remove row index from Title in final output if it exists
        if "Title" in df_final.columns:
            df_final["Title"] = df_final["Title"].str.replace(r" \(Row_\d+\)", "", regex=True)
        df_final.to_excel(final_file_path, index=False)
        logging.info(f"Full file saved: {final_file_path}")

        # Color the 'undesired_flexibility' column in the final Excel file
        color_excel(final_file_path, col="undesired_flexibility")
    else:
        logging.warning("No data to save in final file")


def ollama_warmup():
    logging.info("Checking Ollama service and model availability...")
    
    # Check if Ollama is running
    try:
        response = httpx.get("http://localhost:11434/api/tags", timeout=30)
        if response.status_code != 200:
            logging.error(f"Ollama service is not responding. Status code: {response.status_code}")
            raise SystemExit("Error: Ollama service is not running. Please start Ollama and try again.")
    except httpx.RequestError as e:
        logging.error(f"Failed to connect to Ollama service: {e}")
        raise SystemExit("Error: Could not connect to Ollama service. Please ensure Ollama is installed and running.")
    except Exception as e:
        logging.error(f"Unexpected error when checking Ollama service: {e}")
        raise SystemExit("Error: Unexpected error when checking Ollama service.")
    
    # Check if the specified model is available
    try:
        models = response.json().get("models", [])
        model_names = [model.get("name") for model in models]
        if MODEL_NAME not in model_names:
            logging.error(f"Model '{MODEL_NAME}' is not available in Ollama.")
            raise SystemExit(f"Error: Model '{MODEL_NAME}' is not available. Please pull the model using 'ollama pull {MODEL_NAME}' and try again.")
    except Exception as e:
        logging.error(f"Error checking model availability: {e}")
        raise SystemExit("Error: Could not verify model availability.")
    
    # Warm up the model with a dummy request
    logging.info("Warming up the model with a dummy request...")
    dummy_prompt = "Respond ONLY with OK."
    try:
        _ = call_ollama_api(dummy_prompt, temperature)
        logging.info("Ollama model is warm and ready!")
    except Exception as e:
        logging.error(f"Warm-up failed: {e}")
        raise SystemExit("Error: Failed to warm up the model. Please check Ollama logs for more information.")


def keep_ollama_alive(interval_minutes=30):
    while True:
        time.sleep(interval_minutes * 60)
        _ = call_ollama_api("Respond ONLY with OK.", temperature)
        logging.info("Keep-alive sent to Ollama.")


# ----------- MAIN EXECUTION -----------

def run_batch_processing(input_dir):
    # Recursive search for .csv.gz files
    search_pattern = os.path.join(input_dir, "**", "*.csv.gz")
    files_to_process = glob.glob(search_pattern, recursive=True)
    
    if not files_to_process:
        logging.warning(f"No .csv.gz files found in {input_dir}")
        print(f"No .csv.gz files found in {input_dir}")
    else:
        logging.info(f"Found {len(files_to_process)} files to process.")
        print(f"Found {len(files_to_process)} files to process.")
        
        for input_file in files_to_process:
            try:
                print(f"Starting processing for: {input_file}")
                process_job_postings(input_file)
                print(f"Finished processing for: {input_file}")
            except Exception as e:
                logging.error(f"Failed to process {input_file}: {e}")
                print(f"Failed to process {input_file}: {e}")

# ----------- MAIN EXECUTION -----------
if __name__ == "__main__":
    ollama_warmup()
    run_batch_processing(INPUT_DIR)

    logging.info(f"Log file saved at: {LOG_FILE}")
    print(f"Log file saved at: {LOG_FILE}")
    # clear_results_folder()