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


# === CONFIGURATION ===
# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Build paths relative to the script directory
INPUT_DIR_NAME_FILE = os.path.join(SCRIPT_DIR, "..", "input", "first_500_unit_lightcast_sample.csv")
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen3:8b"
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "output", "results")
LOG_PATH = os.path.join(SCRIPT_DIR, "..", "logs")
FINAL_FILE_PATH = os.path.join(OUTPUT_PATH, f"Job_postings_processed_{MODEL_NAME}.xlsx")
temperature = 0
NUM_PREDICT = 200
MAX_RETRIES = 2
RETRY_SLEEP = 3
BATCH_SIZE = 20   # Save every N records
BATCH_SAVE_PREFIX = os.path.join(OUTPUT_PATH, "batch_temp")
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
    # Create output directories if they don't exist
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(os.path.dirname(BATCH_SAVE_PREFIX), exist_ok=True)
    
    # Check if there are existing batch files to resume from
    existing_results, last_processed_index = load_existing_batches(BATCH_SAVE_PREFIX)
    
    if existing_results:
        logging.info(f"Resuming from existing batches. Last processed index: {last_processed_index}")
        results = existing_results
        resume_from = last_processed_index + 1
    else:
        logging.info("Starting fresh processing")
        results = []
        resume_from = 0
    
    if input_path.endswith(".xlsx"):
        df = pd.read_excel(input_path)
    elif input_path.endswith(".csv"):
        df = pd.read_csv(input_path)
    else:
        logging.error("Invalid file or filetype")
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
        save_batches(results, BATCH_SIZE, BATCH_SAVE_PREFIX)
        
    pbar.close()

    # Save remaining records at the end as a regular batch
    if len(results) % BATCH_SIZE != 0:
        # Save the final batch with all results
        df_final = pd.DataFrame(results)
        save_path = f"{BATCH_SAVE_PREFIX}_{(len(results) // BATCH_SIZE) + 1}.xlsx"
        df_final.to_excel(save_path, index=False)
        logging.info(f"Final regular batch saved: {save_path}")
    
    # Save a separate "final" batch file
    df_final_all = pd.DataFrame(results)
    save_path_final = f"{BATCH_SAVE_PREFIX}_final.xlsx"
    df_final_all.to_excel(save_path_final, index=False)
    logging.info(f"Final batch saved: {save_path_final}")

    # Save the full output by loading all processed batches (excluding the final batch file)
    df_final = load_all_processed_batches(BATCH_SAVE_PREFIX, exclude_final=True)
    if not df_final.empty:
        # Remove row index from Title in final output if it exists
        if "Title" in df_final.columns:
            df_final["Title"] = df_final["Title"].str.replace(r" \(Row_\d+\)", "", regex=True)
        df_final.to_excel(FINAL_FILE_PATH, index=False)
        logging.info(f"Full file saved: {FINAL_FILE_PATH}")

        # Color the 'undesired_flexibility' column in the final Excel file
        color_excel(FINAL_FILE_PATH, col="undesired_flexibility")
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
if __name__ == "__main__":
    # Check if there are existing batch files before clearing
    existing_batches = glob.glob(f"{BATCH_SAVE_PREFIX}_*.xlsx")
    
    if not existing_batches:
        # Only clear results folder if there are no existing batches
        clear_results_folder()
    
    ollama_warmup()
    input_file = os.path.join(INPUT_DIR_NAME_FILE)
    process_job_postings(input_file)
    logging.info(f"Log file saved at: {LOG_FILE}")
    print(f"Log file saved at: {LOG_FILE}")
    # clear_results_folder()