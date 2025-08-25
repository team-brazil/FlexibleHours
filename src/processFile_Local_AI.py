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
# Obter o diretório do script atual
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Construir caminhos relativos ao diretório do script
INPUT_DIR_NAME_FILE = os.path.join(SCRIPT_DIR, "..", "input", "us_postings_sample.xlsx")
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3:8b"
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
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)


# ----------- condense_description (kept) ------------
def condense_description(description, window=3, min_length=2000):
    """
    Only condense text if it is really long.
    Keeps lines with keywords and surrounding context.
    """
    if len(description) < min_length:
        return description

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
    import re
    match = re.search(r'(\{[\s\S]+\})', llm_output)
    if match:
        json_str = match.group(1)
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

# --------- RESPONSE VALITADION ---------

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


# ----------- SAVE BATCHES -----------
def save_batches(results, batch_size, save_path_prefix):
    if len(results) % batch_size == 0 and len(results) > 0:
        batch_number = len(results) // batch_size
        df = pd.DataFrame(results)
        save_path = f"{save_path_prefix}_{batch_number}.xlsx"
        df.to_excel(save_path, index=False)
        logging.info(f"Batch saved: {save_path}")


# ----------- EXCEL HIGHLIGHTING FUNCTION -----------
def color_excel(path, col="undesired_flexibility"):
    wb = load_workbook(path)
    ws = wb.active
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
    df = pd.read_excel(input_path)
    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        desc = row.get("BODY")
        short_desc = condense_description(desc)
        prompt = build_flexibility_prompt(short_desc)
        
        validated = False
        while not validated:
            response = call_ollama_api(prompt)     
            parsed = safe_parse_json(response) if response else None
            validated = validate_response(parsed)

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

        record = {
            "Title": row.get("TITLE_NAME", ""),
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

    # Save remaining records at the end
    if len(results) % BATCH_SIZE != 0:
        df_final = pd.DataFrame(results)
        save_path = f"{BATCH_SAVE_PREFIX}_final.xlsx"
        df_final.to_excel(save_path, index=False)
        logging.info(f"Final batch saved: {save_path}")

    # Save the full output
    pd.DataFrame(results).to_excel(FINAL_FILE_PATH, index=False)
    logging.info(f"Full file saved: {FINAL_FILE_PATH}")

    # Color the 'undesired_flexibility' column in the final Excel file
    color_excel(FINAL_FILE_PATH, col="undesired_flexibility")


def ollama_warmup():
    logging.info("Warming up the model with a dummy request...")
    dummy_prompt = "Respond ONLY with OK."
    try:
        _ = call_ollama_api(dummy_prompt, temperature)
        logging.info("Ollama model is warm and ready!")
    except Exception as e:
        logging.warning(f"Warm-up failed: {e}")


def keep_ollama_alive(interval_minutes=30):
    while True:
        time.sleep(interval_minutes * 60)
        _ = call_ollama_api("Respond ONLY with OK.", temperature)
        logging.info("Keep-alive sent to Ollama.")


# ----------- MAIN EXECUTION -----------
if __name__ == "__main__":
    clear_results_folder()
    ollama_warmup()
    input_file = os.path.join(INPUT_DIR_NAME_FILE)
    process_job_postings(input_file)
    logging.info(f"Log file saved at: {LOG_FILE}")
    print(f"Log file saved at: {LOG_FILE}")
