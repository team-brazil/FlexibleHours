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
import gc
# Importações necessárias para o Multiprocessing (Sua adição)
from multiprocessing import Pool, current_process

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
COOLDOWN = 0.5  # Seconds to wait between API calls

NUM_PREDICT = 200
MAX_RETRIES = 2
RETRY_SLEEP = 3
BATCH_SIZE = 20  # Save every N records
BATCH_SAVE_DIR = os.path.join(OUTPUT_PATH, "batch_temp")
NUM_WORKERS = 4  # Sua configuração de Workers

os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)
os.makedirs(BATCH_SAVE_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_PATH, f"process_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8')
        # Removed StreamHandler so logs don't appear in terminal
    ]
)


def read_file_adaptive(file_path, **kwargs):
    """
    Reads input files adaptively. (Mantido do Original)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    file_extension = os.path.splitext(file_path)[1].lower()

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

    if file_extension not in readers:
        try:
            return _detect_and_read_by_content(file_path, **kwargs)
        except:
            raise ValueError(f"Unsupported file format: {file_extension}")

    reader_func = readers[file_extension]

    try:
        df = reader_func(file_path, **kwargs)
        logging.info(f"File {file_path} read successfully. Format: {file_extension}, Shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {str(e)}")
        raise e


def _detect_and_read_by_content(file_path, **kwargs):
    with open(file_path, 'rb') as f:
        header = f.read(1024)
    header_str = header.decode('utf-8', errors='ignore').strip()
    if header_str.startswith('{') or header_str.startswith('['):
        return pd.read_json(file_path, **kwargs)
    if ',' in header_str or '\t' in header_str:
        return pd.read_csv(file_path, **kwargs)
    raise ValueError(f"File format not automatically detected: {file_path}")


# ----------- condense_description (Mantido do Original) ------------
def condense_description(description, window=3, min_length=2000):
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


# ----------- IMPROVED JSON PROMPT (Mantido do Original) ------------
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


# ----------- OLLAMA API CALL (Mantido do Original) -----------
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
                    "repeat_penalty": 1.2,
                    "top_k": 50,
                    "top_p": 0.9
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


# ----------- SAFE JSON PARSING (Mantido do Original) -----------
def safe_parse_json(llm_output):
    if not llm_output:
        logging.warning("Empty JSON input")
        return None

    import re
    match = re.search(r'(\{[\s\S]+\})', llm_output)
    if match:
        json_str = match.group(1)
        if json_str.strip() == "{}":
            return {}

        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            json_str = json_str.replace("'", '"')
            json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
            try:
                return json.loads(json_str)
            except Exception:
                pass
    logging.warning(f"Could not parse JSON:\n{llm_output}")
    return None


# --------- RESPONSE VALIDATION (Mantido do Original) ---------
def validate_response(parsed_response):
    if not parsed_response:
        return False
    required_keys = ["undesired_flexibility", "undesired_quote", "desired_flexibility", "desired_quote", "reasoning"]
    if not all(key in parsed_response for key in required_keys):
        return False
    flexibility_values = [parsed_response["undesired_flexibility"], parsed_response["desired_flexibility"]]
    if not all(val in ["YES", "NO"] for val in flexibility_values):
        return False
    return True


def yesno_to_dummy(val):
    if isinstance(val, str):
        val = val.strip().upper()
        if val == "YES":
            return 1
        if val == "NO":
            return 0
    return 0


def get_resume_index(save_path_prefix):
    batch_files = glob.glob(f"{save_path_prefix}_*.xlsx")
    if not batch_files:
        return -1

    def extract_batch_number(filename):
        try:
            return int(filename.split('_')[-1].split('.')[0])
        except:
            return -1

    batch_files.sort(key=extract_batch_number)
    last_batch_file = batch_files[-1]

    try:
        df = pd.read_excel(last_batch_file)
        if df.empty:
            return -1
        last_row = df.iloc[-1]
        title = str(last_row.get("Title", ""))
        import re
        match = re.search(r"\(Row_(\d+)\)", title)
        if match:
            return int(match.group(1))
    except Exception as e:
        logging.warning(f"Could not read last batch file to determine resume index: {e}")
    return -1


def load_all_processed_batches(save_path_prefix, exclude_final=False):
    batch_files = glob.glob(f"{save_path_prefix}_*.xlsx")
    if exclude_final:
        batch_files = [f for f in batch_files if not f.endswith("_final.xlsx")]

    def extract_batch_number(filename):
        try:
            if filename.endswith("_final.xlsx"):
                return float('inf')
            return int(filename.split('_')[-1].split('.')[0])
        except:
            return -1

    batch_files.sort(key=extract_batch_number)

    all_records = []
    seen_titles = set()

    for batch_file in batch_files:
        try:
            df = pd.read_excel(batch_file)
            for _, record in df.iterrows():
                title = record.get("Title", "")
                if " (Row_" in title:
                    row_index = title.split(" (Row_")[-1].rstrip(")")
                    unique_key = row_index
                else:
                    unique_key = title

                if unique_key not in seen_titles:
                    seen_titles.add(unique_key)
                    all_records.append(record.to_dict())
            logging.info(f"Loaded batch file: {batch_file}")
        except Exception as e:
            logging.warning(f"Could not load batch file {batch_file}: {e}")

    if all_records:
        combined_df = pd.DataFrame(all_records)
        logging.info(f"Combined batch files into a single dataframe with {len(combined_df)} unique records")
        return combined_df
    else:
        return pd.DataFrame()


# ----------- SAVE BATCHES (Mantido do Original do Programador) -----------
# Nota: O seu tinha 'del df; gc.collect()', mas o original não.
# Estou mantendo o original conforme pedido, pois faremos a limpeza no loop de chunking.
def save_batch_chunk(batch_data, batch_index, save_path_prefix):
    if not batch_data:
        return

    df = pd.DataFrame(batch_data)
    save_path = f"{save_path_prefix}_{batch_index}.xlsx"

    try:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_excel(save_path, index=False)
        logging.info(f"Batch chunk {batch_index} saved with {len(df)} records at: {save_path}")
    except Exception as e:
        logging.error(f"Failed to save batch chunk {save_path}: {e}")


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
    red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")

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
                cell.fill = red
            elif cell.value == 0:
                cell.fill = green
    wb.save(path)
    print(f"Colors applied in {path}")


# ----------- MAIN PIPELINE (Unificado) -----------
def process_job_postings(input_path):
    worker_name = current_process().name  # Identificação do Worker

    filename = os.path.basename(input_path)
    filename_no_ext = os.path.splitext(filename)[0]
    if filename.endswith(".csv.gz"):
        filename_no_ext = filename[:-7]

    # Lógica de caminho relativo do GitHub original
    input_dir_path = os.path.dirname(input_path)
    relative_path = os.path.relpath(input_dir_path, start=os.path.dirname(SCRIPT_DIR))
    path_structure = relative_path.replace(os.sep, '_').replace('/', '_')

    logging.info(f"[{worker_name}] Processing file: {input_path}")

    # Definição dos nomes de arquivo
    if relative_path != ".":
        batch_save_prefix = os.path.join(BATCH_SAVE_DIR, f"{path_structure}_{filename_no_ext}_batch")
        final_file_path = os.path.join(OUTPUT_PATH, f"{path_structure}_{filename_no_ext}_processed_{MODEL_NAME}.xlsx")
    else:
        batch_save_prefix = os.path.join(BATCH_SAVE_DIR, f"{filename_no_ext}_batch")
        final_file_path = os.path.join(OUTPUT_PATH, f"{filename_no_ext}_processed_{MODEL_NAME}.xlsx")

    # Verifica se já processou
    if os.path.exists(final_file_path):
        logging.info(f"[{worker_name}] File already processed: {final_file_path}. Skipping.")
        print(f"[{worker_name}] File already processed: {final_file_path}. Skipping.")
        return True

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(BATCH_SAVE_DIR, exist_ok=True)

    # Resume
    last_index = get_resume_index(batch_save_prefix)
    resume_from = last_index + 1

    if resume_from > 0:
        logging.info(f"[{worker_name}] Resuming processing from row index {resume_from}")

    # --- SUA OTIMIZAÇÃO: Chunking de Memória ---
    # Se for CSV, lê em blocos. Se for Excel/Outros, lê tudo (fallback).
    is_csv = input_path.lower().endswith(('.csv', '.csv.gz', '.tsv'))

    if is_csv:
        # Lê em blocos de 500 para não estourar RAM
        try:
            iterator = pd.read_csv(input_path, chunksize=500)
        except Exception as e:
            logging.error(f"Error reading CSV {input_path}: {str(e)}")
            return False
    else:
        # Fallback (lê tudo) mas gerenciado como um iterador de 1 item
        try:
            df_full = read_file_adaptive(input_path)
            df_full.columns = [column.upper() for column in df_full.columns]
            iterator = [df_full]
        except Exception as e:
            logging.error(f"Error reading file {input_path}: {str(e)}")
            return False

    current_row_global_idx = 0
    current_batch = []
    current_batch_index = (resume_from // BATCH_SIZE) + 1

    # Loop principal (Iterando sobre chunks para economizar memória)
    for chunk_df in iterator:
        # Garante colunas em maiúsculo
        if is_csv:
            chunk_df.columns = [column.upper() for column in chunk_df.columns]

        chunk_len = len(chunk_df)

        # Fast Forward se já processamos este bloco inteiro
        if current_row_global_idx + chunk_len <= resume_from:
            current_row_global_idx += chunk_len
            # Limpeza imediata do chunk pulado
            del chunk_df
            gc.collect()
            continue

        # Processa linhas do chunk
        for _, row in chunk_df.iterrows():
            if current_row_global_idx < resume_from:
                current_row_global_idx += 1
                continue

            # --- Lógica Original de Processamento ---
            desc = row.get("BODY")
            short_desc = condense_description(desc)
            prompt = build_flexibility_prompt(short_desc)

            validated = False
            attempts = 0
            max_attempts = 3
            response = None
            parsed = None

            while not validated and attempts < max_attempts:
                attempts += 1
                response = call_ollama_api(prompt)
                parsed = safe_parse_json(response) if response else None
                validated = validate_response(parsed)
                time.sleep(COOLDOWN)

            if not validated:
                parsed = None
                response = str(response) + " [FAILED VALIDATION]"

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

            title = row.get("TITLE_NAME", "")
            record = {
                "Title": f"{title} (Row_{current_row_global_idx})",
                "Body": desc,
                "llama_raw_response": response,
                "undesired_flexibility": undesired_val,
                "undesired_quote": undesired_quote,
                "desired_flexibility": desired_val,
                "desired_quote": desired_quote,
                "reasoning": reasoning
            }

            current_batch.append(record)
            current_row_global_idx += 1

            # --- GARBAGE COLLECTOR DO PROGRAMADOR ORIGINAL ---
            # Ele colocou isso DENTRO do loop principal.
            if len(current_batch) >= BATCH_SIZE:
                save_batch_chunk(current_batch, current_batch_index, batch_save_prefix)
                current_batch = []  # Clear memory
                current_batch_index += 1
                gc.collect()  # Force garbage collection (Mantido do Original)

        # --- SUA OTIMIZAÇÃO: Limpeza do Chunk ---
        # Após processar o bloco de 500 linhas, removemos da memória
        del chunk_df
        gc.collect()

    # Salva o resto
    if current_batch:
        save_batch_chunk(current_batch, current_batch_index, batch_save_prefix)
        del current_batch
        gc.collect()

    # Gera arquivo final
    df_final = load_all_processed_batches(batch_save_prefix, exclude_final=True)
    if not df_final.empty:
        if "Title" in df_final.columns:
            df_final["Title"] = df_final["Title"].str.replace(r" \(Row_\d+\)", "", regex=True)
        df_final.to_excel(final_file_path, index=False)
        logging.info(f"[{worker_name}] Full file saved: {final_file_path}")
        color_excel(final_file_path, col="undesired_flexibility")

    print(f"[{worker_name}] Finished: {filename}")
    return True


# ----------- WRAPPER PARA O POOL (Sua Adição) -----------
def process_wrapper(file_path):
    try:
        return process_job_postings(file_path)
    except Exception as e:
        print(f"CRITICAL ERROR in {os.path.basename(file_path)}: {e}")
        logging.error(f"Error in worker for {file_path}: {e}")
        return False


def ollama_warmup():
    logging.info("Checking Ollama service...")
    try:
        call_ollama_api("Test", max_retries=1)
        logging.info("Ollama is ready.")
    except Exception as e:
        logging.warning(f"Ollama warmup failed: {e}")


def keep_ollama_alive(interval_minutes=30):
    while True:
        time.sleep(interval_minutes * 60)
        _ = call_ollama_api("Respond ONLY with OK.", temperature)
        logging.info("Keep-alive sent to Ollama.")


# ----------- MAIN EXECUTION (Sua Adição de Multiprocessamento) -----------

def run_batch_processing(input_dir):
    search_pattern = os.path.join(input_dir, "**", "*.csv.gz")
    files_to_process = glob.glob(search_pattern, recursive=True)

    if not files_to_process:
        search_pattern = os.path.join(input_dir, "**", "*.csv")  # Tenta procurar CSV normal
        files_to_process = glob.glob(search_pattern, recursive=True)

    if not files_to_process:
        print(f"No files found in {input_dir}")
        logging.warning(f"No files found in {input_dir}")
        return

    print(f"Found {len(files_to_process)} files. Starting {NUM_WORKERS} workers.")
    logging.info(f"Found {len(files_to_process)} files. Starting {NUM_WORKERS} workers.")

    # Processamento paralelo (Sua lógica)
    with Pool(processes=NUM_WORKERS) as pool:
        pool.map(process_wrapper, files_to_process)

    print("All processing finished.")


if __name__ == "__main__":
    ollama_warmup()
    run_batch_processing(INPUT_DIR)

    logging.info(f"Log file saved at: {LOG_FILE}")
    print(f"Log file saved at: {LOG_FILE}")