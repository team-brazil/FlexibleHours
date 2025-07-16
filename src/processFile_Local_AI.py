import os
import time
import json
import logging
import httpx
import pandas as pd
from tqdm import tqdm
from openpyxl.styles import PatternFill

# === CONFIG ===
INPUT_DIR = "../input"
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3:8b"
OUTPUT_PATH = f"../output/results/Job_postings_processed_{MODEL_NAME}.xlsx"
NUM_PREDICT = 200
MAX_RETRIES = 2
RETRY_SLEEP = 3

BATCH_SIZE = 50
BATCH_SAVE_PREFIX = "../output/results/batch_temp"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# ----------- NEW condense_description (mantida) ------------
def condense_description(description, window=3, min_length=2000):
    """
    Só aplica o filtro se o texto for realmente longo.
    Mantém frases com palavras-chave e contexto em volta.
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


# ----------- PROMPT JSON REFINADO ------------
def build_flexibility_prompt(description):
    return f"""
    Analyze job descriptions to detect:
    - "undesired flexibility" (frequent unpredictable schedule/shift changes, on-call, instability),
    - "desired flexibility" (benefits like flexible schedule, remote work, autonomy).
    
    Instructions:
    - For each category, reply 1 (YES) or 0 (NO).
    - Provide the supporting quote from the description or leave empty.
    - Give a brief reasoning for each answer.
    - Never set both as 1 for the same posting.
    - Respond in this JSON (with double curly braces):
    
    {{
      "undesired_flexibility": 1 or 0,
      "undesired_quote": "...",
      "undesired_reasoning": "...",
      "desired_flexibility": 1 or 0,
      "desired_quote": "...",
      "desired_reasoning": "..."
    }}
    
    Examples:
    
    Job post: "Schedules may change every week, on-call required."
    {{
      "undesired_flexibility": 1,
      "undesired_quote": "Schedules may change every week, on-call required.",
      "undesired_reasoning": "Mentions unpredictable schedule and on-call.",
      "desired_flexibility": 0,
      "desired_quote": "",
      "desired_reasoning": "No mention of positive flexibility."
    }}
    
    Job post: "Flexible schedule and remote work available."
    {{
      "undesired_flexibility": 0,
      "undesired_quote": "",
      "undesired_reasoning": "No instability in the schedule.",
      "desired_flexibility": 1,
      "desired_quote": "Flexible schedule and remote work available.",
      "desired_reasoning": "Offers flexibility as a benefit."
    }}
    """


# ----------- Função para chamada à API OLLAMA -----------
def call_ollama_api(prompt, max_retries=MAX_RETRIES, retry_sleep=RETRY_SLEEP):
    for attempt in range(max_retries):
        try:
            response = httpx.post(
                OLLAMA_URL,
                json={
                    "model": MODEL_NAME,
                    "prompt": prompt,
                    "stream": False
                },
                timeout=120
            )
            if response.status_code == 200:
                return response.json()["response"]
            else:
                logging.warning(f"Status {response.status_code}: {response.text}")
        except Exception as e:
            logging.error(f"Erro ao chamar Ollama: {e}")
        time.sleep(retry_sleep)
    return None


# ----------- Função para parsing seguro do JSON retornado -----------
def safe_parse_json(llm_output):
    """
    Extrai e faz o parsing do primeiro bloco JSON válido encontrado na resposta.
    """
    try:
        start = llm_output.find('{')
        end = llm_output.rfind('}') + 1
        if start == -1 or end == -1:
            return None
        json_str = llm_output[start:end]
        return json.loads(json_str)
    except Exception as e:
        logging.warning(f"Erro ao parsear JSON: {e}\nOutput bruto: {llm_output}")
        return None


# ----------- Função para salvar em lotes -----------
def save_batches(results, batch_size, save_path_prefix):
    if len(results) % batch_size == 0 and len(results) > 0:
        batch_number = len(results) // batch_size
        df = pd.DataFrame(results)
        save_path = f"{save_path_prefix}_{batch_number}.xlsx"
        df.to_excel(save_path, index=False)
        logging.info(f"Batch salvo: {save_path}")


# ----------- Main Pipeline -----------
def process_job_postings(input_path):
    df = pd.read_excel(input_path)
    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        desc = row.get("Body") or row.get("body") or ""
        short_desc = condense_description(desc)
        prompt = build_flexibility_prompt(short_desc)
        response = call_ollama_api(prompt)
        parsed = safe_parse_json(response) if response else None

        # Prepara registro para salvar
        registro = {
            "Title": row.get("Title", ""),
            "Body": desc,
            "llama_raw_response": response,
            "undesired_flexibility": parsed.get("undesired_flexibility") if parsed else None,
            "undesired_quote": parsed.get("undesired_quote") if parsed else None,
            "desired_flexibility": parsed.get("desired_flexibility") if parsed else None,
            "desired_quote": parsed.get("desired_quote") if parsed else None,
            "reasoning": parsed.get("reasoning") if parsed else None
        }
        results.append(registro)
        # Salvamento em lote
        save_batches(results, BATCH_SIZE, BATCH_SAVE_PREFIX)

    # Salva o restante ao final
    if len(results) % BATCH_SIZE != 0:
        df_final = pd.DataFrame(results)
        save_path = f"{BATCH_SAVE_PREFIX}_final.xlsx"
        df_final.to_excel(save_path, index=False)
        logging.info(f"Batch final salvo: {save_path}")

    # Também salva tudo junto no output principal
    pd.DataFrame(results).to_excel(OUTPUT_PATH, index=False)
    logging.info(f"Arquivo completo salvo: {OUTPUT_PATH}")


# ----------- Execução principal -----------
if __name__ == "__main__":
    # Exemplo: processar um arquivo input específico
    input_file = os.path.join(INPUT_DIR, "wrong_classificated_undesired_flexibility.xlsx")
    process_job_postings(input_file)
