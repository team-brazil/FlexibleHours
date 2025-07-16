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

BATCH_SIZE = 20
BATCH_SAVE_PREFIX = "../output/results/batch_temp"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# ----------- condense_description (mantida) ------------
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
You are an expert HR analyst. Analyze the job description below. Respond ONLY in this JSON format:
{{
  "undesired_flexibility": "YES" or "NO",
  "undesired_quote": "exact quote or 'N/A'",
  "desired_flexibility": "YES" or "NO",
  "desired_quote": "exact quote or 'N/A'",
  "reasoning": "Your step-by-step reasoning, max 400 characters"
}}

Rules:
- "undesired_flexibility" is "YES" if there is any sign the employer can impose schedule changes, unpredictable shifts, "as needed", "PRN", "weekend availability", etc.
- "desired_flexibility" is "YES" if the employee can control when they work, like "set your own schedule", "work from anywhere".
- Only use direct quotes from the description in the quote fields, or 'N/A' if none found.
- If both apply, both YES with their quotes.
- Your reasoning must be concise and less than 400 characters.
- Do NOT output anything outside the JSON.

Job Description:
{description}
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


# ----------- Função para transformar YES/NO em dummy -----------
def yesno_to_dummy(val):
    if isinstance(val, str):
        val = val.strip().upper()
        if val == "YES":
            return 1
        if val == "NO":
            return 0
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

        undesired_val = yesno_to_dummy(parsed.get("undesired_flexibility")) if parsed else None
        desired_val = yesno_to_dummy(parsed.get("desired_flexibility")) if parsed else None

        registro = {
            "Title": row.get("Title", ""),
            "Body": desc,
            "llama_raw_response": response,
            "undesired_flexibility": undesired_val,
            "undesired_quote": parsed.get("undesired_quote") if parsed else None,
            "desired_flexibility": desired_val,
            "desired_quote": parsed.get("desired_quote") if parsed else None,
            "reasoning": parsed.get("reasoning") if parsed else None
        }
        results.append(registro)
        save_batches(results, BATCH_SIZE, BATCH_SAVE_PREFIX)

    # Salva o restante ao final
    if len(results) % BATCH_SIZE != 0:
        df_final = pd.DataFrame(results)
        save_path = f"{BATCH_SAVE_PREFIX}_final.xlsx"
        df_final.to_excel(save_path, index=False)
        logging.info(f"Batch final salvo: {save_path}")

    # Salva o arquivo completo já convertido
    pd.DataFrame(results).to_excel(OUTPUT_PATH, index=False)
    logging.info(f"Arquivo completo salvo: {OUTPUT_PATH}")


# ----------- Execução principal -----------
if __name__ == "__main__":
    # Exemplo: processar um arquivo input específico
    input_file = os.path.join(INPUT_DIR, "us_postings_sample.xlsx")
    process_job_postings(input_file)
