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
OUTPUT_PATH = "../output/results/Job_postings_processed_" + MODEL_NAME + ".xlsx"
NUM_PREDICT = 64
MAX_RETRIES = 3
RETRY_SLEEP = 5

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# ----------- NEW CONDENSE_DESCRIPTION ------------
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
        "unpredictable", "as required", "required to", "vary", "subject to", "full availability"
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


# ----------- PROMPT ------------
def build_flexibility_prompt(description):
    return f"""
    You are an expert HR analyst. Your task is to classify ONLY the *work hours flexibility* in job descriptions.
    
    Definitions:
    - **Undesirable Flexibility (Company-Driven Hours):** This ONLY applies when the employer can frequently change, rotate, or require different work schedules or shifts to meet business needs. Examples include: required to cover different shifts as needed, schedule may change at any time, must be available whenever required, or rotating shifts that change week to week. If the schedule is fixed, or if rotating/shift work is described as a set/pre-planned schedule, DO NOT consider it undesirable.
    - **Desirable Flexibility (Employee-Driven Hours):** The employee clearly has the right to choose their own work hours (e.g., "set your own schedule", "work anytime"). Flexibility is desirable ONLY if the employee controls when they work.
    - **Neutral:** The work schedule is fixed (e.g., "Monday-Friday, 9am-5pm", "Full-time, Nights (6pm-6am)") or there is no explicit mention of flexibility. "Open availability" is NOT enough to be considered undesirable unless there is evidence of variable scheduling.
    
    **Important Instructions:**
    1. Only mark "undesired_flexibility" as "YES" if the job description says or implies that the employer can change, rotate, or adjust the employee's work schedule or shifts as needed by the company. 
        - Do NOT mark "undesired_flexibility" as "YES" just because the schedule is at night, on weekends, includes holidays, or is labeled "flexible", unless there is clear evidence that the employer can change or adjust the schedule after hiring.
        - Do NOT mark as undesirable just because multiple shifts or "open availability" are listed—only if it says the employee can be moved or assigned at the company's discretion.
    2. Mark "desirable_flexibility" as "YES" ONLY if the employee clearly chooses their work hours, without company constraint.
    3. If both conditions are present, always prioritize "desirable_flexibility" as "YES" and "undesired_flexibility" as "NO".
    4. If neither, mark both as "NO".
    5. Only consider work hours flexibility. Ignore flexibility about job location (remote, hybrid, work from home), company values, or general requirements not related to work schedule.
    6. For every "YES", provide a single exact, continuous quote from the job description as justification. For every "NO", return "N/A" as the quote.
    
    **Positive (Undesired) Examples:**
    - “May be required to work weekend or holiday shifts as needed.”
    - “MUST be flexible and able to work any shift, including covering for others on short notice.”
    - “Schedule may change at management’s discretion.”
    - “Rotating shifts—your shift may change week to week.”
    *(All of these mean the company controls the hours and can change them as needed.)*
    
    **Positive (Desirable) Examples:**
    - “You may set your own hours.”
    - “Flexible schedule can start between 7am or 8am, your choice.”
    - “Your gig, your schedule.”
    
    **Negative Examples (do NOT mark as undesirable):**
    - “Full-time, Nights (6p-6a) 36 hrs/wk”  # fixed night schedule
    - “Standard 9-5, Monday-Friday.”
    - “Available to work 5pm-8pm or 4pm-8pm Monday-Friday.”
    - “Holiday rotation required” (if it is a pre-set rotation, not changed at company discretion)
    - “1st, 2nd, and 3rd Shifts are now available.” (if employee chooses or is assigned ONE shift)
    - “Schedule is non-negotiable and must be sustained.” (fixed schedule)
    - “We offer various shifts to work with your lifestyle.” (if the employee can choose)
    
    Job Description:
    {description}
    
    Respond ONLY in this JSON format:
    {{
      "undesired_flexibility": "YES or NO",
      "undesired_quote": "exact quote or 'N/A'",
      "desired_flexibility": "YES or NO",
      "desired_quote": "exact quote or 'N/A'",
    }}
    """


# ----------- PROCESS LOGIC ------------
def evaluate_hour_flexibility_local(description, ollama_url=OLLAMA_URL):
    # 1. Usar o condense só para textos realmente grandes
    # condensed = condense_description(description)
    prompt = build_flexibility_prompt(description)
    data = {
        "prompt": prompt,
        "model": MODEL_NAME,
        "format": "json",
        "stream": False,
        "options": {"temperature": 0.0, "num_predict": NUM_PREDICT},
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with httpx.Client() as client:
                logging.info(f"Request to Ollama (Attempt {attempt}/{MAX_RETRIES})")
                start_time = time.time()
                response = client.post(ollama_url, json=data, timeout=60.0)
                elapsed_time = time.time() - start_time
                logging.info(f"Ollama response in {elapsed_time:.3f} sec")
                response.raise_for_status()
                model_output = json.loads(response.json()["response"])

                undesired_flag = 1 if model_output.get("undesired_flexibility", "NO") == "YES" else 0
                undesired_quote = model_output.get("undesired_quote", "")
                desired_flag = 1 if model_output.get("desired_flexibility", "NO") == "YES" else 0
                desired_quote = model_output.get("desired_quote", "")

            # STRICT: Se ambos YES, prioriza o desejável
                if undesired_flag and desired_flag:
                    undesired_flag = 0
                    undesired_quote = "N/A"

                # Tentar extração alternativa de quote para Undesired se necessário
                if undesired_flag and (not undesired_quote.strip() or undesired_quote.strip() == "N/A" or undesired_quote not in description):
                    alt_prompt = f"""
                        The following job description was classified as 'undesirable flexibility' (schedule determined by the employer).
                        Please highlight, copy, or extract the main phrase(s) or excerpt(s) from the text that justify this classification.
                        If no single sentence exists, select the most relevant phrase(s) or combination of phrases that justify the decision.
                        Job Description:
                        {description}
                        Only output the main excerpt(s).
                        """
                    alt_data = {
                        "prompt": alt_prompt,
                        "model": MODEL_NAME,
                        "format": "json",
                        "stream": False,
                        "options": {"temperature": 0.0, "num_predict": NUM_PREDICT},
                    }
                    response_alt = client.post(ollama_url, json=alt_data, timeout=60.0)
                    response_alt.raise_for_status()
                    alt_quote = response_alt.json().get("response", "").strip()
                    undesired_quote = alt_quote if alt_quote else "Model quote not found"

                # Tentar extração alternativa de quote para Desired se necessário
                if desired_flag and (not desired_quote.strip() or desired_quote.strip() == "N/A" or desired_quote not in description):
                    alt_prompt = f"""
                        The following job description was classified as 'desirable flexibility' (hours chosen by the employee).
                        Please highlight, copy, or extract the main phrase(s) or excerpt(s) from the text that justify this classification.
                        If no single sentence exists, select the most relevant phrase(s) or combination of phrases that justify the decision.
                        Job Description:
                        {description}
                        Only output the main excerpt(s).
                        """
                    alt_data = {
                        "prompt": alt_prompt,
                        "model": MODEL_NAME,
                        "format": "json",
                        "stream": False,
                        "options": {"temperature": 0.0, "num_predict": NUM_PREDICT},
                    }
                    response_alt = client.post(ollama_url, json=alt_data, timeout=60.0)
                    response_alt.raise_for_status()
                    alt_quote = response_alt.json().get("response", "").strip()
                    desired_quote = alt_quote if alt_quote else "Model quote not found"

                # --- Pós-processamento: Só preenche quote se dummy == 1 ---
                if undesired_flag == 0:
                    undesired_quote = ""
                if desired_flag == 0:
                    desired_quote = ""

                return undesired_flag, undesired_quote, desired_flag, desired_quote

        except Exception as exc:
            logging.error(f"Ollama attempt {attempt} failed: {exc}")
            if attempt == MAX_RETRIES:
                return 0, "Processing Error", 0, "Processing Error"
            time.sleep(RETRY_SLEEP)
    return 0, "All processing attempts failed", 0, "All processing attempts failed"


# ----------- FILE READING ------------
def read_input_files(directory=INPUT_DIR):
    df_list = []
    for filename in os.listdir(directory):
        if filename.startswith("~$") or filename == ".DS_Store":
            continue
        filepath = os.path.join(directory, filename)
        try:
            if filename.endswith(".csv"):
                temp_df = pd.read_csv(filepath, encoding="utf-8")
            elif filename.endswith(".xlsx"):
                temp_df = pd.read_excel(filepath, engine="openpyxl")
            else:
                logging.warning(f"Skipping unsupported file: {filename}")
                continue

            body_col = next((col for col in temp_df.columns if col.lower() == "body"), None)
            title_col = next((col for col in temp_df.columns if col.lower() == "title_name"), None)
            if not body_col or not title_col:
                logging.warning(f"Columns missing in {filename}. Skipped.")
                continue

            temp_df = temp_df[[title_col, body_col]].rename(
                columns={title_col: "Title", body_col: "Body"}
            )
            df_list.append(temp_df)
        except Exception as e:
            logging.error(f"Error reading {filename}: {e}")

    if not df_list:
        logging.warning("No valid input files found.")
        return None
    return pd.concat(df_list, ignore_index=True)


# ----------- SAVE FILE ------------
def save_with_coloring(df, filepath):
    try:
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Processed_Jobs')
            worksheet = writer.sheets['Processed_Jobs']

            red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
            green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')

            undesired_col = df.columns.get_loc("Undesired_flexibility_dummy") + 1
            desired_col = df.columns.get_loc("Desired_flexibility_dummy") + 1

            for row_idx in range(2, worksheet.max_row + 1):
                undesired_val = worksheet.cell(row=row_idx, column=undesired_col).value
                desired_val = worksheet.cell(row=row_idx, column=desired_col).value

                fill = red_fill if undesired_val == 1 else green_fill if desired_val == 1 else None
                if fill:
                    for cell in worksheet[row_idx]:
                        cell.fill = fill
        logging.info(f"Excel saved: {filepath}")
    except Exception as e:
        logging.error(f"Error saving Excel: {e}")


# ----------- MAIN ------------
def main():
    df = read_input_files()
    if df is None:
        logging.error("No valid input files to process.")
        return

    new_cols = [
        "Undesired_flexibility_dummy", "quote_body_undesired",
        "Desired_flexibility_dummy", "quote_body_desired"
    ]
    for col in new_cols:
        df[col] = ""

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Analyzing jobs"):
        description = row["Body"]
        if pd.isna(description) or not isinstance(description, str) or not description.strip():
            results = (0, "Empty description", 0, "Empty description")
        else:
            results = evaluate_hour_flexibility_local(description)
        df.loc[idx, new_cols] = results
        for q in ["quote_body_undesired", "quote_body_desired"]:
            if df.loc[idx, q] == "N/A":
                df.loc[idx, q] = ""

    final_cols = [
        "Title", "Body",
        "Undesired_flexibility_dummy", "quote_body_undesired",
        "Desired_flexibility_dummy", "quote_body_desired"
    ]
    save_with_coloring(df[final_cols], OUTPUT_PATH)
    logging.info("Processing completed.")


if __name__ == "__main__":
    main()