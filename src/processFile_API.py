import pandas as pd
import google.generativeai as genai
import json
import os
from dotenv import load_dotenv
import logging
from tqdm import tqdm
import time
import re
import multiprocessing

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_input_files(diretorio="../input"):
    """
    Reads CSV and XLSX files from the 'input' directory and returns a DataFrame.
    Assumes the files have a column named 'BODY'.
    """
    df_list = []
    for filename in os.listdir(diretorio):
        if filename.startswith("~$"):
            continue

        filepath = os.path.join(diretorio, filename)
        try:
            if filename.endswith(".csv"):
                temp_df = pd.read_csv(filepath, encoding="utf-8")
            elif filename.endswith(".xlsx"):
                temp_df = pd.read_excel(filepath, engine='openpyxl')
            else:
                logging.warning(f"Arquivo {filename} não é .csv ou .xlsx. Ignorando.")
                continue

            # Checks if column 'BODY' exists in DataFrame
            if 'body' not in temp_df.columns.str.lower().tolist():
                logging.warning(f"File {filename} does not have the 'BODY' column. Ignoring.")
                continue
            df_list.append(temp_df)

        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")

    if not df_list:
        logging.warning("No valid files found or processed. Check files in the 'input' folder.")
        return None

    return pd.concat(df_list, ignore_index=True)


def evaluate_flexibility_hours_gemini(description, api_key, max_retries=3):
    """Evaluate the flexibility of a job description using the Gemini model."""
    time.sleep(4)
    genai.configure(api_key=api_key, transport="rest")
    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = (
        "You are an expert in job vacancy analysis. "
        "Your goal is to identify if a job vacancy presents 'Undesired Flexibility'. "
        "This is an important concept: 'Undesired Flexibility' occurs when a job vacancy claims "
        "to offer flexibility, but this flexibility is only for the company, not for the worker. "
        "For example, the job may require the employee to work irregular hours, weekends, "
        "holidays, or rotating shifts, without offering the option to choose these hours. This is different from "
        "real flexibility, where the employee can choose their hours or has control over their schedule. "
        "Analyze the job proposal text below and determine whether it is a case of 'Undesired Flexibility' or not. "
        f"Job proposal text: {description}. "
        "Respond in the following format: 'undesired_flexibility': (Yes or No) and 'reason': (your explanation). "
        "Respond using a single JSON without any other words. "
    )
    num_retries = 0
    while num_retries < max_retries:
        try:
            logging.info(f"Sending request to Gemini API with description: {description[:100]}...")
            response = model.generate_content(contents=prompt)

            json_match = re.search(r"```json\n(.*?)\n```", response.text, re.DOTALL)
            if json_match:
                json_text = json_match.group(1)
                resposta_json = json.loads(json_text)
            else:
                logging.warning(f"JSON not found in response:: {response.text[:100]}...")
                resposta_json = {"undesired_flexibility": "Erro", "reason": "JSON not found in response"}

            unwanted_flexibility = resposta_json.get('undesired_flexibility', 'Não')
            reason = resposta_json.get('reason', 'No justification')
            return unwanted_flexibility, reason

        except Exception as e:
            if "429" in str(e):
                logging.error(f"Error requesting Gemini API: {e}")
                time.sleep(60)
                num_retries += 1
            else:
                logging.error(f"Error requesting Gemini API: {e}")
                return "Erro", f"Error accessing API: {e}"
    logging.error(f"Maximum retries ({max_retries}) reached.")
    return "Erro", "Maximum retries reached."


def process_batch(batch, api_keys, key_index):
    """Processes a batch of job descriptions using a specific API key."""
    results = []
    for i, description in enumerate(batch):
        api_key = api_keys[key_index % len(api_keys)]
        unwanted_flexibility, reason = evaluate_flexibility_hours_gemini(description, api_key)
        results.append((unwanted_flexibility, reason))
    return results


def calculate_dispersion(row, num_loops):
    results = [row[f'undesired_flexibility_{i}'] for i in range(1, num_loops + 1)]
    if len(set(results)) > 1:
        return "Yes"  
    else:
        return "No"  


def main(num_loops=1, batch_size=1, num_processes=2):
    load_dotenv()

    api_keys = os.getenv("API_KEYS")
    if api_keys:
        api_keys = api_keys.split(',')
    else:
        api_keys = os.getenv("api_keys", [])

    if not api_keys:
        raise ValueError("API_KEYS not found. Check .env file")

    if num_processes > len(api_keys):
        logging.warning(
            f"The number of processes ({num_processes}) is greater than the number of API keys ({len(api_keys)}). Reducing the number of processes to {len(api_keys)}.")
        num_processes = len(api_keys)

    #Reading input files
    df = read_input_files()
    if df is None:
        return

    # Find 'BODY' column with case-insensitive
    body_column = next((col for col in df.columns if col.lower() == 'body'), None)

    for i in range(1, num_loops + 1):
        logging.info(f"Starting loop {i}...")
        df[f'undesired_flexibility_{i}'] = ""
        df[f'reason_{i}'] = ""

        batches = [df[body_column][j:j + batch_size].tolist() for j in range(0, len(df), batch_size)]

        # # Prepare arguments for starmap, distributing keys
        args_list = [(batch, api_keys, j) for j, batch in enumerate(batches)]

        with multiprocessing.Pool(processes=num_processes) as pool:
            results = list(tqdm(pool.starmap(process_batch, args_list), total=len(args_list)))

        flattened_results = [item for sublist in results for item in sublist]
        df[f'undesired_flexibility_{i}'], df[f'reason_{i}'] = zip(*flattened_results)

    # Calcular a dispersão
    logging.info("Calculating dispersion...")
    df['dispersion'] = df.apply(calculate_dispersion, axis=1, num_loops=num_loops)

    # Save
    output_filepath = os.path.join("../output", "Test_Gemini.xlsx")
    try:
        df.to_excel(output_filepath, index=False, engine='openpyxl')
        logging.info(f"File saved successfully in: {output_filepath}")
    except Exception as e:
        logging.error(f"Error saving Excel file: {e}")

    print("Processing completed. Results saved in output/Test_Gemini.xlsx")


if __name__ == "__main__":
    main()