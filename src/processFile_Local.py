import httpx
import logging
import pandas as pd
import time
import os
import json
from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def evaluate_hour_flexibility_local(description, ollama_url="http://localhost:11434/api/generate"):
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

    data = {
        "prompt": prompt,
        "model": "phi",
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": 100,
        },
    }

    try:
        with httpx.Client() as client:
            start_time = time.time()
            logging.info(f"Sendind requisition to Ollama...")
            response = client.post(
                ollama_url,
                json=data,
                timeout=120.0,
            )
            #Ver desempenho
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Time of generation: {elapsed_time:.3f} seconds")

            response.raise_for_status()
            response_json = response.json()
            text_response = response_json["response"]

            response_json_final = json.loads(text_response)
            unwanted_flexibility = response_json_final.get("undesired_flexibility", "Não")
            justification = response_json_final.get("reason", "No justification")
            logging.info("Response received from Ollama successfully.")
            return unwanted_flexibility, justification

    except httpx.RequestError as e:
        logging.error(f"Ollama request error: {e}")
        return "Erro", f"Request error: {e}"
    except httpx.HTTPStatusError as e:
        logging.error(f"Ollama HTTP Error: {e} - Response: {response.text}")
        return "Erro", f"Erro HTTP: {e}"
    except json.JSONDecodeError as e:
        logging.error(f"Ollama response is not valid JSON: {e}")
        return "Erro", "Invalid Ollama response (non-JSON)"
    except Exception as e:
        logging.error(f"Unexpected error when calling Ollama: {e}")
        return "Erro", str(e)


def read_input_files(diretorio="../input"):
    df_list = []
    readed_files = False

    for filename in os.listdir(diretorio):
        if filename.startswith("~$"):
            continue

        filepath = os.path.join(diretorio, filename)
        try:
            if filename.endswith(".csv"):
                temp_df = pd.read_csv(filepath, encoding="utf-8")
            elif filename.endswith(".xlsx"):
                temp_df = pd.read_excel(filepath, engine="openpyxl")
            else:
                logging.warning(f"File {filename} is not .csv or .xlsx. Ignoring.")
                continue

            body_column = next(
                (col for col in temp_df.columns if col.lower() == "body"), None
            )
            if body_column is None:
                logging.warning(f"File {filename} does not have the 'BODY' column. Ignoring.")
                continue

            df_list.append(temp_df)
            readed_files = True

        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")

    if not readed_files:
        logging.warning(
            "No valid files found or processed. Check files in the 'input' folder'."
        )
        return None

    if not df_list:
        return None

    return pd.concat(df_list, ignore_index=True)


def calculate_dispersion(row, num_loops):
    """Calculates dispersion."""
    results = [row[f"undesired_flexibility_{i}"] for i in range(1, num_loops + 1)]
    return "Yes" if len(set(results)) > 1 else "No"


def main(num_loops=1, batch_size=1):
    """Main function."""
    # Reading input files
    df = read_input_files()
    if df is None:
        logging.error("No valid input file found. Ending.")
        return

    body_column = next((col for col in df.columns if col.lower() == "body"), None)

    for i in range(1, num_loops + 1):
        logging.info(f"Starting loop {i}...")
        df[f"undesired_flexibility_{i}"] = ""
        df[f"reason_{i}"] = ""

        for index, row in tqdm(df.iterrows(), total=len(df), desc=f"Loop {i}"):
            description = row[body_column]
            unwanted_flexibility, justification = evaluate_hour_flexibility_local(description)
            df.loc[index, f"undesired_flexibility_{i}"] = unwanted_flexibility
            df.loc[index, f"reason_{i}"] = justification

    # 2. Calcular a dispersão
    logging.info("Calculating dispersion...")
    df["dispersion"] = df.apply(calculate_dispersion, axis=1, num_loops=num_loops)

    # 3. Salvar os resultados
    output_filepath = os.path.join("../output", "Test_Local.xlsx")
    try:
        df.to_excel(output_filepath, index=False, engine="openpyxl")
        logging.info(f"File saved successfully in: {output_filepath}")
    except Exception as e:
        logging.error(f"Error saving Excel file: {e}")

    logging.info("Processing completed.")


if __name__ == "__main__":
    main()