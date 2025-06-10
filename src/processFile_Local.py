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
    prompt = f"""
    You are a specialized JSON-generating bot. Your ONLY output MUST be a single, valid JSON object. Do not include any explanatory text, apologies, or any other content outside of the JSON structure.
    
    You are an expert in job vacancy analysis. Your task is to identify 'Undesired Flexibility' and 'Desirable Flexibility' within a given job description.
    
    **Definitions:**
    
    1.  **Undesired Flexibility:** This occurs when a job vacancy mentions flexibility, but this flexibility primarily benefits the company, not the worker. The worker has little to no choice or control over these flexible arrangements.
        * **Examples of Undesired Flexibility:**
            * Requiring irregular hours or unpredictable schedules dictated by the company.
            * Mandatory weekend or holiday work as a regular part of the job without extra compensation or employee choice.
            * Rotating shifts controlled by the company, not the employee.
            * "Flexible hours" meaning the employee must be available whenever the company needs them.
            * Employer may change job duties at the last-minute without providing any formal notice.
    
    2.  **Desirable Flexibility:** This refers to genuine flexibility where the employee has significant input, choice, or control over their working arrangements, allowing for better work-life balance.
        * **Examples of Desirable Flexibility:**
            * Employee can choose their start and end times within a given range (flextime).
            * Option for remote work (fully remote or hybrid) where the employee has some say in the arrangement.
            * Compressed workweeks (e.g., working 4 longer days for a 3-day weekend) if offered as an employee option.
            * Ability to adjust schedules for personal appointments with an agreed-upon way to make up work.
            * Employer offers or agrees to provide an advance notice of employees' work schedules and shifts.
            * Control over break times.
            * Job sharing options.
    
    **Task:**
    
    Analyze the following job proposal text:
    {description}
    
    Based on your analysis, provide a response in a single JSON object using the EXACT structure below.
    
    **JSON Output Structure:**
    
    ```json
    {{
      "undesired_flexibility": "YES" or "NO",
      "undesired_reason": "A concise explanation of why it is or is not Undesired Flexibility. Quote or refer to specific parts of the job description that led to your conclusion. If 'NO', briefly state why it doesn't meet the criteria.",
      "undesired_difficulty_classification": "If you encountered ambiguity or difficulty in classifying Undesired Flexibility (e.g., vague wording, conflicting information), explain your reasoning here. If the classification was straightforward, leave this field blank.",
      "desired_flexibility": "YES" or "NO",
      "desired_reason": "A concise explanation of why it is or is not Desirable Flexibility. Quote or refer to specific parts of the job description that led to your conclusion. If 'NO', briefly state why it doesn't meet the criteria.",
      "desired_difficulty_classification": "If you encountered ambiguity or difficulty in classifying Desirable Flexibility (e.g., vague wording, conflicting information), explain your reasoning here. If the classification was straightforward, leave this field blank."
    }}
    ```
    """

    data = {
        "prompt": prompt,
        "model": "gemma3",
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0.2,
            "num_predict": 200,
        },
    }

    try:
        with httpx.Client() as client:
            start_time = time.time()
            logging.info("Sendind requisition to Ollama...")
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
            undesired_flexibility = response_json_final.get("undesired_flexibility", "No")
            undesired_reason = response_json_final.get("undesired_reason", "No justification")
            undesired_difficulty_classification = response_json_final.get("undesired_difficulty_classification", "")
            desired_flexibility = response_json_final.get("desired_flexibility", "No")
            desired_reason = response_json_final.get("desired_reason", "No justification")
            desired_difficulty_classification = response_json_final.get("desired_difficulty_classification", "")
            logging.info("Response received from Ollama successfully.")
            return undesired_flexibility, undesired_reason, undesired_difficulty_classification, desired_flexibility, desired_reason, desired_difficulty_classification

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


def main():
    """Main function."""
    # Reading input files
    df = read_input_files()
    if df is None:
        logging.error("No valid input file found. Ending.")
        return

    body_column = next((col for col in df.columns if col.lower() == "body"), None)

    logging.info(f"Starting avaluation")
    df[f"undesired_flexibility"] = ""
    df[f"undesired_reason"] = ""
    df[f"undesired_difficulty_classification"] = ""
    df[f"desired_flexibility"] = ""
    df[f"desired_reason"] = ""
    df[f"desired_difficulty_classification"] = ""

    for index, row in tqdm(df.iterrows(), total=len(df)):
        description = row[body_column]
        undesired_flexibility, undesired_reason, undesired_difficulty_classification, desired_flexibility, desired_reason, desired_difficulty_classification = evaluate_hour_flexibility_local(description)
        df.loc[index, f"undesired_flexibility"] = undesired_flexibility
        df.loc[index, f"undesired_reason"] = undesired_reason
        df.loc[index, f"undesired_difficulty_classification"] = undesired_difficulty_classification
        df.loc[index, f"desired_flexibility"] = desired_flexibility
        df.loc[index, f"desired_reason"] = desired_reason
        df.loc[index, f"desired_difficulty_classification"] = desired_difficulty_classification


    # Saving files
    output_filepath = os.path.join("../output", "Test_Local.xlsx")
    try:
        df.to_excel(output_filepath, index=False, engine="openpyxl")
        logging.info(f"File saved successfully in: {output_filepath}")
    except Exception as e:
        logging.error(f"Error saving Excel file: {e}")

    logging.info("Processing completed.")


if __name__ == "__main__":
    main()