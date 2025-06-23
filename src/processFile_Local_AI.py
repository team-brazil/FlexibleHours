import httpx
import logging
import pandas as pd
import time
import os
import json
from tqdm import tqdm
from openpyxl.styles import PatternFill

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def evaluate_hour_flexibility_local(description, ollama_url="http://localhost:11434/api/generate"):
    """
    Analyzes a job description to classify its schedule flexibility,
    now with a Python validation layer to ensure a quote is provided for 'YES' answers.
    """
    prompt = f"""
    You are an expert HR analyst bot specializing in classifying workplace flexibility from job descriptions. Your goal is to produce perfectly clean, auditable, and accurate JSON data by following a strict set of rules.
    
    **1. Core Concepts & Definitions**
    
    * **Undesirable Flexibility:** This is company-centric flexibility. It means the employee's schedule is unpredictable or subject to the employer's needs, giving the employee LOW autonomy.
        * *Examples:* Mandatory weekend work, rotating shifts, schedule changed by the manager.
    * **Desirable Flexibility:** This is employee-centric flexibility. It means the employee has significant control and autonomy over WHEN or WHERE they work.
        * *Examples:* Remote work, setting your own hours, flexible schedules.
    * **Neutral:** A job is neutral if it has a standard, fixed schedule (e.g., "Monday to Friday, 9am to 6pm") with no explicit mention of either undesirable or desirable flexibility types. In this case, both classifications will be 'NO'.
    
    **2. Critical Rules of Analysis (NON-NEGOTIABLE)**
    
    * **Rule #1 - The Quote Mandate:** If a classification is 'YES', you MUST provide a direct, exact quote from the text in the 'reason' field. A 'YES' without a quote is an invalid analysis.
    * **Rule #2 - The 'N/A' for 'NO' Mandate:** If a classification is 'NO', you MUST return the string 'N/A' in the 'reason' field. This helps confirm you analyzed the category.
    * **Rule #3 - The Mutual Exclusivity Mandate:** A job CANNOT be both 'undesirable' and 'desirable' at the same time. If you find evidence for both, you must decide which evidence is STRONGER and MORE EXPLICIT. Classify the stronger one as 'YES' and the other MUST BE 'NO'.
    
    **3. Keyword Evidence Guide**
    Use these keywords to find evidence.
    
    * **Evidence for 'Undesired Flexibility':**
        * **Mandatory Non-Standard Hours:** "work on weekends", "work on holidays", "on-call weekends", "weekend shifts", "holiday rotation", "on-call duty", "mandatory overtime".
        * **Fixed & Inflexible Shift Work:** "rotating shifts", "shift work", "fixed shifts", "day and night shifts", "evening shifts required", and specific schedules like "6x1 schedule", "12x36 schedule", "5x2 with rotating days off".
        * **Unpredictable & Company-Controlled Schedule:** "schedule subject to change", "schedule may vary based on business needs", "full schedule availability required", "must be flexible to work various shifts".
    * **Evidence for 'Desirable Flexibility':**
        * **Location Flexibility:** "remote work", "fully remote", "100% remote", "remote-first", "hybrid model", "home office", "work from anywhere", "telecommuting".
        * **Time Flexibility:** "flexible hours", "flextime", "flexible schedule", "time bank", "core hours policy".
        * **Schedule Autonomy:** "set your own schedule", "manage your own hours", "you build your timetable", "asynchronous work", "self-managed schedule".
    
    **4. Your Step-by-Step Task**
    
    1.  **Analyze the Job Description** provided below.
    2.  **Evaluate Evidence** for both 'undesirable' and 'desirable' categories based on the keywords.
    3.  **Apply the Rules,** especially the Mutual Exclusivity rule if needed.
    4.  **Construct the final JSON** output precisely according to the structure.
    
    **Job Description:**
    {description}
    
    **Required JSON Output Structure:**
    ```json
    {{
      "undesired_flexibility": "YES or NO",
      "undesired_reason": "Exact quote from text if 'YES', or 'N/A' if 'NO'.",
      "desired_flexibility": "YES or NO",
      "desired_reason": "Exact quote from text if 'YES', or 'N/A' if 'NO'."
    }}
    ```
    """

    data = {
        "prompt": prompt,
        "model": "llama3.2",
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": 8192,
        },
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            with httpx.Client() as client:
                start_time = time.time()
                logging.info(f"Sending request to Ollama (Attempt {attempt + 1}/{max_retries})...")
                response = client.post(
                    ollama_url,
                    json=data,
                    timeout=60.0,
                )

                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"Generation time: {elapsed_time:.3f} seconds")

                response.raise_for_status()

                text_response = response.json()["response"]
                final_response_json = json.loads(text_response)

                # --- RAW VALUES FROM THE MODEL ---
                model_undesired_flex = 1 if final_response_json.get("undesired_flexibility") == "YES" else 0
                model_undesired_reason = final_response_json.get("undesired_reason", "")
                model_desired_flex = 1 if final_response_json.get("desired_flexibility") == "YES" else 0
                model_desired_reason = final_response_json.get("desired_reason", "")

                # *** NEW VALIDATION LOGIC (SAFETY CHECK) ***
                # For Undesired
                if model_undesired_flex == 1 and (model_undesired_reason.strip() == "" or model_undesired_reason.strip() == "N/A"):
                    logging.warning(f"Inconsistency found for 'Undesired': Model returned YES without a quote. Overriding to NO.")
                    final_undesired_flex = 0
                    final_undesired_reason = ""
                else:
                    final_undesired_flex = model_undesired_flex
                    final_undesired_reason = model_undesired_reason

                # For Desired
                if model_desired_flex == 1 and (model_desired_reason.strip() == "" or model_desired_reason.strip() == "N/A"):
                    logging.warning(f"Inconsistency found for 'Desired': Model returned YES without a quote. Overriding to NO.")
                    final_desired_flex = 0
                    final_desired_reason = ""
                else:
                    final_desired_flex = model_desired_flex
                    final_desired_reason = model_desired_reason

                logging.info("Ollama response received and successfully validated.")
                return final_undesired_flex, final_undesired_reason, final_desired_flex, final_desired_reason

        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed with an unexpected error: {e}")
            if attempt + 1 == max_retries:
                return 0, "", 0, ""

        time.sleep(5)

    return 0, "All processing attempts failed", 0, "All processing attempts failed"


def read_input_files(directory="../input"):
    """
    Reads .csv and .xlsx files from the input directory, keeping and renaming
    the columns of interest to 'Title' and 'Body'.
    """
    df_list = []
    files_were_read = False

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
                logging.warning(f"File {filename} is not .csv or .xlsx. Ignoring.")
                continue

            # Kept as in the previous version to read the input file correctly
            body_column = next((col for col in temp_df.columns if col.lower() == "body"), None)
            title_column = next((col for col in temp_df.columns if col.lower() == "title"), None)

            if body_column is None or title_column is None:
                logging.warning(f"File {filename} does not have 'Body' and/or 'Title' columns. Ignoring.")
                continue

            temp_df = temp_df[[title_column, body_column]]
            temp_df.rename(columns={title_column: 'Title', body_column: 'Body'}, inplace=True)
            df_list.append(temp_df)
            files_were_read = True

        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")

    if not files_were_read:
        logging.warning(
            "No valid files found or processed. Check the files in the 'input' folder."
        )
        return None

    return pd.concat(df_list, ignore_index=True) if df_list else None


def save_with_coloring(df, filepath):
    """Saves the DataFrame to an Excel file, coloring the rows."""
    try:
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Processed_Jobs')

            workbook = writer.book
            worksheet = writer.sheets['Processed_Jobs']

            red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
            green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')

            try:
                undesired_col_idx = df.columns.get_loc("Undesired_flexibility_dummy") + 1
                desired_col_idx = df.columns.get_loc("Desired_flexibility_dummy") + 1
            except KeyError as e:
                logging.error(f"Column not found in the final DataFrame: {e}. Coloring will not be possible.")
                return

            for row_idx in range(2, worksheet.max_row + 1):
                undesired_cell_value = worksheet.cell(row=row_idx, column=undesired_col_idx).value
                desired_cell_value = worksheet.cell(row=row_idx, column=desired_col_idx).value

                fill_to_apply = None
                if undesired_cell_value == 1:
                    fill_to_apply = red_fill
                elif desired_cell_value == 1:
                    fill_to_apply = green_fill

                if fill_to_apply:
                    for cell in worksheet[row_idx]:
                        cell.fill = fill_to_apply

        logging.info(f"File saved successfully with colors at: {filepath}")

    except Exception as e:
        logging.error(f"Error saving the Excel file with formatting: {e}")


def main():
    """Main function to read, process, and save the data."""
    df = read_input_files()
    if df is None:
        logging.error("No valid input files found. Exiting.")
        return

    new_columns = [
        "Undesired_flexibility_dummy", "quote_body_undesired",
        "Desired_flexibility_dummy", "quote_body_desired"
    ]
    for col in new_columns:
        df[col] = ""

    for index, row in tqdm(df.iterrows(), total=len(df), desc="Analyzing jobs"):
        description = row["Body"]
        if pd.isna(description) or not isinstance(description, str) or description.strip() == "":
            logging.warning(f"Empty or invalid description on row {index}. Skipping.")
            results = (0, "Empty description", 0, "Empty description")
        else:
            results = evaluate_hour_flexibility_local(description)

        df.loc[index, new_columns] = results

        # Clears the 'N/A' that may come from the model to leave the Excel cell empty
        if df.loc[index, "quote_body_undesired"] == "N/A":
            df.loc[index, "quote_body_undesired"] = ""
        if df.loc[index, "quote_body_desired"] == "N/A":
            df.loc[index, "quote_body_desired"] = ""

    final_columns_order = [
        "Title", "Body",
        "Undesired_flexibility_dummy", "quote_body_undesired",
        "Desired_flexibility_dummy", "quote_body_desired"
    ]
    final_df = df[final_columns_order]

    output_filepath = os.path.join("../output/results", "Job_postings_processed.xlsx")
    save_with_coloring(final_df, output_filepath)

    logging.info("Processing completed.")


if __name__ == "__main__":
    main()