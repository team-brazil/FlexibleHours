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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_input_files(directory="../input"):
    """
    Reads CSV and XLSX files from the 'input' directory and returns a DataFrame.
    Assumes the files have a column named 'BODY'.
    """
    df_list = []
    for filename in os.listdir(directory):
        # Ignores temporary Excel files that start with ~$
        if filename.startswith("~$"):
            continue

        filepath = os.path.join(directory, filename)
        try:
            if filename.endswith(".csv"):
                temp_df = pd.read_csv(filepath, encoding="utf-8")
            elif filename.endswith(".xlsx"):
                temp_df = pd.read_excel(filepath, engine='openpyxl')
            else:
                logging.warning(f"File {filename} is not .csv or .xlsx. Ignoring.")
                continue

            # Checks if the 'BODY' column exists in the DataFrame (case-insensitive)
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
    """
    Evaluate the flexibility of a job description using the Gemini model.
    """
    # Delay to avoid hitting API rate limits
    time.sleep(4)
    genai.configure(api_key=api_key, transport="rest")
    model = genai.GenerativeModel('gemini-1.5-flash')
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
    num_retries = 0
    while num_retries < max_retries:
        try:
            logging.info(f"Sending request to Gemini API with description: {description[:100]}...")
            response = model.generate_content(contents=prompt)

            # Extract JSON from the markdown code block in the response
            json_match = re.search(r"```json\n(.*?)\n```", response.text, re.DOTALL)
            if json_match:
                json_text = json_match.group(1)
                response_json = json.loads(json_text)
            else:
                logging.warning(f"JSON not found in response: {response.text[:100]}...")
                response_json = {"undesired_flexibility": "Error", "reason": "JSON not found in response"}

            unwanted_flexibility = response_json.get('undesired_flexibility', 'No')
            reason = response_json.get('reason', 'No justification')
            return unwanted_flexibility, reason

        except Exception as e:
            # Handle rate limiting errors by waiting and retrying
            if "429" in str(e):
                logging.error(f"Error requesting Gemini API (Rate Limit): {e}")
                time.sleep(60)
                num_retries += 1
            else:
                logging.error(f"Error requesting Gemini API: {e}")
                return "Error", f"Error accessing API: {e}"
    logging.error(f"Maximum retries ({max_retries}) reached.")
    return "Error", "Maximum retries reached."


def process_batch(batch, api_keys, key_index):
    """
    Processes a batch of job descriptions using a specific API key.
    """
    results = []
    for i, description in enumerate(batch):
        # Cycle through the available API keys for each new request in the batch
        api_key = api_keys[key_index % len(api_keys)]
        unwanted_flexibility, reason = evaluate_flexibility_hours_gemini(description, api_key)
        results.append((unwanted_flexibility, reason))
    return results


def calculate_dispersion(row, num_loops):
    """
    Calculates if there is dispersion in the results of multiple loops for a given row.
    Returns 'Yes' if results differ, 'No' otherwise.
    """
    results = [row[f'undesired_flexibility_{i}'] for i in range(1, num_loops + 1)]
    # A set will contain only unique values. If its length is greater than 1, there was a difference.
    if len(set(results)) > 1:
        return "Yes"
    else:
        return "No"


def main(num_loops=1, batch_size=1, num_processes=2):
    """
    Main function to orchestrate the reading, processing, and saving of job description analysis.
    num_loops is the number of times each question is repeated to check for consistent results
    num_processes is the number of processes the task is divided among.
    batch_size is the number of samples per batch.
    """
    load_dotenv()

    api_keys = os.getenv("API_KEYS")
    if api_keys:
        api_keys = api_keys.split(',')
    else:
        # Fallback to another possible environment variable name
        api_keys = os.getenv("api_keys", [])

    if not api_keys:
        raise ValueError("API_KEYS not found. Check your .env file")

    # The number of parallel processes should not exceed the number of available API keys
    if num_processes > len(api_keys):
        logging.warning(
            f"The number of processes ({num_processes}) is greater than the number of API keys ({len(api_keys)}). "
            f"Reducing the number of processes to {len(api_keys)}."
        )
        num_processes = len(api_keys)

    # Reading input files
    df = read_input_files()
    if df is None:
        return

    # Find the 'BODY' column using a case-insensitive search
    body_column = next((col for col in df.columns if col.lower() == 'body'), None)

    for i in range(1, num_loops + 1):
        logging.info(f"Starting loop {i}...")
        df[f'undesired_flexibility_{i}'] = ""
        df[f'reason_{i}'] = ""

        # Divide the DataFrame into batches for processing
        batches = [df[body_column][j:j + batch_size].tolist() for j in range(0, len(df), batch_size)]

        # Prepare arguments for the multiprocessing pool, distributing keys
        args_list = [(batch, api_keys, j) for j, batch in enumerate(batches)]

        # Use a multiprocessing Pool to process batches in parallel
        with multiprocessing.Pool(processes=num_processes) as pool:
            # tqdm provides a progress bar
            results = list(tqdm(pool.starmap(process_batch, args_list), total=len(args_list)))

        # The pool returns a list of lists; flatten it into a single list of results
        flattened_results = [item for sublist in results for item in sublist]
        df[f'undesired_flexibility_{i}'], df[f'reason_{i}'] = zip(*flattened_results)

    # Calculate the dispersion if the process was run more than once
    if num_loops > 1:
        logging.info("Calculating dispersion...")
        df['dispersion'] = df.apply(calculate_dispersion, axis=1, num_loops=num_loops)

    # Save the results to an Excel file
    output_filepath = os.path.join("../output", "Test_Gemini.xlsx")
    try:
        df.to_excel(output_filepath, index=False, engine='openpyxl')
        logging.info(f"File saved successfully in: {output_filepath}")
    except Exception as e:
        logging.error(f"Error saving Excel file: {e}")

    print("Processing completed. Results saved in output/Test_Gemini.xlsx")


if __name__ == "__main__":
    main()