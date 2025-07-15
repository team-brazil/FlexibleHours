import re
import os
import nltk
import pandas as pd
import multiprocessing
from tqdm import tqdm
from nltk.stem import WordNetLemmatizer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Download NLTK data (only if necessary)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)
try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet', quiet=True)
# try: # Vader is not directly used in this version
#     nltk.data.find('sentiment/vader_lexicon')
# except LookupError:
#     nltk.download('vader_lexicon', quiet=True)


def read_input_files(directory="../input"):
    df_list = []
    for filename in os.listdir(directory):
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

            # Check if the 'BODY' column exists (case-insensitive)
            body_column_present = any(col.lower() == 'body' for col in temp_df.columns)
            if not body_column_present:
                logging.warning(f"File {filename} does not contain the 'BODY' column. Ignoring.")
                continue
            # Rename columns to lowercase for easier access
            temp_df.columns = [col.lower() for col in temp_df.columns]

            df_list.append(temp_df)

        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")

    if not df_list:
        logging.warning("No valid files found or processed. Check files in the 'input' folder.")
        return None

    return pd.concat(df_list, ignore_index=True)


def preprocess_text(text):
    if not isinstance(text, str):
        return ""
    text = text.lower() # Convert to lowercase
    # Remove punctuation, but keep hyphens within words (e.g., on-call) and slashes (e.g., 24/7)
    text = re.sub(r'[^\w\s\-/]', '', text)
    tokens = re.findall(r'\b[\w\-/]+\b', text)
    lemmatizer = WordNetLemmatizer()
    try:
        tokens = [lemmatizer.lemmatize(token) for token in tokens]
    except LookupError:
        logging.warning("WordNet not found for lemmatization. Proceeding without lemmatization.")
        pass

    return ' '.join(tokens)


def analyze_undesired_flexibility(body_text):
    preprocessed_body = preprocess_text(body_text)

    # LEXICON DEFINITION BASED ON PATTERNS (REGEX)
    # Each item: (regex, {'reason': "Description", 'score': SCORE_VALUE})
    # Higher scores indicate greater "undesirability"
    # Scores:
    # 1-2: General mention, ambiguous, or mildly concerning
    # 3-4: Moderately concerning, likely undesirable
    # 5: Strongly undesirable, critical
    patterns_lexicon = [
        # General/Ambiguous Mentions (Low Scores)
        (r'\bflexib\w*\s+(hour|schedule|time|shift)', {'reason': "Flexibility in hours/schedule mentioned", 'score': 1}),
        (r'\b(full-time|part-time).*(and|or).*(full-time|part-time)', {'reason': "Combination of FT/PT mentioned", 'score': 1}),
        (r'(night|weekend|holiday|evening).*\b(shift|work|hour)', {'reason': "Mention of non-standard work hours (night, weekend, etc.)", 'score': 2}),
        (r'irregular.*\b(hour|schedule)', {'reason': "Mention of irregular hours", 'score': 2}),
        (r'early\s+morning|late\s+night', {'reason': "Mention of extreme hours (early morning, late night)", 'score': 2}),
        (r'rotating.*shift', {'reason': "Mention of rotating shifts", 'score': 2}),
        (r'split.*shift', {'reason': "Mention of split shifts", 'score': 2}),
        (r'float\w*\s+holiday', {'reason': "Mention of floating holidays", 'score': 1}),
        (r'\bwork\b.*holiday', {'reason': "Mention of work on holidays", 'score': 2}),
        (r'(schedule|hour|shift).*vary', {'reason': "Mention of varying hours/schedule", 'score': 2}),
        (r'extended.*hour', {'reason': "Mention of extended hours", 'score': 2}),
        (r'(availab\w*|work).*\b(weekend|holiday)', {'reason': "Mention of weekend or holiday availability/work", 'score': 2}),
        (r'shift.*\b(assign\w*|schedule)', {'reason': "Mention of variable shift assignment/schedule", 'score': 2}),
        (r'\b(as|when)\s+(needed|required)', {'reason': "'As needed/required' clause present", 'score': 3}), # Can be problematic
        (r'\b(subject\s+to|depend.*on|accord.*to).*availability', {'reason': "'Subject to/depending on availability' clause present", 'score': 3}),
        (r'unpredict\w+\s+(hour|schedule)', {'reason': "Mention of unpredictable hours/schedule", 'score': 3}),
        (r'rotat\w+\s+(shift|schedule|roster)', {'reason': "Mention of rotating shifts/schedule/roster", 'score': 2}),
        (r'(part[- ]time|casual).*(flexib\w*|as\s+needed|when\s+needed)', {'reason': "Part-time/casual with flexibility/'as needed' clause mentioned", 'score': 2}),
        (r'remote.*(irregular|unpredict\w+|vary).*hour', {'reason': "Mention of remote work with irregular/variable hours", 'score': 2}),
        (r'travel.*(require|necess).*flexib\w*', {'reason': "Mention of travel requiring flexibility", 'score': 2}),

        # Strong Indicators of Undesirable Flexibility (High Scores)
        (r'(availab\w*|work).*24/7', {'reason': "UNDESIRED: 24/7 availability/work required", 'score': 5}),
        (r'on[- ]call', {'reason': "UNDESIRED: On-call work", 'score': 5}),
        (r'(change|vary|alter).*(without|no|little|minimal)\s+(prior\s+)?notice', {'reason': "UNDESIRED: Changes/Variations without (or with little) prior notice", 'score': 5}),
        (r'based\s+(primarily|solely|exclusively)?\s*on\s+(our|business|company|employer|management|client|customer)\s+(need|needs|requirement|requirements|demand|demands)', {'reason': "UNDESIRED: Hours based (primarily/exclusively) on employer/business needs", 'score': 5}),
        (r'overtime.*\b(required|needed|necessary|expected|mandat\w+|compulsory)', {'reason': "UNDESIRED: Mandatory/Required/Expected overtime", 'score': 5}),
        (r'\b(work|schedule|hour|shift).*(on demand|at short notice|last minute)', {'reason': "UNDESIRED: Work on demand / at short notice / last minute", 'score': 5}),
        (r'total\s+availability', {'reason': "UNDESIRED: Total availability required", 'score': 5}),
        (r'complete\s+flexibility', {'reason': "UNDESIRED: Complete flexibility required (often employer-centric)", 'score': 5}),
        (r'no\s+(real\s+)?choice\s+of\s+schedule', {'reason': "UNDESIRED: No (real) choice of schedule for employee", 'score': 5}),
        (r'non[- ]negotiable\s+(hour|schedule)', {'reason': "UNDESIRED: Non-negotiable hours/schedule", 'score': 5}),
        (r'\b(must|require\w*)\s+(be\s+)?(fully|completely|highly|extremely|totally)\s*flexible', {'reason': "UNDESIRED: Requirement to be (fully/extremely) flexible (employer demand)", 'score': 5}),
        (r'(hour|schedule|shift).*(will|can|may)\s+(vary|change)\s+(significantly|drastically|at any time)', {'reason': "UNDESIRED: Hours/Schedule can vary significantly / at any time", 'score': 5}),
        (r'flexib\w*\s+(to\s+)?(meet|suit|accommodate|for)\s+(our|the\s+company|business|employer|client)\s+(need|needs|demand|demands|requirement|requirements)', {'reason': "UNDESIRED: Flexibility to meet employer/business needs", 'score': 5}),
        (r'on[- ]call\s+for\s+(all\s+)?(emergenc\w+|urgent\s+matters|critical\s+issues)', {'reason': "UNDESIRED: On-call for emergencies/urgent matters", 'score': 5}),
        (r'unpredictable\s+work\s+patterns', {'reason': "UNDESIRED: Unpredictable work patterns (stronger than just 'unpredictable hours')", 'score': 5}),
        (r'(employer|company|management)\s+(dictates|determines|sets)\s+(the\s+)?schedule', {'reason': "UNDESIRED: Employer dictates/determines schedule", 'score': 5}),
    ]

    # LEXICON DEFINITION BASED ON CONTEXT (CO-OCCURRENCE OF WORDS)
    # Each item: ((word1, word2), {'reason': "Description", 'score': SCORE_VALUE})
    contexts_lexicon = [
        # General/Ambiguous Mentions
        (('flexib', 'schedule'), {'reason': "Context: 'flexib' and 'schedule' mentioned", 'score': 1}),
        (('work', 'weekend'), {'reason': "Context: 'work' and 'weekend' mentioned", 'score': 1}),
        (('availab', 'holiday'), {'reason': "Context: 'availab' and 'holiday' mentioned", 'score': 1}),
        (('shift', 'assign'), {'reason': "Context: 'shift' and 'assign' mentioned", 'score': 1}),
        (('casual', 'flexible'), {'reason': "Context: 'casual' and 'flexible' mentioned", 'score': 1}),

        (('hour', 'vary'), {'reason': "Context: 'hour' and 'vary' mentioned", 'score': 2}),
        (('shift', 'rotate'), {'reason': "Context: 'shift' and 'rotate' mentioned", 'score': 2}),
        (('hour', 'unpredict'), {'reason': "Context: 'hour' and 'unpredict' mentioned", 'score': 2}),
        (('schedule', 'vary'), {'reason': "Context: 'schedule' and 'vary' mentioned", 'score': 2}),

        # Strong Indicators of Undesirable Flexibility
        (('business', 'need'), {'reason': "UNDESIRED Context: 'business' and 'need' co-occur", 'score': 4}),
        (('company', 'need'), {'reason': "UNDESIRED Context: 'company' and 'need' co-occur", 'score': 4}),
        (('employer', 'need'), {'reason': "UNDESIRED Context: 'employer' and 'need' co-occur", 'score': 4}),
        (('flexib', 'company'), {'reason': "UNDESIRED Context: 'flexib' and 'company' (suggesting company needs)", 'score': 4}),
        (('flexib', 'business'), {'reason': "UNDESIRED Context: 'flexib' and 'business' (suggesting business needs)", 'score': 4}),
        (('flexib', 'employer'), {'reason': "UNDESIRED Context: 'flexib' and 'employer' (suggesting employer needs)", 'score': 4}),

        (('overtime', 'require'), {'reason': "UNDESIRED Context: 'overtime' and 'require' co-occur", 'score': 5}),
        (('overtime', 'mandat'), {'reason': "UNDESIRED Context: 'overtime' and 'mandat' (from mandatory) co-occur", 'score': 5}),
        (('remote', 'total availability'), {'reason': "UNDESIRED Context: 'remote' and 'total availability' co-occur", 'score': 5}),
        (('on-call', 'emergency'), {'reason': "UNDESIRED Context: 'on-call' and 'emergency' co-occur", 'score': 5}),
        (('availability', 'demand'), {'reason': "UNDESIRED Context: 'availability' and 'demand' co-occur", 'score': 5}),
        (('schedule', 'dictate'), {'reason': "UNDESIRED Context: 'schedule' and 'dictate' co-occur", 'score': 5}),
    ]

    # Threshold to consider flexibility as "critically undesirable"
    # This value might need adjustment based on testing and your definition of sensitivity
    UNWANTED_SCORE_THRESHOLD = 5 # Example: a single critical item with score 5 is sufficient

    all_reasons_and_scores_found = []
    total_unwanted_score = 0

    # --- Pattern Matching (Lexicon-based) ---
    for pattern_regex, lexicon_entry in patterns_lexicon:
        if re.search(pattern_regex, preprocessed_body):
            reason_text = lexicon_entry['reason']
            score = lexicon_entry['score']
            # Add only if the specific reason has not been found yet (to avoid duplicating scores from the same pattern)
            if not any(item['reason'] == reason_text for item in all_reasons_and_scores_found):
                all_reasons_and_scores_found.append({'reason': reason_text, 'score': score, 'type': 'pattern'})
                total_unwanted_score += score

    # --- Contextual Analysis (Lexicon-based) ---
    for (word1, word2), lexicon_entry in contexts_lexicon:
        # Using \b for word boundaries to avoid partial matches
        pattern_w1 = r'\b' + re.escape(word1) + r'\b'
        pattern_w2 = r'\b' + re.escape(word2) + r'\b'
        if re.search(pattern_w1, preprocessed_body) and \
                re.search(pattern_w2, preprocessed_body):
            reason_text = lexicon_entry['reason']
            score = lexicon_entry['score']
            # Add only if the specific reason has not been found yet
            if not any(item['reason'] == reason_text for item in all_reasons_and_scores_found):
                all_reasons_and_scores_found.append({'reason': reason_text, 'score': score, 'type': 'context'})
                total_unwanted_score += score

    # Determine if it's "critically undesirable" based on the total score
    is_critically_unwanted = total_unwanted_score >= UNWANTED_SCORE_THRESHOLD

    # Sort found reasons by score (highest first) for easier reading
    all_reasons_and_scores_found.sort(key=lambda x: x['score'], reverse=True)

    return is_critically_unwanted, total_unwanted_score, all_reasons_and_scores_found


def classify_job_description(body_text):
    # The analyze_undesired_flexibility function now returns 3 values
    has_undesired_flexibility, score, reasons_details = analyze_undesired_flexibility(body_text)

    # Format the reasons string to include the score
    reasons_str_list = [f"{item['reason']} (Score: {item['score']}, Type: {item['type']})" for item in reasons_details]

    if has_undesired_flexibility:
        return "Yes", score, "; ".join(reasons_str_list) if reasons_str_list else "Critical indicators found, but no specific details."
    else:
        if reasons_str_list:
            return "No", score, "; ".join(reasons_str_list)
        else:
            return "No", score, "No indicators of flexibility (desired or undesired) found."


def process_batch_of_jobs(batch_of_bodies):
    # Ensure 'body' is a string
    return [classify_job_description(str(body) if body is not None else "") for body in batch_of_bodies]


def calculate_dispersion_of_results(row, num_loops_executed):
    # Adapts to the new results structure if needed, or remove if no longer the focus
    # This function might need review depending on what 'undesired_flexibility_lex_{i}' now stores.
    # For this example, let's assume it still stores "Yes"/"No".
    results_list = [row[f'undesired_flexibility_lex_{i}'] for i in range(1, num_loops_executed + 1) if f'undesired_flexibility_lex_{i}' in row]
    if not results_list: return "N/A" # If no result columns are found
    return "Yes" if len(set(results_list)) > 1 else "No"


def main_analysis_pipeline(num_loops=1):
    main_df = read_input_files()
    if main_df is None or main_df.empty:
        logging.info("DataFrame is empty or not loaded. Exiting.")
        return

    body_column_name = 'body'
    if body_column_name not in main_df.columns:
        logging.error(f"Column '{body_column_name}' not found in DataFrame after initial processing. Check read_input_files.")
        return

    batch_processing_size = 100

    for i in range(1, num_loops + 1):
        logging.info(f"Starting loop {i}...")
        # Ensure we are passing only the 'body' column and handling NaNs
        body_text_series = main_df[body_column_name].fillna('')
        batches_to_process = [body_text_series[j:j + batch_processing_size].tolist() for j in range(0, len(main_df), batch_processing_size)]

        # Parallel processing
        # Use multiprocessing.cpu_count() - 1 if experiencing resource issues, or a fixed number
        num_available_processes = max(1, multiprocessing.cpu_count() -1) if multiprocessing.cpu_count() > 1 else 1

        flattened_batch_results = []
        # Use tqdm with imap for a progress bar
        with multiprocessing.Pool(processes=num_available_processes) as processing_pool:
            # pool.imap returns an iterator, convert to list to use tqdm
            # and process results as they arrive (if processing is lengthy)
            # or wait for all with list(tqdm(...))
            results_iterator = processing_pool.imap(process_batch_of_jobs, batches_to_process)
            for result_batch_item in tqdm(results_iterator, total=len(batches_to_process), desc=f"Loop {i} - Processing Batches"):
                flattened_batch_results.extend(result_batch_item)

        # Add results to the DataFrame
        # Now we have 3 result columns from classify_job_description
        main_df[f'undesired_flexibility_lex_{i}'], main_df[f'unwanted_score_lex_{i}'], main_df[f'reason_lex_{i}'] = zip(*flattened_batch_results)
        logging.info(f"Loop {i} completed.")

    # Calculate dispersion (if applicable and if reference columns are correct)
    # logging.info("Calculating dispersion...")
    # main_df['dispersion_lex'] = main_df.apply(calculate_dispersion_of_results, axis=1, num_loops_executed=num_loops) # Adjust reference column name

    # Save
    output_file_name = "Output_Lexicon_Based_Analysis_EN.xlsx"
    output_file_path = os.path.join("../output", output_file_name) # Assumes output folder exists
    os.makedirs("../output", exist_ok=True) # Create output folder if it doesn't exist

    try:
        main_df.to_excel(output_file_path, index=False, engine='openpyxl')
        logging.info(f"File saved successfully to: {output_file_path}")
    except Exception as e:
        logging.error(f"Error saving Excel file: {e}")


if __name__ == '__main__':
    # test_data = {'body': ["Must be available 24/7.", "We offer flexible hours.", "Work on demand as needed by the business."]}
    # df_sample_test = pd.DataFrame(test_data)
    # # (You would need to adapt the main_analysis_pipeline function or call analyze_undesired_flexibility directly to test this way)
    main_analysis_pipeline(num_loops=1) # Runs the analysis once by default
