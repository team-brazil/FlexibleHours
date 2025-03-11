import re
import os
import nltk
import pandas as pd
import multiprocessing
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import logging

# Configurar o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Download NLTK data (apenas se necessário)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')
try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet')
try:
    nltk.data.find('sentiment/vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')


def read_input_files(diretorio="../input"):
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
                logging.warning(f"File {filename}  is not .csv or .xlsx. Ignoring.")
                continue

            # Verificar se a coluna 'BODY' existe
            if 'body' not in temp_df.columns.str.lower().tolist():
                logging.warning(f"File {filename} does not have the 'BODY' column. Ignoring.")
                continue
            df_list.append(temp_df)

        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")

    if not df_list:
        logging.warning("No valid files found or processed. Check files in the 'input' folder'.")
        return None

    return pd.concat(df_list, ignore_index=True)


def preprocess_text(text):
    if not isinstance(text, str):
        return ""
    # Simple tokenization
    text = text.lower()
    tokens = re.findall(r'\b\w+\b', text.lower())
    stop_words = set(stopwords.words('english'))
    tokens = [token for token in tokens if token not in stop_words]

    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(token) for token in tokens]

    return ' '.join(tokens)


def analyze_undesired_flexibility(body):
    preprocessed_body = preprocess_text(body)
    patterns = [
        (r'\bflexib\w+\s+(hour|schedule|time|shift)', "Flexible hours/schedule"),
        (r'\b(full-time|part-time).*and.*(full-time|part-time)', "Combination of full-time and part-time"),
        (r'(night|weekend|holiday|evening).*\b(shift|work|hour)', "Non-standard work hours"),
        (r'irregular.*\b(hour|schedule)', "Irregular hours"),
        (r'early morning|late night', "Extreme hours"),
        (r'(availab\w+|work).*24/7', "24/7 availability"),
        (r'on[- ]call', "On-call work"),
        (r'rotating.*shift', "Rotating shifts"),
        (r'split.*shift', "Split shifts"),
        (r'(change|vary).*without.*notice', "Changes without notice"),
        (r'based.*business.*need', "Hours based on business needs"),
        (r'float\w*\s+holiday', "Floating holidays"),
        (r'work.*holiday', "Holiday work"),
        (r'(schedule|hour|shift).*vary', "Variable hours"),
        (r'overtime.*\b(required|needed|necessary)', "Mandatory overtime"),
        (r'extended.*hour', "Extended hours"),
        (r'(availab\w+|work).*\b(weekend|holiday)', "Weekend or holiday availability"),
        (r'shift.*\b(assign\w+|schedule)', "Variable shift assignment"),
        (r'\b(work|schedule|hour).*demand', "Work on demand"),
        (r'\b(as|when).*needed', "As needed"),
        (r'\b(subject\s+to|depend.*on|accord.*to).*availability', "Subject to availability"),
        (r'unpredict\w+\s+(hour|schedule)', "Unpredictable hours"),
        (r'total\s+availability', "Total availability"),
        (r'complete\s+flexibility', "Complete flexibility"),
        (r'no\s+choice\s+of\s+schedule', "No choice of schedule"),
        (r'non[- ]negotiable\s+(hour|schedule)', "Non-negotiable hours"),
        (r'\b(must|require).*flexible', "Must be flexible"),
        (r'(hour|schedule|shift).*vary.*without.*notice', "Hours vary without notice"),
        (r'flexib.*\b(meet|suit|accommodate|for)\b.*(company|business|employer|need|demand)', "Flexibility to meet company needs"),
        (r'rotat\w+\s+(shift|schedule|roster)', "Rotating shifts/schedule"),
        (r'on[- ]call.*(emergenc|urgent)', "On-call for emergencies"),
        (r'(part[- ]time|casual).*(flexib|as\s+needed|when\s+needed)', "Part-time/casual with flexibility"),
        (r'remote.*(irregular|unpredict\w+|vary).*hour', "Remote work with irregular hours"),
        (r'travel.*(require|necess).*flexib', "Travel requiring flexibility"),
    ]

    contexts = [
        ('flexib', 'schedule', "Flexible schedule"),
        ('work', 'weekend', "Weekend work"),
        ('availab', 'holiday', "Holiday availability"),
        ('shift', 'assign', "Variable shift assignment"),
        ('hour', 'vary', "Variable hours"),
        ('business', 'need', "Based on business needs"),
        ('overtime', 'require', "Required overtime"),
        ('remote', 'total availability', "Remote work with total availability"),
        ('on-call', 'emergency', "On-call for emergencies"),
        ('casual', 'flexible', "Casual work with flexibility"),
        ('shift', 'rotate', "Rotating shifts"),
        ('hour', 'unpredict', "Unpredictable hours"),
        ('schedule', 'vary', "Variable schedule"),
        ('flexib', 'company', "Flexibility for the company"),
        ('flexib', 'business', "Flexibility for the business"),
    ]

    reasons = []

    # --- Pattern matching with synonyms ---
    for pattern, reason in patterns:
        for word in re.findall(r'\b\w+\b', pattern):
            for syn in wordnet.synsets(word):
                for lemma in syn.lemmas():
                    new_pattern = pattern.replace(word, lemma.name())
                    # --- Escape the replacement ---
                    new_pattern = re.escape(new_pattern)  # Escape special characters
                    if re.search(new_pattern, preprocessed_body):
                        reasons.append(reason)
                        break  # Break inner loop if a synonym matches
                else:
                    continue  # Continue if no synonyms match
                break  # Break outer loop if a synonym matches
        else:
            if re.search(pattern, preprocessed_body):
                reasons.append(reason)

    # --- Contextual analysis with synonyms ---
    for word1, word2, reason in contexts:
        if word1 in preprocessed_body and word2 in preprocessed_body:
            if reason not in reasons:
                reasons.append(reason)

    return bool(reasons), reasons


def classify_job(body):
    has_undesired_flexibility, reasons = analyze_undesired_flexibility(body)
    if has_undesired_flexibility:
        return "Yes", "; ".join(reasons)
    else:
        return "No", "No clear indicators of undesired flexibility found"


def process_batch(batch):
    return [classify_job(body) for body in batch]


def calculate_dispersion(row, num_loops):
    results = [row[f'undesired_flexibility_{i}'] for i in range(1, num_loops + 1)]
    if len(set(results)) > 1:
        return "Yes"
    else:
        return "No"


def main(num_loops=1):
    df = read_input_files()
    if df is None:
        return

    # Find 'BODY' column with case-insensitive
    body_column = next((col for col in df.columns if col.lower() == 'body'), None)

    # Prepare data for batch processing
    batch_size = 100

    for i in range(1, num_loops + 1):
        logging.info(f"Starting loop {i}...")
        batches = [df[body_column][j:j + batch_size].tolist() for j in range(0, len(df), batch_size)]

        # Parallel processing
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            results = list(tqdm(pool.imap(process_batch, batches), total=len(batches)))

        # Flatten the results
        flattened_results = [item for sublist in results for item in sublist]

        # Add results to the DataFrame
        df[f'undesired_flexibility_{i}'], df[f'reason_{i}'] = zip(*flattened_results)

    # calculate dispertio
    logging.info("Calculating dispersion...")
    df['dispersion'] = df.apply(calculate_dispersion, axis=1, num_loops=num_loops)

    # Save
    output_filepath = os.path.join("../output", "Test_NTLK_new.xlsx")
    try:
        df.to_excel(output_filepath, index=False, engine='openpyxl')
        logging.info(f"Arquivo salvo com sucesso em: {output_filepath}")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo Excel: {e}")


if __name__ == '__main__':
    main()