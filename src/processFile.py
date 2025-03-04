import re
import os
import nltk
import pandas as pd
import multiprocessing
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
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


def ler_arquivos_input(diretorio="../input"):
    """
    Lê os arquivos .csv ou .xlsx do diretório especificado.

    Args:
        diretorio (str): O diretório onde os arquivos estão localizados.

    Returns:
        pandas.DataFrame: Um DataFrame contendo os dados lidos ou None se nenhum arquivo foi processado.
    """
    df_list = []
    for filename in os.listdir(diretorio):
        if filename.startswith("~$"):  # Ignora arquivos temporários do Excel
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

            # Verificar se a coluna 'BODY' existe
            if 'body' not in temp_df.columns.str.lower().tolist():
                logging.warning(f"Arquivo {filename} não possui a coluna 'BODY'. Ignorando.")
                continue
            df_list.append(temp_df)

        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {filename}: {e}")

    if not df_list:
        logging.warning("Nenhum arquivo válido encontrado ou processado. Verifique os arquivos na pasta 'input'.")
        return None

    return pd.concat(df_list, ignore_index=True)


def preprocess_text(text):
    if not isinstance(text, str):
        return ""
    # Simple tokenization
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
    ]

    reasons = []
    for pattern, reason in patterns:
        if re.search(pattern, preprocessed_body):
            reasons.append(reason)

    contexts = [
        ('flexib', 'schedule', "Flexible schedule"),
        ('work', 'weekend', "Weekend work"),
        ('availab', 'holiday', "Holiday availability"),
        ('shift', 'assign', "Variable shift assignment"),
        ('hour', 'vary', "Variable hours"),
        ('business', 'need', "Based on business needs"),
        ('overtime', 'require', "Required overtime"),
    ]

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


def main():
    # Ler os arquivos
    df = ler_arquivos_input()
    if df is None:
        return

    # Encontrar a coluna 'BODY' de forma case-insensitive
    body_column = next((col for col in df.columns if col.lower() == 'body'), None)

    # Prepare data for batch processing
    batch_size = 100  # Adjust as needed
    batches = [df[body_column][i:i + batch_size].tolist() for i in range(0, len(df), batch_size)]

    # Parallel processing
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = list(tqdm(pool.imap(process_batch, batches), total=len(batches)))

    # Flatten the results
    flattened_results = [item for sublist in results for item in sublist]

    # Add results to the DataFrame
    df['undesired_flexibility'], df['reason'] = zip(*flattened_results)

    # Salvar em Excel
    output_filepath = os.path.join("../output", "Test_NTLK.xlsx")
    try:
        df.to_excel(output_filepath, index=False, engine='openpyxl')
        logging.info(f"Arquivo salvo com sucesso em: {output_filepath}")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo Excel: {e}")


if __name__ == '__main__':
    main()