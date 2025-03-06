import csv
import time
import requests
import concurrent.futures
from ratelimit import limits, sleep_and_retry
from dotenv import load_dotenv
import os
import pandas as pd
import logging
import threading
import json
import re
import hashlib
import random

# Configurar o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Recupera as chaves de API do arquivo .env
API_KEYS = os.getenv("API_KEYS")
if API_KEYS:
    API_KEYS = API_KEYS.split(',')
else:
    raise ValueError("API_KEYS não encontradas no arquivo .env")

# Configurações
MAX_RETRIES = 3  # Reduzir o número de retentativas para acelerar o processo
CALLS_PER_MINUTE = 9
ONE_MINUTE = 60
WAIT_TIME_BETWEEN_RETRIES = 2  # Reduzir o tempo de espera entre as tentativas

# Novo modelo e endpoint da API
MODEL_NAME = "gemini-1.5-flash"
API_ENDPOINT = f"https://generativelanguage.googleapis.com/v1/models/{MODEL_NAME}:generateContent"

# Lock para garantir que cada chave de API respeite o limite de requisições
api_key_locks = {api_key: threading.Lock() for api_key in API_KEYS}

class APIKeyManager:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.key_usage = {key: 0 for key in api_keys}

    def get_next_key(self):
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return self.api_keys[self.current_key_index]

@sleep_and_retry
@limits(calls=CALLS_PER_MINUTE, period=ONE_MINUTE)
def make_api_request(description, api_key):
    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key
    }
    prompt = (
        "Você é um especialista em análise de vagas de emprego. "
        "Seu objetivo é identificar se uma vaga de emprego apresenta 'flexibilidade de horas indesejada' (Undesired Flexibility). "
        "Este é um conceito importante: 'flexibilidade de horas indesejada' acontece quando uma vaga de emprego diz "
        "que há flexibilidade, mas essa flexibilidade é apenas para a empresa, e não para o trabalhador. "
        "Por exemplo, a vaga pode exigir que o funcionário trabalhe em horários irregulares, finais de semana, "
        "feriados, ou em turnos rotativos, sem oferecer a opção de escolher esses horários. Isso é diferente de uma "
        "flexibilidade real, onde o empregado pode escolher seus horários ou tem controle sobre sua escala. "
        "Analise o texto da proposta de emprego abaixo e determine se ele é ou não um caso de 'flexibilidade de horas indesejada'. "
        f"Texto da proposta de emprego: {description}. "
        "Responda da seguinte forma: 'undesired_flexibility': (Yes ou No) e 'reason': (sua explicação). "
        "Responda usando um único JSON sem nenhuma outra palavra. "
    )
    data = {
        "contents": [{
            "parts": [{
                "text": prompt
            }]
        }]
    }

    response = requests.post(API_ENDPOINT, json=data, headers=headers)
    response.raise_for_status()
    return response.json()

def extract_json_from_response(response_text):
    try:
        json_match = re.search(r"```json\n(.*?)\n```", response_text, re.DOTALL)
        if json_match:
            json_data = json.loads(json_match.group(1))
        else:
            json_data = json.loads(response_text)
        if 'undesired_flexibility' not in json_data or 'reason' not in json_data:
            raise ValueError("JSON não contém os campos esperados")
        if json_data['undesired_flexibility'] not in ['Yes', 'No']:
            raise ValueError("Valor inválido para undesired_flexibility")
        return json_data
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"Erro ao decodificar/validar JSON: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado ao extrair JSON: {e}")
        return None

def exponential_backoff(attempt, max_delay=10):  # Reduzir o tempo máximo de espera
    delay = min(2 ** attempt + random.uniform(0, 1), max_delay)
    time.sleep(delay)

def process_description(description, api_key_manager):
    for attempt in range(MAX_RETRIES):
        api_key = api_key_manager.get_next_key()  # Troca a chave antes de cada tentativa
        try:
            with api_key_locks[api_key]:
                response = make_api_request(description, api_key)
                response_text = response['candidates'][0]['content']['parts'][0]['text'].strip()

                json_data = extract_json_from_response(response_text)

                if json_data:
                    return json_data['undesired_flexibility'], json_data['reason']

            logging.warning(f"Resposta inválida na tentativa {attempt + 1}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de rede na tentativa {attempt + 1}: {e}")
        except Exception as e:
            logging.error(f"Erro inesperado na tentativa {attempt + 1}: {e}")

        exponential_backoff(attempt)

    logging.error("Máximo de retentativas atingido")
    return "Erro", "Máximo de retentativas atingido"

def process_dataframe(df, api_key_manager, description_column='body'):
    results = []
    reasons = []
    for i, row in df.iterrows():
        try:
            description = row[description_column]
            logging.info(f"Processando linha {i+1}")
            undesired_flexibility, reason = process_description(description, api_key_manager)
            results.append(undesired_flexibility)
            reasons.append(reason)
            logging.info(f"Linha {i+1} processada com sucesso")
        except Exception as e:
            logging.error(f"Erro ao processar linha {i+1}: {e}")
            results.append("Erro")
            reasons.append(f"Erro ao processar: {e}")

        time.sleep(0.1)  # Reduzir o tempo de espera entre as linhas
    return results, reasons

def ler_arquivos_input(diretorio="../input", description_column='body'):
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

            body_column = next((col for col in temp_df.columns if col.lower() == description_column.lower()), None)
            if not body_column:
                logging.warning(f"Arquivo {filename} não possui a coluna '{description_column}'. Ignorando.")
                continue

            df_list.append(temp_df)

        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {filename}: {e}")

    if not df_list:
        logging.warning("Nenhum arquivo válido encontrado ou processado. Verifique os arquivos na pasta 'input'.")
        return None

    return pd.concat(df_list, ignore_index=True)

def main(description_column='BODY'):
    # Instancia o APIKeyManager
    api_key_manager = APIKeyManager(API_KEYS)

    # 1. Leitura dos arquivos de entrada
    df = ler_arquivos_input(description_column=description_column)
    if df is None:
        return

    # 2. Processamento do DataFrame
    results, reasons = process_dataframe(df, api_key_manager, description_column=description_column)

    # 3. Adicionar os resultados ao DataFrame
    if results and reasons:
        df['Undesired Flexibility'] = results
        df['Reason'] = reasons
    else:
        logging.error("Não foi possível processar o DataFrame devido a um erro na coluna de descrição.")
        return

    # 4. Salvar o DataFrame em um arquivo Excel
    output_filepath = os.path.join("../output", "Test_Gemini.xlsx")
    os.makedirs("output", exist_ok=True)
    try:
        df.to_excel(output_filepath, index=False, engine='openpyxl')
        logging.info(f"Arquivo salvo com sucesso em: {output_filepath}")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo Excel: {e}")

if __name__ == "__main__":
    main()