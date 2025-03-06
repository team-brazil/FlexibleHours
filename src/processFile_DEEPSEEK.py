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
from google.api_core import exceptions as google_exceptions  # Importação correta

# Configurar o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def ler_arquivos_input(diretorio="../input"):
    """
    Lê arquivos CSV e XLSX do diretório 'input' e retorna um DataFrame.
    Assume que os arquivos têm uma coluna chamada 'BODY'.
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

            # Verifica se a coluna 'BODY' existe no DataFrame
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


def avaliar_flexibilidade_gemini(descricao, api_key, max_retries=3):
    """Avalia a flexibilidade de uma descrição de vaga usando o modelo Gemini."""
    genai.configure(api_key=api_key, transport="rest")
    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = (
        "Você é um especialista em análise de vagas de emprego. "
        "Seu objetivo é identificar se uma vaga de emprego apresenta 'flexibilidade de horas indesejada' (Undesired Flexibility). "
        "Este é um conceito importante: 'flexibilidade de horas indesejada' acontece quando uma vaga de emprego diz "
        "que há flexibilidade, mas essa flexibilidade é apenas para a empresa, e não para o trabalhador. "
        "Por exemplo, a vaga pode exigir que o funcionário trabalhe em horários irregulares, finais de semana, "
        "feriados, ou em turnos rotativos, sem oferecer a opção de escolher esses horários. Isso é diferente de uma "
        "flexibilidade real, onde o empregado pode escolher seus horários ou tem controle sobre sua escala. "
        "Analise o texto da proposta de emprego abaixo e determine se ele é ou não um caso de 'flexibilidade de horas indesejada'. "
        f"Texto da proposta de emprego: {descricao}. "
        "Responda da seguinte forma: 'undesired_flexibility': (Yes ou No) e 'reason': (sua explicação). "
        "Responda usando um único JSON sem nenhuma outra palavra. "
    )
    num_retries = 0
    while num_retries < max_retries:
        try:
            logging.info(f"Enviando solicitação para API Gemini com a descrição: {descricao[:100]}...")
            response = model.generate_content(contents=prompt)

            # Extrai o conteúdo JSON da resposta usando regex
            json_match = re.search(r"```json\n(.*?)\n```", response.text, re.DOTALL)
            if json_match:
                json_text = json_match.group(1)
                resposta_json = json.loads(json_text)
            else:
                logging.warning(f"JSON não encontrado na resposta: {response.text[:100]}...")
                resposta_json = {"undesired_flexibility": "Erro", "reason": "JSON não encontrado na resposta"}

            classificacao = resposta_json.get('undesired_flexibility', 'Não')
            justificativa = resposta_json.get('reason', 'Sem justificativa')
            return classificacao, justificativa

        except Exception as e:
            if "429" in str(e):
                logging.error(f"Erro na requisição à API Gemini: {e}")
                time.sleep(60)
                num_retries += 1
            else:
                logging.error(f"Erro na requisição à API Gemini: {e}")
                return "Erro", f"Erro ao acessar a API: {e}"
    logging.error(f"Maximo de retries ({max_retries}) atingido.")
    return "Erro", "Maximo de retries atingido"


def process_batch(batch, api_keys, key_index):
    """Processa um lote de descrições de vagas, usando uma chave de API específica."""
    results = []
    for i, descricao in enumerate(batch):
        api_key = api_keys[key_index % len(api_keys)]
        classificacao, justificativa = avaliar_flexibilidade_gemini(descricao, api_key)
        results.append((classificacao, justificativa))
        time.sleep(5)  # Atraso de 5 segundos entre as requisições
    return results


def calculate_dispersion(row, num_loops):
    """
    Calcula a dispersão para uma linha, comparando os resultados de 'undesired_flexibility' em diferentes loops.
    """
    results = [row[f'undesired_flexibility_{i}'] for i in range(1, num_loops + 1)]
    if len(set(results)) > 1:
        return "Yes"  # Há dispersão
    else:
        return "No"  # Não há dispersão


def main(num_loops=10, batch_size=1, num_processes=8):
    # Carrega as variáveis de ambiente do arquivo .env
    load_dotenv()

    # Carrega as configurações do arquivo config.json
    with open("config.json", "r") as f:
        config = json.load(f)

    api_keys = os.getenv("API_KEYS")
    if api_keys:
        api_keys = api_keys.split(',')
    else:
        api_keys = config.get("api_keys", [])

    if not api_keys:
        raise ValueError("API_KEYS não encontrados. Verifique o arquivo .env ou config.json.")

    # Determinar o número de processos
    if num_processes > len(api_keys):
        logging.warning(
            f"O número de processos ({num_processes}) é maior que o número de API keys ({len(api_keys)}). Reduzindo o número de processos para {len(api_keys)}.")
        num_processes = len(api_keys)

    # 2. Leitura dos arquivos de entrada
    df = ler_arquivos_input()
    if df is None:
        return

    # Encontrar a coluna 'BODY' de forma case-insensitive
    body_column = next((col for col in df.columns if col.lower() == 'body'), None)

    for i in range(1, num_loops + 1):
        logging.info(f"Starting loop {i}...")
        df[f'undesired_flexibility_{i}'] = ""
        df[f'reason_{i}'] = ""

        batches = [df[body_column][j:j + batch_size].tolist() for j in range(0, len(df), batch_size)]

        # Prepare os argumentos para starmap, distribuindo as chaves
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
        logging.info(f"Arquivo salvo com sucesso em: {output_filepath}")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo Excel: {e}")

    print("Processamento concluído. Resultados salvos em output/Test_Gemini.xlsx")


if __name__ == "__main__":
    main()