import httpx
import logging
import pandas as pd
import time
import os
import json
from tqdm import tqdm


# Configurar o logging (detalhado)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def avaliar_flexibilidade_deepseek(descricao, ollama_url="http://localhost:11434/api/generate"):
    """
    Avalia a flexibilidade usando o endpoint do Ollama.
    """
    prompt = (
        "Você é um especialista em análise de vagas de emprego. "
        "Seu objetivo é identificar se uma vaga de emprego apresenta 'flexibilidade de horas indesejada' (Undesired Flexibility). "
        "Este é um conceito importante: 'flexibilidade de horas indesejada' acontece quando uma vaga de emprego diz "
        "que há flexibilidade, mas essa flexibilidade é apenas para a empresa, e não para o trabalhador. "
        "Por exemplo, a vaga pode exigir que o funcionário trabalhe em horários irregulares, finais de semana, "
        "feriados, ou em turnos rotativos, sem oferecer a opção de escolher esses horários. Isso é diferente de uma "
        "flexibilidade real, onde o empregado pode escolher seus horários ou tem controle sobre sua escala. "
        f"Texto da proposta de emprego: {descricao}. "
        "Responda da seguinte forma: 'undesired_flexibility': (Yes ou No) e 'reason': (sua explicação). "
        "Responda usando um único JSON sem nenhuma outra palavra. "
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
            logging.info(f"Enviando requisição para Ollama...")
            response = client.post(
                ollama_url,
                json=data,
                timeout=120.0,
            )
            #Ver desempenho
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de geração: {elapsed_time:.3f} segundos")

            response.raise_for_status()
            response_json = response.json()
            resposta_texto = response_json["response"]

            resposta_json_final = json.loads(resposta_texto)
            classificacao = resposta_json_final.get("undesired_flexibility", "Não")
            justificativa = resposta_json_final.get("reason", "Sem justificativa")
            logging.info("Resposta recebida do Ollama com sucesso.")
            return classificacao, justificativa

    except httpx.RequestError as e:
        logging.error(f"Erro de requisição ao Ollama: {e}")
        return "Erro", f"Erro de requisição: {e}"
    except httpx.HTTPStatusError as e:
        logging.error(f"Erro HTTP do Ollama: {e} - Resposta: {response.text}")
        return "Erro", f"Erro HTTP: {e}"
    except json.JSONDecodeError as e:
        logging.error(f"Resposta do Ollama não é um JSON válido: {e}")
        return "Erro", "Resposta inválida do Ollama (não JSON)"
    except Exception as e:
        logging.error(f"Erro inesperado ao chamar o Ollama: {e}")
        return "Erro", str(e)



def ler_arquivos_input(diretorio="../input"):
    """Lê arquivos CSV e XLSX do diretório 'input' e retorna um DataFrame."""
    df_list = []
    arquivos_lidos = False

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
                logging.warning(f"Arquivo {filename} não é .csv ou .xlsx. Ignorando.")
                continue

            body_column = next(
                (col for col in temp_df.columns if col.lower() == "body"), None
            )
            if body_column is None:
                logging.warning(f"Arquivo {filename} não possui a coluna 'BODY'. Ignorando.")
                continue

            df_list.append(temp_df)
            arquivos_lidos = True

        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {filename}: {e}")

    if not arquivos_lidos:
        logging.warning(
            "Nenhum arquivo válido encontrado ou processado. Verifique os arquivos na pasta 'input'."
        )
        return None

    if not df_list:
        return None

    return pd.concat(df_list, ignore_index=True)


def calculate_dispersion(row, num_loops):
    """Calcula a dispersão."""
    results = [row[f"undesired_flexibility_{i}"] for i in range(1, num_loops + 1)]
    return "Yes" if len(set(results)) > 1 else "No"


def main(num_loops=10, batch_size=1):
    """Função principal."""

    # 1. Leitura dos arquivos de entrada
    df = ler_arquivos_input()
    if df is None:
        logging.error("Nenhum arquivo de entrada válido encontrado. Encerrando.")
        return

    body_column = next((col for col in df.columns if col.lower() == "body"), None)

    for i in range(1, num_loops + 1):
        logging.info(f"Starting loop {i}...")
        df[f"undesired_flexibility_{i}"] = ""
        df[f"reason_{i}"] = ""

        for index, row in tqdm(df.iterrows(), total=len(df), desc=f"Loop {i}"):
            descricao = row[body_column]
            classificacao, justificativa = avaliar_flexibilidade_deepseek(descricao)
            df.loc[index, f"undesired_flexibility_{i}"] = classificacao
            df.loc[index, f"reason_{i}"] = justificativa

    # 2. Calcular a dispersão
    logging.info("Calculating dispersion...")
    df["dispersion"] = df.apply(calculate_dispersion, axis=1, num_loops=num_loops)

    # 3. Salvar os resultados
    output_filepath = os.path.join("../output", "Test_DeepSeek.xlsx")
    try:
        df.to_excel(output_filepath, index=False, engine="openpyxl")
        logging.info(f"Arquivo salvo com sucesso em: {output_filepath}")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo Excel: {e}")

    logging.info("Processamento concluído.")

if __name__ == "__main__":
    main()