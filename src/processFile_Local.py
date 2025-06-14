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
    """
    Analisa a descrição de uma vaga de emprego para classificar a flexibilidade de horário,
    com lógica de retentativa e tratamento de erro aprimorado.
    """
    prompt = f"""
    You are a deterministic JSON-generating bot executing a strict classification task. Your output MUST be a single, valid JSON object. Follow the rules with no deviation.

    **Primary Rule: High-Confidence Classification**
    You will classify flexibility as 'YES' ONLY IF you find explicit, unambiguous evidence in the text. If there is any doubt, ambiguity, or the text only hints at flexibility, you MUST classify it as 'NO'. Your goal is 100% precision. It is better to miss a potential case (false negative) than to incorrectly classify a case (false positive).

    **Definitions & Keywords for Classification:**

    1.  **Undesired Flexibility (YES only if text contains):**
        * Mandatory/required work on weekends or holidays ("disponibilidade para trabalhar aos fins de semana", "trabalho em feriados").
        * Explicitly mentioned rotating shifts or unpredictable schedules ("horários rotativos", "escala 6x1", "disponibilidade de horário").
        * Company-dictated schedule changes ("horário pode sofrer alterações").
    
    2.  **Desirable Flexibility (YES only if text contains):**
        * Explicit mention of remote work, home office, or hybrid model ("trabalho remoto", "híbrido", "home office").
        * Explicit mention of flexible hours or flextime ("horário flexível", "banco de horas").
        * Employee has clear control over their schedule ("você monta seu horário").

    **Task:**
    Analyze the job description below. Adhere strictly to the **Primary Rule**. For 'reason' fields, quote the EXACT phrase that justifies your 'YES' decision.

    **Job Description:**
    {description}

    **JSON Output Structure (Strict Adherence Required):**
    ```json
    {{
      "undesired_flexibility": "YES" or "NO",
      "undesired_reason": "If 'YES', quote the exact evidence. If 'NO', state 'No explicit evidence of company-controlled flexibility.'",
      "undesired_difficulty_classification": "If classification was ambiguous, explain why and quote the exact evidence. Otherwise, leave blank",
      "desired_flexibility": "YES" or "NO",
      "desired_reason": "If 'YES', quote the exact evidence. If 'NO', state 'No explicit evidence of employee-controlled flexibility.'",
      "desired_difficulty_classification": "If classification was ambiguous, explain and quote the exact evidence. Otherwise, leave blank"
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
                logging.info(f"Enviando requisição para o Ollama (Tentativa {attempt + 1}/{max_retries})...")
                response = client.post(
                    ollama_url,
                    json=data,
                    timeout=60.0,
                )

                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"Tempo de geração: {elapsed_time:.3f} segundos")

                response.raise_for_status()

                text_response = response.json()["response"]
                response_json_final = json.loads(text_response)

                undesired_flexibility = response_json_final.get("undesired_flexibility", "Not Found")
                undesired_reason = response_json_final.get("undesired_reason", "without justification")
                undesired_difficulty_classification = response_json_final.get("undesired_difficulty_classification", "")
                desired_flexibility = response_json_final.get("desired_flexibility", "Not Found")
                desired_reason = response_json_final.get("desired_reason", "without justification")
                desired_difficulty_classification = response_json_final.get("desired_difficulty_classification", "")

                logging.info("Resposta do Ollama recebida com sucesso.")
                return undesired_flexibility, undesired_reason, undesired_difficulty_classification, desired_flexibility, desired_reason, desired_difficulty_classification

        # ---- TRATAMENTO DE ERRO APRIMORADO ----
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logging.error(f"Tentativa {attempt + 1} falhou com erro de rede/HTTP: {e}")
            if attempt + 1 == max_retries:
                return "Not Found", f"Falha de conexão com o modelo: {e}", "", "Not Found", f"Falha de conexão com o modelo: {e}", ""
        except json.JSONDecodeError as e:
            logging.error(f"Tentativa {attempt + 1} falhou com JSON inválido: {e}. Resposta problemática: {response.text[:200]}...")
            if attempt + 1 == max_retries:
                return "Not Found", "Modelo retornou JSON inválido", "", "Not Found", "Modelo retornou JSON inválido", ""
        except Exception as e:
            logging.error(f"Tentativa {attempt + 1} falhou com erro inesperado: {e}")
            if attempt + 1 == max_retries:
                return "Not Found", f"Erro inesperado no processamento: {e}", "", "Not Found", f"Erro inesperado no processamento: {e}", ""

        # Espera um pouco antes de tentar novamente
        time.sleep(5)

    # Se todas as tentativas falharem, retorna o valor padrão de falha
    return "Not Found", "Todas as tentativas de processamento falharam", "", "Not Found", "Todas as tentativas de processamento falharam", ""


def read_input_files(diretorio="../input"):
    """
    Lê arquivos .csv e .xlsx do diretório de entrada e os concatena em um único DataFrame.
    """
    df_list = []
    readed_files = False

    for filename in os.listdir(diretorio):
        if filename.startswith("~$") or filename == ".DS_Store":
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

            temp_df.rename(columns={body_column: 'body'}, inplace=True)
            df_list.append(temp_df)
            readed_files = True

        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {filename}: {e}")

    if not readed_files:
        logging.warning(
            "Nenhum arquivo válido encontrado ou processado. Verifique os arquivos na pasta 'input'."
        )
        return None

    return pd.concat(df_list, ignore_index=True) if df_list else None


def main():
    """Função principal para ler, processar e salvar os dados."""
    df = read_input_files()
    if df is None:
        logging.error("Nenhum arquivo de entrada válido encontrado. Encerrando.")
        return

    # Adiciona as colunas para os resultados da análise
    new_columns = [
        "undesired_flexibility", "undesired_reason", "undesired_difficulty_classification",
        "desired_flexibility", "desired_reason", "desired_difficulty_classification"
    ]
    for col in new_columns:
        df[col] = ""

    # Itera sobre o DataFrame e processa cada descrição
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Analisando vagas"):
        description = row["body"]
        if pd.isna(description) or not isinstance(description, str) or description.strip() == "":
            logging.warning(f"Descrição vazia ou inválida na linha {index}. Pulando.")
            results = ("Inválido", "Descrição vazia", "", "Inválido", "Descrição vazia", "")
        else:
            results = evaluate_hour_flexibility_local(description)

        # Atribui os resultados às colunas correspondentes
        df.loc[index, new_columns] = results

    # Salva o resultado em um novo arquivo Excel
    output_filepath = os.path.join("../output/results", "Test_Local_Processed.xlsx")
    try:
        df.to_excel(output_filepath, index=False, engine="openpyxl")
        logging.info(f"Arquivo salvo com sucesso em: {output_filepath}")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo Excel: {e}")

    logging.info("Processamento concluído.")


if __name__ == "__main__":
    main()