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
    Analisa a descrição de uma vaga para classificar a flexibilidade de horário,
    agora com uma camada de validação em Python para garantir a citação em respostas 'YES'.
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

                # --- VALORES BRUTOS VINDOS DO MODELO ---
                undesired_flex_model = 1 if response_json_final.get("undesired_flexibility") == "YES" else 0
                undesired_reason_model = response_json_final.get("undesired_reason", "")
                desired_flex_model = 1 if response_json_final.get("desired_flexibility") == "YES" else 0
                desired_reason_model = response_json_final.get("desired_reason", "")

                # *** NOVA LÓGICA DE VALIDAÇÃO (TRAVA DE SEGURANÇA) ***
                # Para Undesired
                if undesired_flex_model == 1 and (undesired_reason_model.strip() == "" or undesired_reason_model.strip() == "N/A"):
                    logging.warning(f"Inconsistência encontrada para 'Undesired': Modelo retornou YES sem citação. Anulando para NO.")
                    undesired_flex_final = 0
                    undesired_reason_final = ""
                else:
                    undesired_flex_final = undesired_flex_model
                    undesired_reason_final = undesired_reason_model

                # Para Desired
                if desired_flex_model == 1 and (desired_reason_model.strip() == "" or desired_reason_model.strip() == "N/A"):
                    logging.warning(f"Inconsistência encontrada para 'Desired': Modelo retornou YES sem citação. Anulando para NO.")
                    desired_flex_final = 0
                    desired_reason_final = ""
                else:
                    desired_flex_final = desired_flex_model
                    desired_reason_final = desired_reason_model

                logging.info("Resposta do Ollama recebida e validada com sucesso.")
                return undesired_flex_final, undesired_reason_final, desired_flex_final, desired_reason_final

        except Exception as e:
            logging.error(f"Tentativa {attempt + 1} falhou com erro inesperado: {e}")
            if attempt + 1 == max_retries:
                return 0, "", 0, ""

        time.sleep(5)

    return 0, "Todas as tentativas de processamento falharam", 0, "Todas as tentativas de processamento falharam"


def read_input_files(diretorio="../input"):
    """
    Lê arquivos .csv e .xlsx do diretório de entrada, mantendo e renomeando
    as colunas de interesse para 'Title' e 'Body'.
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

            # Mantido como na versão anterior para ler o arquivo de entrada corretamente
            body_column = next((col for col in temp_df.columns if col.lower() == "body"), None)
            title_column = next((col for col in temp_df.columns if col.lower() == "title"), None)

            if body_column is None or title_column is None:
                logging.warning(f"Arquivo {filename} não possui as colunas 'Body' e/ou 'Title'. Ignorando.")
                continue

            temp_df = temp_df[[title_column, body_column]]
            temp_df.rename(columns={title_column: 'Title', body_column: 'Body'}, inplace=True)
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


def save_with_coloring(df, filepath):
    """Salva o DataFrame em um arquivo Excel, colorindo as linhas."""
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
                logging.error(f"Coluna não encontrada no DataFrame final: {e}. Não será possível colorir.")
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

        logging.info(f"Arquivo salvo com sucesso e com cores em: {filepath}")

    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo Excel com formatação: {e}")


def main():
    """Função principal para ler, processar e salvar os dados."""
    df = read_input_files()
    if df is None:
        logging.error("Nenhum arquivo de entrada válido encontrado. Encerrando.")
        return

    new_columns = [
        "Undesired_flexibility_dummy", "quote_body_undesired",
        "Desired_flexibility_dummy", "quote_body_desired"
    ]
    for col in new_columns:
        df[col] = ""

    for index, row in tqdm(df.iterrows(), total=len(df), desc="Analisando vagas"):
        description = row["Body"]
        if pd.isna(description) or not isinstance(description, str) or description.strip() == "":
            logging.warning(f"Descrição vazia ou inválida na linha {index}. Pulando.")
            results = (0, "Descrição vazia", 0, "Descrição vazia")
        else:
            results = evaluate_hour_flexibility_local(description)

        df.loc[index, new_columns] = results

        # Limpa o 'N/A' que pode vir do modelo para deixar a célula vazia no Excel
        if df.loc[index, "quote_body_undesired"] == "N/A":
            df.loc[index, "quote_body_undesired"] = ""
        if df.loc[index, "quote_body_desired"] == "N/A":
            df.loc[index, "quote_body_desired"] = ""


    final_columns_order = [
        "Title", "Body",
        "Undesired_flexibility_dummy", "quote_body_undesired",
        "Desired_flexibility_dummy", "quote_body_desired"
    ]
    df_final = df[final_columns_order]

    output_filepath = os.path.join("../output/results", "Job_postings_processed.xlsx")
    save_with_coloring(df_final, output_filepath)

    logging.info("Processamento concluído.")


if __name__ == "__main__":
    main()