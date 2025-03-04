import pandas as pd
import requests
import json
import os
from openai import OpenAI
from dotenv import load_dotenv


def ler_arquivos_input(diretorio="input"):
    """
    Lê arquivos CSV e XLSX do diretório 'input' e retorna uma lista de descrições de vagas.
    Assume que os arquivos têm uma coluna chamada 'BODY'.
    """
    descricoes = []
    for filename in os.listdir(diretorio):
        if filename.startswith("~$"):  # Ignora arquivos temporários do Excel
            continue

        filepath = os.path.join(diretorio, filename)
        try:
            if filename.endswith(".csv"):
                df = pd.read_csv(filepath, encoding="utf-8")
            elif filename.endswith(".xlsx"):
                df = pd.read_excel(filepath, engine='openpyxl')
            else:
                continue

            # Verifica se a coluna 'BODY' existe no DataFrame
            if 'BODY' in df.columns:
                # Converte todos os valores da coluna 'BODY' para string e remove valores nulos
                df['BODY'] = df['BODY'].astype(str).replace('nan', '').replace('None', '')
                descricoes.extend(df['BODY'].tolist())  # Chama o método tolist()
            else:
                print(f"Aviso: Arquivo {filename} não possui a coluna 'BODY'. Ignorando.")
        except Exception as e:
            print(f"Erro ao ler o arquivo {filename}: {e}")

    return descricoes


def avaliar_flexibilidade_gemini(descricao, api_key, api_url, config):
    client = OpenAI(api_key=api_key, base_url=api_url)

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "You are a helpful assistant"},
                {"role": "user", "content": "Hello"},
            ],
            stream=False
        )
        resultado = response.json

        classificacao = resultado.get('classificacao', 'Não')
        justificativa = resultado.get('justificativa', 'Sem justificativa')

        return classificacao, justificativa

    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição à API DeepSeek: {e}")
        return "Erro", f"Erro ao acessar a API: {e}"
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar a resposta JSON: {e}")
        return "Erro", f"Erro ao decodificar a resposta da API: {e}"
    except Exception as e:
        print(f"Erro inesperado: {e}")
        return "Erro", f"Erro inesperado: {e}"


def salvar_resultados_csv(descricoes, classificacoes, justificativas, diretorio="output", nome_arquivo="resultados.csv"):
    """
    Salva os resultados em um arquivo CSV no diretório 'output'.
    """
    data = {
        'descricao': descricoes,
        'flexibilidade_indesejada': classificacoes,
        'justificativa': justificativas
    }
    df = pd.DataFrame(data)

    # Cria o diretório se não existir
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

    filepath = os.path.join(diretorio, nome_arquivo)
    df.to_csv(filepath, index=False, encoding="utf-8")


def main():
    # Carrega as variáveis de ambiente do arquivo .env
    load_dotenv()

    # 1. Carrega as configurações do arquivo config.json
    with open("../config.json", "r") as f:
        config = json.load(f)

    api_key = os.getenv("API_KEY") or config.get("api_key")
    api_url = os.getenv("API_URL") or config.get("api_url")

    # 2. Leitura dos arquivos de entrada
    descricoes = ler_arquivos_input()

    # 3. Preparação das listas para armazenar os resultados
    classificacoes = []
    justificativas = []

    # 4. Avaliação de cada descrição de vaga
    for descricao in descricoes:
        classificacao, justificativa = avaliar_flexibilidade_gemini(descricao, api_key, api_url, config)
        classificacoes.append(classificacao)
        justificativas.append(justificativa)

    # 5. Armazenamento dos resultados em um arquivo CSV
    salvar_resultados_csv(descricoes, classificacoes, justificativas)

    print("Processamento concluído. Resultados salvos em output/resultados.csv")


if __name__ == "__main__":
    main()