import os
import glob

# === CONFIGURAÇÃO ===
# Ajuste este caminho se necessário, baseando-me na estrutura que vimos antes
BATCH_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "output", "results", "batch_temp")

# Lista de meses para EXCLUIR (Formato YYYY-MM que aparece no nome do arquivo)
# Copiei exatamente da mensagem do seu colega
EXCLUDE_LIST = [
    # 2025
    "2025-02", "2025-01",
    # 2024
    "2024-12", "2024-11", "2024-10", "2024-09",
    # 2022
    "2022-01",
    # 2018
    "2018-08", "2018-03",
    # 2017
    "2017-12",
    # 2015 (Todos)
    "2015-12", "2015-11", "2015-10", "2015-09", "2015-08", "2015-07",
    "2015-06", "2015-05", "2015-04", "2015-03", "2015-02", "2015-01",
    # 2014
    "2014-12", "2014-11", "2014-10", "2014-09", "2014-07", "2014-06", "2014-04", "2014-01",
    # 2013
    "2013-12", "2013-11", "2013-06", "2013-05", "2013-04", "2013-03", "2013-02", "2013-01",
    # 2012 (Todos)
    "2012-12", "2012-11", "2012-10", "2012-09", "2012-08", "2012-07",
    "2012-06", "2012-05", "2012-04", "2012-03", "2012-02", "2012-01",
    # 2011 (Todos)
    "2011-12", "2011-11", "2011-10", "2011-09", "2011-08", "2011-07",
    "2011-06", "2011-05", "2011-04", "2011-03", "2011-02", "2011-01",
    # 2010
    "2010-12", "2010-11", "2010-10", "2010-09"
]


def clean_batches():
    if not os.path.exists(BATCH_DIR):
        print(f"Diretório não encontrado: {BATCH_DIR}")
        return

    print(f"Verificando arquivos em: {BATCH_DIR}")
    print(f"Procurando por {len(EXCLUDE_LIST)} meses específicos para exclusão...")

    deleted_count = 0

    # Lista todos os arquivos .xlsx na pasta batch
    all_files = glob.glob(os.path.join(BATCH_DIR, "*.xlsx"))

    for file_path in all_files:
        filename = os.path.basename(file_path)

        # Verifica se algum dos meses proibidos está no nome do arquivo
        # O nome geralmente é algo como: ...all_for_2011-01-01_concatenated...
        should_delete = False
        for bad_month in EXCLUDE_LIST:
            if bad_month in filename:
                should_delete = True
                break

        if should_delete:
            try:
                os.remove(file_path)
                print(f"[REMOVIDO] {filename}")
                deleted_count += 1
            except Exception as e:
                print(f"[ERRO] Falha ao remover {filename}: {e}")

    print("-" * 30)
    print(f"Limpeza concluída. Total de arquivos removidos: {deleted_count}")
    print("Agora você pode rodar o script principal sem risco de conflito futuro.")


if __name__ == "__main__":
    confirmation = input("Tem certeza que deseja apagar os batches dos meses listados? (S/N): ")
    if confirmation.lower() == 's':
        clean_batches()
    else:
        print("Operação cancelada.")