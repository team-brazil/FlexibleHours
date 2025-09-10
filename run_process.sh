#!/bin/bash

# Script para configurar executar o processFile_Local_AI.py

# Verificar se o diretório .venv existe
if [ ! -d ".venv" ]; then
    echo "Criando ambiente virtual..."
    python3 -m venv .venv
    
    # Ativar o ambiente virtual
    source .venv/bin/activate
    
    # Atualizar pip
    pip install --upgrade pip
    
    # Instalar dependências
    if [ -f "requirements.txt" ]; then
        echo "Instalando dependências..."
        pip install -r requirements.txt
    else
        echo "Arquivo requirements.txt não encontrado."
        exit 1
    fi
else
    echo "Ambiente virtual já existe."
    source .venv/bin/activate
fi

# Executar o script Python
echo "Executando src/processFile_Local_AI.py..."
python src/processFile_Local_AI.py

echo "Script concluído."