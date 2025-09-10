#!/bin/bash

# Script para executar todos os testes do projeto

# Verificar se foi passado o parâmetro --coverage
RUN_COVERAGE=false
if [[ "$1" == "--coverage" ]]; then
    RUN_COVERAGE=true
fi

# Verificar se o diretório .venv existe
if [ ! -d ".venv" ]; then
    echo "Criando ambiente virtual..."
    python3 -m venv .venv
    
    # Ativar o ambiente virtual
    source .venv/bin/activate
    
    # Atualizar pip
    pip install --upgrade pip
    
    # Instalar dependências de desenvolvimento
    if [ -f "requirements-dev.txt" ]; then
        echo "Instalando dependências de desenvolvimento..."
        pip install -r requirements-dev.txt
    else
        echo "Arquivo requirements-dev.txt não encontrado."
        exit 1
    fi
else
    echo "Ambiente virtual já existe."
    source .venv/bin/activate
fi

# Executar os testes
if [ "$RUN_COVERAGE" = true ]; then
    echo "Executando testes com coleta de cobertura..."
    coverage run -m pytest tests/ -v
    echo "Gerando relatório de cobertura..."
    coverage report
    coverage html
    echo "Relatórios de cobertura gerados em .coverage e htmlcov/"
else
    echo "Executando testes..."
    python -m pytest tests/ -v
fi

echo "Testes concluídos."