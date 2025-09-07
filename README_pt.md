# FlexibleHours

Projeto para análise de flexibilidade em postagens de vagas de emprego usando inteligência artificial local.

## Descrição

Este projeto analisa postagens de vagas de emprego para identificar requisitos de flexibilidade indesejada (como turnos variáveis, plantões, etc.) e flexibilidade desejada (como horários flexíveis escolhidos pelo empregado). Utiliza um modelo de linguagem local (Ollama) para processar as descrições das vagas.

## Funcionalidades

- Processamento de arquivos CSV e XLSX contendo postagens de vagas de emprego
- Análise de flexibilidade usando IA local (Ollama)
- Classificação de vagas com base em critérios pré-definidos
- Geração de relatórios em formato Excel com coloração condicional
- Funcionalidade de retomada de processamento a partir de pontos de interrupção
- Salvamento em batches para evitar perda de dados em caso de interrupção

## Requisitos

- Python 3.6+
- Ollama (com modelo qwen3:8b ou similar)
- Pandas
- OpenPyXL
- httpx
- tqdm

## Instalação

1. Clone o repositório:
   ```
   git clone <url-do-repositório>
   cd FlexibleHours
   ```

2. Crie um ambiente virtual e ative-o:
   ```
   python3 -m venv .venv
   source .venv/bin/activate  # No Windows: .venv\Scripts\activate
   ```

3. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```

4. Instale o Ollama e baixe o modelo necessário:
   ```
   # Siga as instruções em https://ollama.ai para instalar o Ollama
   ollama pull qwen3:8b
   ```

## Uso

1. Certifique-se de que o Ollama está em execução:
   ```
   ollama serve
   ```

2. Execute o script principal:
   ```
   ./run_process.sh
   ```

   Ou diretamente com Python:
   ```
   python src/processFile_Local_AI.py
   ```

## Estrutura do Projeto

```
FlexibleHours/
├── src/
│   └── processFile_Local_AI.py     # Script principal
├── input/
│   ├── 1000_unit_lightcast_sample.csv  # Arquivo de exemplo
│   └── us_postings_sample.xlsx         # Arquivo de exemplo
├── output/
│   └── results/                        # Resultados do processamento
├── logs/                               # Arquivos de log
├── tests/                              # Testes automatizados
├── requirements.txt                    # Dependências do projeto
├── requirements-dev.txt                # Dependências de desenvolvimento
├── run_process.sh                      # Script para executar o processamento
├── run_tests.sh                        # Script para executar os testes
└── README.md                           # Este arquivo
```

## Testes

O projeto inclui uma suíte abrangente de testes automatizados. Para executar os testes:

```
./run_tests.sh
```

### Cobertura de Código

O projeto está configurado para gerar relatórios de cobertura de código. A configuração da cobertura está definida no arquivo `.coveragerc`.

Para executar os testes com coleta de cobertura, use:

```
./run_tests.sh --coverage
```

Este comando irá:
1. Executar os testes com coleta de cobertura
2. Gerar um relatório textual no terminal
3. Gerar um relatório em formato HTML no diretório `htmlcov/`

Alternativamente, você pode usar os comandos do coverage diretamente:

```
coverage run -m pytest tests/
coverage report
coverage html
```

Os arquivos de cobertura (`.coverage`, `htmlcov/`) não são versionados e estão incluídos no `.gitignore`.

Para mais informações sobre os testes, consulte [tests/README.md](tests/README.md).

## Configuração

As configurações principais estão no início do arquivo `src/processFile_Local_AI.py`:

- `INPUT_DIR_NAME_FILE`: Caminho para o arquivo de entrada
- `OLLAMA_URL`: URL do servidor Ollama
- `MODEL_NAME`: Nome do modelo a ser usado
- `OUTPUT_PATH`: Diretório de saída
- `BATCH_SIZE`: Número de registros por batch

## Funcionamento dos Scripts

### Script Principal (`src/processFile_Local_AI.py`)

O script principal realiza as seguintes operações:

1. **Processamento de Arquivos**: Lê arquivos CSV ou XLSX contendo postagens de vagas de emprego
2. **Análise de Flexibilidade**: Usa a API do Ollama para analisar as descrições das vagas e classificá-las quanto à flexibilidade
3. **Retomada de Processamento**: Permite retomar o processamento a partir do ponto de interrupção anterior
4. **Salvamento em Batches**: Salva resultados intermediários em batches para evitar perda de dados
5. **Geração de Relatórios**: Cria relatórios em formato Excel com coloração condicional

### Script de Testes (`tests/test_processFile_Local_AI.py`)

O script de testes inclui:

1. **Testes Unitários**: Testa todas as funções principais do script
2. **Testes de Integração**: Testa o processo completo de análise de vagas
3. **Testes de Retomada**: Verifica a funcionalidade de retomada de processamento
4. **Mocks**: Usa mocks para simular chamadas à API do Ollama

## Contribuindo

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## Licença

Distribuído sob a licença MIT. Veja `LICENSE` para mais informações.

## Contato

Seu Nome - seu.email@exemplo.com

Link do Projeto: [https://github.com/seu-usuario/FlexibleHours](https://github.com/seu-usuario/FlexibleHours)

## English Version

For an English version of this README, see [README.md](README.md).