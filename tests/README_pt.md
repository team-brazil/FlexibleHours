# Testes

Este diretório contém a suíte de testes automatizados para o projeto FlexibleHours.

## Estrutura dos Testes

- `test_processFile_Local_AI.py`: Arquivo principal de testes que cobre todas as funcionalidades do script `src/processFile_Local_AI.py`

## Executando os Testes

Para executar todos os testes, você pode usar o script principal:

```bash
./run_tests.sh
```

Ou executar diretamente com pytest:

```bash
python -m pytest tests/ -v
```

### Executando Testes com Cobertura

Para executar os testes com coleta de cobertura de código:

```bash
./run_tests.sh --coverage
```

Ou usando os comandos do coverage diretamente:

```bash
coverage run -m pytest tests/
coverage report
coverage html
```

## Descrição dos Testes

### Testes Unitários

Os testes unitários verificam o funcionamento correto de funções individuais do script principal:

- `test_condense_description_*`: Testa a função de condensação de descrições longas
- `test_build_flexibility_prompt`: Verifica a criação correta do prompt para a API do Ollama
- `test_safe_parse_json_*`: Testa a função de parsing seguro de JSON
- `test_validate_response_*`: Verifica a validação de respostas da API
- `test_yesno_to_dummy`: Testa a conversão de valores YES/NO para 1/0

### Testes de Processamento de Batches

Esses testes verificam o funcionamento correto do sistema de salvamento em batches:

- `test_load_existing_batches_*`: Testa o carregamento de batches existentes para retomada de processamento
- `test_save_batches_*`: Verifica o salvamento correto de batches

### Testes de Integração

Esses testes verificam o funcionamento completo do processo de análise de vagas:

- `test_process_job_postings_resume`: Testa a retomada de processamento a partir de batches existentes
- `test_process_job_postings_resume_incomplete`: Verifica a retomada correta após uma interrupção

## Mocks e Stubs

Os testes utilizam mocks para simular chamadas à API do Ollama e para isolar as unidades de teste. Isso permite:

- Executar testes de forma rápida e determinística
- Testar cenários específicos sem depender de serviços externos
- Verificar o comportamento do código em caso de falhas na API

## Contribuindo com Testes

Ao adicionar novas funcionalidades ao projeto, certifique-se de incluir testes adequados cobrindo os novos casos de uso. Siga o padrão existente nos testes atuais.

## English Version

For an English version of this README, see [README.md](README.md).