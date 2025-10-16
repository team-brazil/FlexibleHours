"""
Testes para a função read_file_adaptive
"""
import os
import tempfile
import pandas as pd
import pytest
from src.processFile_Local_AI import read_file_adaptive


def test_read_csv_file():
    """Testa a leitura de um arquivo CSV"""
    # Criar um arquivo CSV temporário para teste
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
        tmp.write("col1,col2,col3\n1,2,3\n4,5,6\n")
        tmp_path = tmp.name
    
    try:
        df = read_file_adaptive(tmp_path)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 3)
        assert list(df.columns) == ['col1', 'col2', 'col3']
    finally:
        os.remove(tmp_path)


def test_read_tsv_file():
    """Testa a leitura de um arquivo TSV"""
    # Criar um arquivo TSV temporário para teste
    with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as tmp:
        tmp.write("col1\tcol2\tcol3\n1\t2\t3\n4\t5\t6\n")
        tmp_path = tmp.name
    
    try:
        df = read_file_adaptive(tmp_path)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 3)
        assert list(df.columns) == ['col1', 'col2', 'col3']
    finally:
        os.remove(tmp_path)


def test_read_excel_file():
    """Testa a leitura de um arquivo Excel"""
    # Criar um arquivo Excel temporário para teste
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        # Criar um DataFrame e salvá-lo como Excel
        df_original = pd.DataFrame({'col1': [1, 4], 'col2': [2, 5], 'col3': [3, 6]})
        df_original.to_excel(tmp_path, index=False)
        
        # Ler com a função adaptativa
        df = read_file_adaptive(tmp_path)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 3)
        assert list(df.columns) == ['col1', 'col2', 'col3']
    finally:
        os.remove(tmp_path)


def test_read_json_file():
    """Testa a leitura de um arquivo JSON"""
    # Criar um arquivo JSON temporário para teste
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
        tmp.write('[{"col1": 1, "col2": 2, "col3": 3}, {"col1": 4, "col2": 5, "col3": 6}]')
        tmp_path = tmp.name
    
    try:
        df = read_file_adaptive(tmp_path)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 3)
        assert list(df.columns) == ['col1', 'col2', 'col3']
    finally:
        os.remove(tmp_path)


def test_unsupported_format():
    """Testa o tratamento de formato não suportado"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
        tmp.write("test content")
        tmp_path = tmp.name
    
    try:
        with pytest.raises(ValueError, match="Unsupported file format"):
            read_file_adaptive(tmp_path)
    finally:
        os.remove(tmp_path)


def test_file_not_found():
    """Testa o tratamento de arquivo não encontrado"""
    with pytest.raises(FileNotFoundError, match="File not found"):
        read_file_adaptive("/path/that/does/not/exist.csv")


def test_read_csv_with_kwargs():
    """Testa a leitura de CSV com parâmetros adicionais"""
    # Criar um arquivo CSV temporário para teste
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
        tmp.write("1,2,3\n4,5,6\n")
        tmp_path = tmp.name
    
    try:
        df = read_file_adaptive(tmp_path, header=None, names=['a', 'b', 'c'])
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 3)
        assert list(df.columns) == ['a', 'b', 'c']
    finally:
        os.remove(tmp_path)


if __name__ == "__main__":
    pytest.main([__file__])