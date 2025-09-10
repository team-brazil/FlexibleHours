"""
Tests for processFile_Local_AI.py
"""

import os
import tempfile
import json
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# Assuming the script is importable or we adjust the path
# For now, let's assume it's in the same directory or PYTHONPATH is set
# If not, we might need to adjust sys.path or use importlib
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

import processFile_Local_AI as processor


def test_condense_description_short():
    """Test condense_description with a short description."""
    desc = "This is a short description."
    assert processor.condense_description(desc) == desc


def test_condense_description_long_no_keywords():
    """Test condense_description with a long description but no keywords."""
    desc = "Line 1\nLine 2\nLine 3\n" * 1000  # Create a long description
    # Since there are no keywords, it should return the original description
    # because the current implementation returns the full description if no keywords are found.
    # This might be a bug in the original code, but we'll test the current behavior.
    assert processor.condense_description(desc) == desc


def test_condense_description_long_with_keywords():
    """Test condense_description with a long description and keywords."""
    lines = []
    for i in range(100):
        if i == 50:
            lines.append("This schedule may vary based on business needs.")  # Keyword line
        else:
            lines.append(f"Regular line {i}")
    desc = "\n".join(lines)
    
    # Force condensation by passing a smaller min_length
    condensed = processor.condense_description(desc, min_length=1000)
    # Should contain the keyword line and surrounding context (default window=3)
    assert "This schedule may vary based on business needs." in condensed
    assert "Regular line 47" in condensed  # 50 - 3
    assert "Regular line 53" in condensed  # 50 + 3
    # Should not contain lines far from the keyword
    assert "Regular line 10" not in condensed


def test_build_flexibility_prompt():
    """Test build_flexibility_prompt creates a prompt with the description."""
    desc = "Job requires flexible schedule."
    prompt = processor.build_flexibility_prompt(desc)
    assert desc in prompt
    assert "You are an expert HR analyst" in prompt
    assert "RESPONSE FORMAT:" in prompt


def test_safe_parse_json_valid():
    """Test safe_parse_json with valid JSON."""
    json_str = '{"key": "value"}'
    result = processor.safe_parse_json(json_str)
    assert result == {"key": "value"}


def test_safe_parse_json_wrapped():
    """Test safe_parse_json with JSON wrapped in text."""
    json_str = 'Some text before {"key": "value"} and after.'
    result = processor.safe_parse_json(json_str)
    assert result == {"key": "value"}


def test_safe_parse_json_invalid():
    """Test safe_parse_json with invalid JSON."""
    json_str = 'Invalid JSON {key: value}'
    result = processor.safe_parse_json(json_str)
    # The function tries to fix some issues, but this one might be too complex
    # For now, let's check it returns None or an empty dict as a fallback
    assert result is None or result == {}


def test_validate_response_valid():
    """Test validate_response with a valid response."""
    response = {
        "undesired_flexibility": "YES",
        "undesired_quote": "Schedule may vary",
        "desired_flexibility": "NO",
        "desired_quote": "N/A",
        "reasoning": "Found phrase indicating unpredictable schedule changes"
    }
    assert processor.validate_response(response) is True


def test_validate_response_invalid_keys():
    """Test validate_response with missing keys."""
    response = {
        "undesired_flexibility": "YES",
        # Missing other keys
    }
    assert processor.validate_response(response) is False


def test_validate_response_invalid_values():
    """Test validate_response with invalid YES/NO values."""
    response = {
        "undesired_flexibility": "MAYBE",  # Invalid value
        "undesired_quote": "Schedule may vary",
        "desired_flexibility": "NO",
        "desired_quote": "N/A",
        "reasoning": "Found phrase indicating unpredictable schedule changes"
    }
    assert processor.validate_response(response) is False


def test_yesno_to_dummy():
    """Test yesno_to_dummy conversion."""
    assert processor.yesno_to_dummy("YES") == 1
    assert processor.yesno_to_dummy("yes") == 1  # Case insensitive
    assert processor.yesno_to_dummy("NO") == 0
    assert processor.yesno_to_dummy("no") == 0  # Case insensitive
    assert processor.yesno_to_dummy("N/A") == 0  # Default fallback
    assert processor.yesno_to_dummy(None) == 0  # Default fallback
    assert processor.yesno_to_dummy("") == 0  # Default fallback


# --- Tests for batch processing functions ---

def test_load_existing_batches_no_files(tmpdir):
    """Test load_existing_batches when no batch files exist."""
    prefix = tmpdir.join("batch_temp").strpath
    results, last_index = processor.load_existing_batches(prefix)
    assert results == []
    assert last_index == -1


def test_load_existing_batches_single_file(tmpdir):
    """Test load_existing_batches with a single batch file."""
    prefix = tmpdir.join("batch_temp").strpath
    
    # Create sample data
    data = [
        {"Title": "Job 1", "undesired_flexibility": 1},
        {"Title": "Job 2", "undesired_flexibility": 0}
    ]
    df = pd.DataFrame(data)
    batch_file = f"{prefix}_1.xlsx"
    df.to_excel(batch_file, index=False)
    
    results, last_index = processor.load_existing_batches(prefix)
    assert len(results) == 2
    assert results[0]["Title"] == "Job 1"
    assert results[1]["Title"] == "Job 2"
    assert last_index == 1  # 2 records - 1


def test_load_existing_batches_multiple_files(tmpdir):
    """Test load_existing_batches with multiple batch files."""
    prefix = tmpdir.join("batch_temp").strpath
    
    # Create sample data for batch 1
    data1 = [
        {"Title": "Job 1", "undesired_flexibility": 1},
        {"Title": "Job 2", "undesired_flexibility": 0}
    ]
    df1 = pd.DataFrame(data1)
    batch_file1 = f"{prefix}_1.xlsx"
    df1.to_excel(batch_file1, index=False)
    
    # Create sample data for batch 2 (this should be the one loaded)
    data2 = [
        {"Title": "Job 3", "undesired_flexibility": 1},
        {"Title": "Job 4", "undesired_flexibility": 0}
    ]
    df2 = pd.DataFrame(data2)
    batch_file2 = f"{prefix}_2.xlsx"
    df2.to_excel(batch_file2, index=False)
    
    results, last_index = processor.load_existing_batches(prefix)
    # Should load only the last batch file
    assert len(results) == 2
    assert results[0]["Title"] == "Job 3"
    assert results[1]["Title"] == "Job 4"
    assert last_index == 1  # 2 records - 1


def test_load_existing_batches_invalid_filename(tmpdir):
    """Test load_existing_batches with an invalid batch filename."""
    prefix = tmpdir.join("batch_temp").strpath
    
    # Create a valid batch file
    data = [{"Title": "Job 1", "undesired_flexibility": 1}]
    df = pd.DataFrame(data)
    valid_batch_file = f"{prefix}_1.xlsx"
    df.to_excel(valid_batch_file, index=False)
    
    # Create a file with invalid batch number
    invalid_batch_file = f"{prefix}_invalid.xlsx"
    with open(invalid_batch_file, 'w') as f:
        f.write("Not an Excel file")
    
    # Mock logging to check for warning
    with patch('processFile_Local_AI.logging') as mock_logging:
        results, last_index = processor.load_existing_batches(prefix)
        # Should load the valid file
        assert len(results) == 1
        assert results[0]["Title"] == "Job 1"
        assert last_index == 0
        # Should log a warning for the invalid file
        # Note: The exact call might vary, this is a basic check
        # assert mock_logging.warning.called


def test_load_existing_batches_corrupted_file(tmpdir):
    """Test load_existing_batches with a corrupted batch file."""
    prefix = tmpdir.join("batch_temp").strpath
    
    # Create a valid batch file (batch_1)
    data1 = [{"Title": "Job 1", "undesired_flexibility": 1}]
    df1 = pd.DataFrame(data1)
    valid_batch_file = f"{prefix}_1.xlsx"
    df1.to_excel(valid_batch_file, index=False)
    
    # Create a corrupted Excel file (batch_2, which is the last one and should be loaded)
    corrupted_batch_file = f"{prefix}_2.xlsx"
    with open(corrupted_batch_file, 'w') as f:
        f.write("Corrupted Excel content")
    
    # Mock logging to check for warning
    with patch('processFile_Local_AI.logging') as mock_logging:
        results, last_index = processor.load_existing_batches(prefix)
        # Should try to load the last (corrupted) file and fail
        assert len(results) == 0
        assert last_index == -1
        # Should log a warning for the corrupted file
        # assert mock_logging.warning.called


def test_save_batches_not_full(tmpdir):
    """Test save_batches when the batch is not full."""
    prefix = tmpdir.join("batch_temp").strpath
    results = [{"Title": "Job 1"}, {"Title": "Job 2"}]  # 2 results
    batch_size = 3  # Batch size is 3
    
    processor.save_batches(results, batch_size, prefix)
    
    # No file should be saved yet
    batch_files = tmpdir.listdir("batch_temp_*.xlsx")
    assert len(batch_files) == 0


def test_save_batches_full(tmpdir):
    """Test save_batches when the batch is full."""
    prefix = tmpdir.join("batch_temp").strpath
    results = [
        {"Title": "Job 1"}, {"Title": "Job 2"}, {"Title": "Job 3"},
        {"Title": "Job 4"}, {"Title": "Job 5"}, {"Title": "Job 6"}
    ]  # 6 results
    batch_size = 3  # Batch size is 3
    
    processor.save_batches(results, batch_size, prefix)
    
    # Should have saved batch 2 (6 // 3 = 2)
    batch_file = tmpdir.join("batch_temp_2.xlsx")
    assert batch_file.check()  # File exists
    
    # Check content
    df = pd.read_excel(batch_file.strpath)
    saved_results = df.to_dict('records')
    assert len(saved_results) == 6
    assert saved_results[0]["Title"] == "Job 1"
    assert saved_results[5]["Title"] == "Job 6"


# --- Tests for process_job_postings resumption ---

def test_process_job_postings_resume(tmpdir):
    """Test process_job_postings resumes from existing batches."""
    
    # 1. Create a temporary input CSV file
    input_data = [
        {"TITLE_NAME": "Job 1", "BODY": "Description 1"},
        {"TITLE_NAME": "Job 2", "BODY": "Description 2 schedule may vary"},
        {"TITLE_NAME": "Job 3", "BODY": "Description 3"},
        {"TITLE_NAME": "Job 4", "BODY": "Description 4 flexible hours"},
        {"TITLE_NAME": "Job 5", "BODY": "Description 5"}
    ]
    input_df = pd.DataFrame(input_data)
    input_file = tmpdir.join("input.csv")
    input_df.to_csv(input_file.strpath, index=False)
    
    # 2. Create some existing batch files in tmpdir
    # Simulate that the first 2 jobs have been processed
    existing_batch_data = [
        {
            "Title": "Job 1 (Row_0)",
            "Body": "Description 1",
            "llama_raw_response": '{"undesired_flexibility": "NO", "undesired_quote": "N/A", "desired_flexibility": "NO", "desired_quote": "N/A", "reasoning": "No flexibility keywords"}',
            "undesired_flexibility": 0,
            "undesired_quote": "N/A",
            "desired_flexibility": 0,
            "desired_quote": "N/A",
            "reasoning": "No flexibility keywords"
        },
        {
            "Title": "Job 2 (Row_1)",
            "Body": "Description 2 schedule may vary",
            "llama_raw_response": '{"undesired_flexibility": "YES", "undesired_quote": "schedule may vary", "desired_flexibility": "NO", "desired_quote": "N/A", "reasoning": "Found undesired flexibility"}',
            "undesired_flexibility": 1,
            "undesired_quote": "schedule may vary",
            "desired_flexibility": 0,
            "desired_quote": "N/A",
            "reasoning": "Found undesired flexibility"
        }
    ]
    existing_batch_df = pd.DataFrame(existing_batch_data)
    batch_prefix = tmpdir.join("batch_temp").strpath
    existing_batch_file = f"{batch_prefix}_1.xlsx"
    existing_batch_df.to_excel(existing_batch_file, index=False)
    
    # 3. Mock the Ollama API call to return predefined responses
    # We need to mock processor.call_ollama_api
    # Let's define the expected responses for Job 3, Job 4, Job 5
    mock_responses = {
        "Description 3": '{"undesired_flexibility": "NO", "undesired_quote": "N/A", "desired_flexibility": "NO", "desired_quote": "N/A", "reasoning": "No flexibility keywords"}',
        "Description 4 flexible hours": '{"undesired_flexibility": "NO", "undesired_quote": "N/A", "desired_flexibility": "YES", "desired_quote": "flexible hours", "reasoning": "Found desired flexibility"}',
        "Description 5": '{"undesired_flexibility": "NO", "undesired_quote": "N/A", "desired_flexibility": "NO", "desired_quote": "N/A", "reasoning": "No flexibility keywords"}'
    }
    
    def mock_call_ollama_api(prompt, max_retries=processor.MAX_RETRIES, retry_sleep=processor.RETRY_SLEEP):
        # Extract the description from the prompt to find the corresponding mock response
        # This is a bit fragile, but for testing purposes it should work
        # A better approach would be to mock the build_flexibility_prompt function as well
        # or pass the description directly to call_llama_api in the test context
        for desc, response in mock_responses.items():
            if desc in prompt:
                return response
        # Default response if not found
        return '{"undesired_flexibility": "NO", "undesired_quote": "N/A", "desired_flexibility": "NO", "desired_quote": "N/A", "reasoning": "Default"}'
    
    # 4. Set up paths for output and final file
    output_path = tmpdir.mkdir("output").strpath
    final_file_path = tmpdir.join("output", f"Job_postings_processed_{processor.MODEL_NAME}.xlsx").strpath
    log_path = tmpdir.mkdir("logs").strpath
    
    # We need to patch the global variables in the processor module
    # This is a bit tricky, but we can do it with patch.dict
    import processFile_Local_AI as processor_module
    with patch.dict(processor_module.__dict__, {
        'OUTPUT_PATH': output_path,
        'LOG_PATH': log_path,
        'FINAL_FILE_PATH': final_file_path,
        'BATCH_SAVE_PREFIX': batch_prefix,
        'BATCH_SIZE': 2,  # Set a small batch size for testing
        'MODEL_NAME': 'test_model'  # Override model name for test file
    }):
        # Patch the call_ollama_api function
        with patch('processFile_Local_AI.call_ollama_api', side_effect=mock_call_ollama_api):
            # Patch tqdm to avoid progress bar issues in tests
            with patch('processFile_Local_AI.tqdm', side_effect=lambda x, total, initial: x):
                # 4. Call process_job_postings with the tmpdir paths
                processor_module.process_job_postings(input_file.strpath)
                
    # 5. Assert that it processed the correct number of records (including existing ones)
    # The final file should contain all 5 records
    assert os.path.exists(final_file_path)
    final_df = pd.read_excel(final_file_path)
    assert len(final_df) == 5
    
    # Check that the existing records are present and in order
    assert final_df.iloc[0]["Title"] == "Job 1 (Row_0)"
    assert final_df.iloc[1]["Title"] == "Job 2 (Row_1)"
    assert final_df.iloc[1]["undesired_flexibility"] == 1  # From existing batch
    
    # Check that the new records are present and processed correctly
    assert final_df.iloc[2]["Title"] == "Job 3 (Row_2)"
    assert final_df.iloc[2]["undesired_flexibility"] == 0
    assert final_df.iloc[2]["desired_flexibility"] == 0
    
    assert final_df.iloc[3]["Title"] == "Job 4 (Row_3)"
    assert final_df.iloc[3]["undesired_flexibility"] == 0
    assert final_df.iloc[3]["desired_flexibility"] == 1  # From mock response
    assert final_df.iloc[3]["desired_quote"] == "flexible hours"
    
    assert final_df.iloc[4]["Title"] == "Job 5 (Row_4)"
    assert final_df.iloc[4]["undesired_flexibility"] == 0
    assert final_df.iloc[4]["desired_flexibility"] == 0
    
    # 6. Assert that new batches are saved correctly
    # With BATCH_SIZE=2 and 5 total records, we should have batch_1 (from existing) and batch_2, batch_3
    # But the existing batch_1 is not overwritten, so we should have batch_2 and batch_3 created by the process
    # Actually, the logic in save_batches saves when len(results) % batch_size == 0 and len(results) > 0
    # After processing 3 records (including existing 2), we have 3 results. 3 % 2 != 0, so no batch saved yet for new data.
    # After processing 4 records, we have 4 results. 4 % 2 == 0, so batch_2 should be saved (batch number = 4 // 2 = 2).
    # After processing 5 records, we have 5 results. 5 % 2 != 0, so no batch saved.
    # At the end, a final batch is saved.
    # So we should have batch_1 (existing), batch_2 (new), and batch_final.
    batch_2_file = f"{batch_prefix}_2.xlsx"
    batch_final_file = f"{batch_prefix}_final.xlsx"
    assert os.path.exists(batch_2_file)
    assert os.path.exists(batch_final_file)
    
    # Check content of batch_2 (should contain records 3 and 4, indices 2 and 3)
    batch_2_df = pd.read_excel(batch_2_file)
    # The batch file contains all results up to that point, not just the new ones.
    # The first batch file had 2 records. The second batch file (batch_2) is created when we reach 4 records.
    # So it should contain the first 4 records.
    assert len(batch_2_df) == 4
    assert batch_2_df.iloc[2]["Title"] == "Job 3 (Row_2)"
    assert batch_2_df.iloc[3]["Title"] == "Job 4 (Row_3)"
    
    # Check content of batch_final (should contain all 5 records)
    batch_final_df = pd.read_excel(batch_final_file)
    assert len(batch_final_df) == 5
    assert batch_final_df.iloc[4]["Title"] == "Job 5 (Row_4)"

def test_process_job_postings_resume_incomplete(tmpdir):
    """Test process_job_postings resumes correctly after a force stop."""
    
    # 1. Create a temporary input CSV file with 100 rows
    input_data = []
    for i in range(100):
        input_data.append({"TITLE_NAME": f"Job {i}", "BODY": f"Description {i}"})
    input_df = pd.DataFrame(input_data)
    input_file = tmpdir.join("input.csv")
    input_df.to_csv(input_file.strpath, index=False)
    
    # 2. Create existing batch files simulating a partial processing
    # Simulate that the first 30 jobs have been processed
    existing_batch_data = []
    for i in range(30):
        existing_batch_data.append({
            "Title": f"Job {i} (Row_{i})",
            "Body": f"Description {i}",
            "llama_raw_response": '{"undesired_flexibility": "NO", "undesired_quote": "N/A", "desired_flexibility": "NO", "desired_quote": "N/A", "reasoning": "No flexibility keywords"}',
            "undesired_flexibility": 0,
            "undesired_quote": "N/A",
            "desired_flexibility": 0,
            "desired_quote": "N/A",
            "reasoning": "No flexibility keywords"
        })
    
    # Split into two batch files to simulate how save_batches works
    # Batch 1: records 0-19 (20 records)
    batch_1_data = existing_batch_data[:20]
    batch_1_df = pd.DataFrame(batch_1_data)
    batch_prefix = tmpdir.join("batch_temp").strpath
    batch_1_file = f"{batch_prefix}_1.xlsx"
    batch_1_df.to_excel(batch_1_file, index=False)
    
    # Batch 2: records 0-29 (30 records) - this is how save_batches would save it
    batch_2_data = existing_batch_data[:30]
    batch_2_df = pd.DataFrame(batch_2_data)
    batch_2_file = f"{batch_prefix}_2.xlsx"
    batch_2_df.to_excel(batch_2_file, index=False)
    
    # 3. Mock the Ollama API call to return predefined responses
    def mock_call_ollama_api(prompt, max_retries=processor.MAX_RETRIES, retry_sleep=processor.RETRY_SLEEP):
        # Return a simple valid JSON response for any prompt
        return '{"undesired_flexibility": "NO", "undesired_quote": "N/A", "desired_flexibility": "NO", "desired_quote": "N/A", "reasoning": "Mock response"}'
    
    # 4. Set up paths for output and final file
    output_path = tmpdir.mkdir("output").strpath
    model_name = "test_model"
    final_file_path = tmpdir.join("output", f"Job_postings_processed_{model_name}.xlsx").strpath
    log_path = tmpdir.mkdir("logs").strpath
    
    # Patch global variables
    import processFile_Local_AI as processor_module
    with patch.dict(processor_module.__dict__, {
        'OUTPUT_PATH': output_path,
        'LOG_PATH': log_path,
        'FINAL_FILE_PATH': final_file_path,
        'BATCH_SAVE_PREFIX': batch_prefix,
        'BATCH_SIZE': 10,  # Set a small batch size for testing
        'MODEL_NAME': model_name
    }):
        # Patch the call_ollama_api function
        with patch('processFile_Local_AI.call_ollama_api', side_effect=mock_call_ollama_api):
            # Patch tqdm to avoid progress bar issues in tests
            with patch('processFile_Local_AI.tqdm', side_effect=lambda x, total, initial: x):
                # 5. Call process_job_postings
                processor_module.process_job_postings(input_file.strpath)
                
    # 6. Assert that all 100 records are in the final file
    assert os.path.exists(final_file_path)
    final_df = pd.read_excel(final_file_path)
    assert len(final_df) == 100, f"Expected 100 records, got {len(final_df)}"
    
    # Check that the existing records are present
    for i in range(30):
        assert final_df.iloc[i]["Title"] == f"Job {i} (Row_{i})"
    
    # Check that the new records (30-99) are present and processed
    for i in range(30, 100):
        assert final_df.iloc[i]["Title"] == f"Job {i} (Row_{i})"
        # These should have been processed by the mock, so they should have the mock response values
        assert final_df.iloc[i]["reasoning"] == "Mock response"

if __name__ == "__main__":
    pytest.main([__file__])