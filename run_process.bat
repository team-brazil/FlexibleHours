@echo off
setlocal

:: Script to set up and run processFile_Local_AI.py

:: Check if .venv directory exists
if not exist ".venv\" (
    echo Creating virtual environment...
    python -m venv .venv
    
    :: Activate virtual environment
    call .venv\Scripts\activate.bat
    
    :: Update pip
    python -m pip install --upgrade pip
    
    :: Install dependencies
    if exist "requirements.txt" (
        echo Installing dependencies...
        pip install -r requirements.txt
    ) else (
        echo requirements.txt file not found.
        exit /b 1
    )
) else (
    echo Virtual environment already exists.
    call .venv\Scripts\activate.bat
)

:: Run the Python script
echo Running src/processFile_Local_AI.py...
python src\processFile_Local_AI.py

echo Script completed.
pause
