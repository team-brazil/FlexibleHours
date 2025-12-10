@echo off
setlocal

:: Script to run all project tests

:: Check if --coverage parameter was passed
set "RUN_COVERAGE=false"
if "%~1"=="--coverage" (
    set "RUN_COVERAGE=true"
)

:: Check if .venv directory exists
if not exist ".venv\" (
    echo Creating virtual environment...
    python -m venv .venv
    
    :: Activate virtual environment
    call .venv\Scripts\activate.bat
    
    :: Update pip
    python -m pip install --upgrade pip
    
    :: Install development dependencies
    if exist "requirements-dev.txt" (
        echo Installing development dependencies...
        pip install -r requirements-dev.txt
    ) else (
        echo requirements-dev.txt file not found.
        exit /b 1
    )
) else (
    echo Virtual environment already exists.
    call .venv\Scripts\activate.bat
)

:: Run tests
if "%RUN_COVERAGE%"=="true" (
    echo Running tests with coverage collection...
    coverage run -m pytest tests/ -v
    echo Generating coverage report...
    coverage report
    coverage html
    echo Coverage reports generated in .coverage and htmlcov/
) else (
    echo Running tests...
    python -m pytest tests/ -v
)

echo Tests completed.
pause
