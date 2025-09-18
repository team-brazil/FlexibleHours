#!/bin/bash

# Script to run all project tests

# Check if --coverage parameter was passed
RUN_COVERAGE=false
if [[ "$1" == "--coverage" ]]; then
    RUN_COVERAGE=true
fi

# Check if .venv directory exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    
    # Activate virtual environment
    source .venv/bin/activate
    
    # Update pip
    pip install --upgrade pip
    
    # Install development dependencies
    if [ -f "requirements-dev.txt" ]; then
        echo "Installing development dependencies..."
        pip install -r requirements-dev.txt
    else
        echo "requirements-dev.txt file not found."
        exit 1
    fi
else
    echo "Virtual environment already exists."
    source .venv/bin/activate
fi

# Run tests
if [ "$RUN_COVERAGE" = true ]; then
    echo "Running tests with coverage collection..."
    coverage run -m pytest tests/ -v
    echo "Generating coverage report..."
    coverage report
    coverage html
    echo "Coverage reports generated in .coverage and htmlcov/"
else
    echo "Running tests..."
    python -m pytest tests/ -v
fi

echo "Tests completed."