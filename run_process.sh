#!/bin/bash

# Script to set up and run processFile_Local_AI.py

# Check if .venv directory exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    
    # Activate virtual environment
    source .venv/bin/activate
    
    # Update pip
    pip install --upgrade pip
    
    # Install dependencies
    if [ -f "requirements.txt" ]; then
        echo "Installing dependencies..."
        pip install -r requirements.txt
    else
        echo "requirements.txt file not found."
        exit 1
    fi
else
    echo "Virtual environment already exists."
    source .venv/bin/activate
fi

# Run the Python script
echo "Running src/processFile_Local_AI.py..."
python src/processFile_Local_AI.py

echo "Script completed."