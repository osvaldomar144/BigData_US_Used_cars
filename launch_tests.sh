#!/usr/bin

# Controlla che il primo argomento ($1) sia "local[*]" oppure "yarn"
if [ "$1" != "local[*]" ] && [ "$1" != "yarn" ]; then
    echo "Error: Invalid master argument. Use 'local[*]' or 'yarn'."
    exit 1
fi

# Esegue lo script Python 'performance_test.py' con due diversi job ID e il master ricevuto come argomento
python3 performance_test.py job-1 $1 --fraction "0.01 0.2 0.5 0.7"
python3 performance_test.py job-2 $1 --fraction "0.01 0.2 0.5 0.7"