# 🚗 Analisi Dati US Used Cars con Big Data

Progetto di analisi ed elaborazione dati utilizzando tecnologie Big Data (Hadoop & Spark) sul dataset [US Used Cars](https://www.kaggle.com/datasets/austinreese/usa-cers-dataset) da Kaggle.

---

## ⚙️ Setup dell’Ambiente

Assicurarsi di avere correttamente installato e configurato Java, Hadoop e Spark. Controlla le variabili d’ambiente:

```bash
echo $JAVA_HOME       # Es: /usr/lib/jvm/java-1.11.0-openjdk-amd64
echo $HADOOP_HOME     # Es: /home/<utente>/hadoop-3.4.1
echo $SPARK_HOME      # Es: /home/<utente>/spark-3.5.5-bin-hadoop3
```

Per i test di performance (`performance_test.py`), crea un ambiente virtuale Python:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt --no-cache-dir
```

---

## 🚀 Avvio del Progetto

### 1. Avvia Hadoop
```bash
source init.sh
```

### 2. Scarica ed elabora il dataset
```bash
cd dataset
bash download.sh
bash generate_data.sh local[*]
```

### 🛑 Stop Hadoop
```bash
$HADOOP_HOME/sbin/stop-dfs.sh
```

---

## 📂 Esecuzione dei Job

Ogni modulo contiene uno script `run.sh` con 3 parametri richiesti:

1. **Job Name** (es. `job-1`)
2. **Dataset Name** (es. `data-20.0%`)
3. **Tipo di Esecuzione**: `local[*]` o `yarn`

Esempio per `spark-core`:

```bash
cd spark-core
bash run.sh job-1 data-20.0% local[*]
```

I risultati sono salvati su HDFS nella struttura:

```bash
/user/$USER/
├── data/
├── map-reduce/
├── spark-core/
└── spark-sql/
```

---

## 📊 Test di Performance

Esecuzione dei i test comparativi con:

```bash
bash launch_tests.sh local[*]
bash launch_tests.sh yarn
```

I risultati e i grafici dei tempi saranno salvati nella cartella `log/`.

---

## ☁️ Integrazione con AWS

1. **Recupera i dataset da HDFS**:

```bash
cd ./dataset/data
hdfs dfs -get /user/$USER/data/*.csv $(pwd)
```

2. **Comprimi i file**:
```bash
zip -r files.zip *.csv
```

3. **Invia al cluster AWS**:
```bash
scp files.zip hadoop@<aws-endpoint>:/path/to/put/files.zip
```

4. **Carica i dati su HDFS nel cluster AWS**:
```bash
bash aws.sh
```

Struttura finale dei dati su AWS:
```bash
dataset/
├── aws.sh
└── data/
    ├── data-1.0%.csv
    ├── data-20.0%.csv
    ├── data-50.0%.csv
    ├── data-70.0%.csv
    └── data_cleaned.csv
```

---