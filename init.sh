#!/bin/bash

# Se la variabile d'ambiente ROOT_DIR non è impostata (stringa vuota),
# allora viene impostata alla directory corrente (pwd = print working directory)
if [ -z "$ROOT_DIR" ]; then
    export ROOT_DIR=$(pwd)
fi

# --- Avvio di Hadoop DFS ---

# Ferma eventuali servizi Hadoop DFS attivi
$HADOOP_HOME/sbin/stop-dfs.sh

# Rimuove tutti i file temporanei dal sistema
rm -rf /tmp/*

# Format del NameNode HDFS
$HADOOP_HOME/bin/hdfs namenode -format

# Avvia i servizi DFS di Hadoop (NameNode, DataNodes)
$HADOOP_HOME/sbin/start-dfs.sh