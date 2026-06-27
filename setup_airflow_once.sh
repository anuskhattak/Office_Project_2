#!/usr/bin/env bash
set -euo pipefail

cd /mnt/e/Office_Project_2

if [ ! -d airflow_venv ]; then
  python3 -m venv airflow_venv
fi

source airflow_venv/bin/activate

export AIRFLOW_HOME=/mnt/e/Office_Project_2/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=/mnt/e/Office_Project_2/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

if ! command -v java >/dev/null 2>&1 || [ ! -d "$JAVA_HOME" ]; then
  apt update
  apt install -y openjdk-17-jdk
fi

if ! python -c "import airflow" >/dev/null 2>&1; then
  pip install --default-timeout=300 --retries 10 apache-airflow==2.9.3
fi

pip install --default-timeout=300 --retries 10 pyspark==3.5.3 numpy matplotlib python-dotenv

python -c "import pyspark, numpy, matplotlib; print('deps ok')"
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.appName('airflow_spark_check').getOrCreate(); print('spark ok'); s.stop()"

if ! airflow users list | grep -q '^admin[[:space:]]'; then
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

echo
echo "Airflow is starting."
echo "Open http://localhost:8080 and login with admin / admin."
echo "Trigger DAG: healthcare_messy_to_gold"
echo

airflow standalone
