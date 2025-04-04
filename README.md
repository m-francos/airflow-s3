Projeto de pipeline de dados usando o conceito de Lakehouse, com três camadas: landing, bronze, silver e gold. O pipeline é orquestrado com Apache Airflow.


- landing: arquivos de entrada em JSON.
- bronze: arquivos Parquet extraídos da landing.
- silver: dados limpos da bronze.
- gold: dataset final com dados tratados.
- scripts: scripts de transformação para cada camada.
- airflow/dags/: DAG usada para orquestrar o pipeline no Airflow.

## Como executar

1. Ative o ambiente virtual e inicie o Airflow:
   bash
   source airflow_venv/bin/activate
   airflow standalone
2. Inicie o Airflow:
   airflow standalone
3. Acesse a interface web do Airflow no navegador:
   Ative e rode a DAG lakehouse_pipeline
   Clique no switch para ativar
   Depois clique em “Trigger DAG” (botão de play) para executar
