Lakehouse Pipeline com Airflow

Pipeline de dados com Airflow usando as camadas landing, bronze, silver e gold.

    landing: arquivos de entrada em JSON

    bronze: arquivos Parquet extraídos da landing

    silver: dados limpos da bronze

    gold: dataset final com dados tratados

Como rodar

    Clone o repositório:
    git clone https://github.com/m-francos/lakehouse-airflow.git
    cd lakehouse-airflow

    Ative o ambiente virtual:
    source airflow_venv/bin/activate

    Rode o script de inicialização:
    ./start.sh

    Acesse o Airflow em http://localhost:8080
    Usuário: admin
    Senha: admin

    Ative e execute a DAG chamada lakehouse_pipeline
