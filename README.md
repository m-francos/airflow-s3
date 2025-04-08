Lakehouse Pipeline com Airflow

Pipeline de dados com Airflow usando as camadas landing, bronze, silver e gold.

    landing: arquivos de entrada em JSON
    bronze: arquivos Parquet extraídos da landing
    silver: dados limpos da bronze
    gold: dataset final com dados tratados

Como rodar

    Windows
    
        # Clone o repositório
            git clone https://github.com/m-francos/lakehouse-airflow.git
            cd lakehouse-airflow

        # Crie o ambiente virtual
            python -m venv airflow_venv

        # Ative o ambiente virtual
            airflow_venv\Scripts\activate

        # Instale os pacotes
            pip install -r requirements.txt

        # Rode o script de inicialização
            ./start.sh

        # Acesse o Airflow
            http://localhost:8080

        # Ative e execute a DAG chamada lakehouse_pipeline

    
    MacOS & Linux

        # Clone o repositório
            git clone https://github.com/m-francos/lakehouse-airflow.git
            cd lakehouse-airflow

        # Crie o ambiente virtual
            python3 -m venv airflow_venv

        # Ative o ambiente virtual
            source airflow_venv/bin/activate

        # Instale os pacotes
            pip install -r requirements.txt

        # Rode o script de inicialização
            ./start.sh

        # Acesse o Airflow
            http://localhost:8080

        # Ative e execute a DAG chamada lakehouse_pipeline
