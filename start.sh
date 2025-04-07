#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export AIRFLOW__CORE__DAGS_FOLDER="$DIR/airflow/dags"

airflow standalone

