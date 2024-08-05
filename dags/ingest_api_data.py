from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
import requests
import json
from datetime import timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'MS_sIX2Rm@somosyoung.com.br',
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ingest_api_data',
    default_args=default_args,
    description='Ingest data from API and transfer to DW',
    schedule_interval=timedelta(minutes=10),
    start_date=days_ago(1),
    catchup=False,
)

def check_db():
    # Conexão com o banco de dados de Data Warehouse
    pg_dw_hook = PostgresHook(postgres_conn_id='postgres_dw')
    conn_dw = pg_dw_hook.get_conn()
    cursor_dw = conn_dw.cursor()
    cursor_dw.execute("""
        CREATE TABLE IF NOT EXISTS api_data_dw (
            id SERIAL PRIMARY KEY,
            idahoy INTEGER,
            id_campaign INTEGER,
            campaign VARCHAR,
            company VARCHAR,
            user_type VARCHAR,
            to_email VARCHAR,
            mailer VARCHAR,
            subject VARCHAR,
            sent_at TIMESTAMP,
            token VARCHAR,
            opened_at TIMESTAMP,
            clicked_at TIMESTAMP,
            raw_data JSONB
        )
    """)
    conn_dw.commit()
    cursor_dw.close()
    conn_dw.close()

    # Conexão com o banco de dados do Airflow
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_data (
            id SERIAL PRIMARY KEY,
            idahoy INTEGER,
            id_campaign INTEGER,
            campaign VARCHAR,
            company VARCHAR,
            user_type VARCHAR,
            to_email VARCHAR,
            mailer VARCHAR,
            subject VARCHAR,
            sent_at TIMESTAMP,
            token VARCHAR,
            opened_at TIMESTAMP,
            clicked_at TIMESTAMP,
            raw_data JSONB
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

def ingest_data():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    last_idahoy = 1811657

    while True:
        try:
            logger.info(f"Fazendo requisição para a API com idahoy = {last_idahoy}")
            response = requests.post(
                'https://app.alunos.me/api/ahoy_viewer_ti',
                headers={'Content-Type': 'application/json'},
                json={
                    'token': 'G5n2w1lm*eJF$UukF5c^bN5Re#',
                    'idahoy': last_idahoy
                },
                auth=('MS_sIX2Rm@somosyoung.com.br', 'ty8IvsmHa3Sdbk01')
            )
            response.raise_for_status()

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            break
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            break
        else:
            logger.info("Requisição bem-sucedida")

        data = response.json()

        if not data:
            logger.info("Nenhum dado retornado pela API. Encerrando o loop.")
            break

        logger.info(f"Número de registros recebidos: {len(data)}")

        for record in data:
            cursor.execute("""
                INSERT INTO api_data (idahoy, id_campaign, campaign, company, user_type, to_email, mailer, subject, sent_at, token, opened_at, clicked_at, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (record['id'], record['id_campaign'], record['campaign'], record['company'], record['user_type'], record['to'], record['mailer'], record['subject'], record['sent_at'], record['token'], record['opened_at'], record['clicked_at'], json.dumps(record)))

        last_idahoy = max(record['id'] for record in data)
        logger.info(f"Atualizando last_idahoy para: {last_idahoy}")

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Dados inseridos com sucesso e conexão com o banco de dados fechada.")

def transfer_to_dw():
    # Conexão com o banco de dados do Airflow
    src_pg_hook = PostgresHook(postgres_conn_id='postgres')
    src_conn = src_pg_hook.get_conn()
    src_cursor = src_conn.cursor()

    # Conexão com o banco de dados de Data Warehouse
    dest_pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    dest_conn = dest_pg_hook.get_conn()
    dest_cursor = dest_conn.cursor()

    # Seleciona dados do banco de dados do Airflow
    src_cursor.execute("SELECT * FROM api_data")
    rows = src_cursor.fetchall()

    # Insere os dados no banco de dados de Data Warehouse
    for row in rows:
        dest_cursor.execute("""
            INSERT INTO api_data_dw (idahoy, id_campaign, campaign, company, user_type, to_email, mailer, subject, sent_at, token, opened_at, clicked_at, raw_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], json.dumps(row[13])
        ))

    # Confirma as transações e fecha as conexões
    dest_conn.commit()
    src_cursor.close()
    src_conn.close()
    dest_cursor.close()
    dest_conn.close()

check_db_task = PythonOperator(
    task_id='check_db',
    python_callable=check_db,
    dag=dag,
)

ingest_data_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

transfer_to_dw_task = PythonOperator(
    task_id='transfer_to_dw',
    python_callable=transfer_to_dw,
    dag=dag,
)

check_db_task >> ingest_data_task >> transfer_to_dw_task
