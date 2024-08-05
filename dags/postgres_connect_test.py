import psycopg2

# Parâmetros de conexão
conn = psycopg2.connect(
    dbname="airflow_dw",
    user="airflow",
    password="airflow",
    host="localhost",
    port="5432"
)

# Cria um cursor
cursor = conn.cursor()

# Executa uma consulta SQL
cursor.execute("SELECT * FROM api_data_dw")

# Recupera todos os resultados
records = cursor.fetchall()

# Fecha a conexão
cursor.close()
conn.close()

# Exibe os resultados
for record in records:
    print(record)
