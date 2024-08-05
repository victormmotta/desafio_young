# Projeto de Ingestão de Dados com Apache Airflow

## Descrição

Este projeto consiste em ingerir dados de uma API e organizá-los em um banco de dados relacional. Utilizamos Apache Airflow para orquestrar o processo de ETL (Extract, Transform, Load), rodando em um ambiente Docker. O objetivo é que o projeto funcione ao rodar o comando `docker-compose up` em qualquer máquina que contenha os requisitos necessários.

## Requisitos

Para executar este projeto, você precisará dos seguintes componentes instalados na sua máquina:

1. **Docker Desktop**: Para gerenciar os containers Docker.
2. **WSL 2** (para usuários Windows): Para fornecer um ambiente Linux no Windows.
3. **DBeaver**: Para acessar e consultar os dados no banco de dados.

### Instalação dos Requisitos

#### Docker Desktop

Você pode baixar e instalar o Docker Desktop a partir do [site oficial do Docker](https://www.docker.com/products/docker-desktop).

#### WSL 2 (para usuários Windows)

Siga as instruções no [site oficial da Microsoft](https://docs.microsoft.com/en-us/windows/wsl/install) para instalar e configurar o WSL 2.

#### DBeaver

Você pode baixar e instalar o DBeaver a partir do [site oficial do DBeaver](https://dbeaver.io/download/).

## Passo a Passo para Execução

### 1. Clonar o Repositório

Clone este repositório para a sua máquina local:

```bash
git clone https://github.com/victormmotta/desafio_young.git
```

### 2. Configurar Variáveis de Ambiente
Certifique-se de que as variáveis de ambiente necessárias estão configuradas corretamente no arquivo .env. Este arquivo deve estar no mesmo diretório que o docker-compose.yml.

### 3. Executar o Docker Compose
Para iniciar os serviços do Airflow e do banco de dados, execute:

```bash
docker-compose up -d
```

### 4. Acessar a Interface do Airflow
Abra seu navegador e acesse a interface do Airflow:

```bash
http://localhost:8080
```

Use as seguintes credenciais para login:

Usuário: airflow

Senha: airflow

### 5. Criar Conexões no Airflow
#### 5.1. Criar Conexão para o Banco de Dados postgres
No Airflow, vá para a seção Admin -> Connections e adicione uma nova conexão com as seguintes informações:

Conn Id: postgres_default

Conn Type: Postgres

Host: postgres

Schema: airflow

Login: airflow

Password: airflow

Port: 5432

#### 5.2. Criar Conexão para o Banco de Dados postgres_dw
No Airflow, vá para a seção Admin -> Connections e adicione uma nova conexão com as seguintes informações:

Conn Id: postgres_dw

Conn Type: Postgres

Host: postgres_dw

Schema: airflow_dw

Login: airflow

Password: airflow

Port: 5432

### 6. Executar a DAG
No Airflow, vá para a aba DAGs e ative a DAG ingest_api_data. Em seguida, acione manualmente a execução da DAG para garantir que tudo esteja funcionando corretamente.

### 7. Verificar os Dados no DBeaver
Abra o DBeaver e crie uma nova conexão para o banco de dados postgres_dw:

Host: localhost
Port: 5432
Database: airflow_dw
Username: airflow
Password: airflow
Após conectar, você pode verificar os dados ingeridos no banco de dados airflow_dw.

## Considerações Finais
Este projeto configura um ambiente de ingestão de dados utilizando Apache Airflow e Docker. A cada execução, os dados são coletados de uma API e armazenados em um banco de dados PostgreSQL.
