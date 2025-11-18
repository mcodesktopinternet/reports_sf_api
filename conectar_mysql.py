import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional
import os
from dotenv import load_dotenv


def conectar_mysql(
    host: str = "localhost",
    port: int = 3306,
    database: str = None,
    user: str = None,
    password: str = None,
    use_env: bool = True
) -> Optional[mysql.connector.connection.MySQLConnection]:
    """
    Conecta ao banco de dados MySQL.
    
    Args:
        host: Endereço do servidor MySQL
        port: Porta do servidor MySQL
        database: Nome do banco de dados
        user: Nome do usuário
        password: Senha do usuário
        use_env: Se True, tenta carregar credenciais do arquivo .env
    
    Returns:
        Conexão MySQL ou None se falhar
    """
    try:
        if use_env:
            load_dotenv()
            host = os.getenv('MYSQL_HOST', host)
            port = int(os.getenv('MYSQL_PORT', port))
            database = os.getenv('MYSQL_DATABASE', database)
            user = os.getenv('MYSQL_USER', user)
            password = os.getenv('MYSQL_PASSWORD', password)
        
        connection = mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            autocommit=True
        )
        print(f"Conectado com sucesso ao MySQL: {host}:{port}/{database}")
        
        return connection
        
    except mysql.connector.Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None
    except Exception as e:
        print(f"Erro inesperado: {e}")
        return None

def insert_dataframe_mysql(
    df: pd.DataFrame,
    table_name: str,
    connection: mysql.connector.connection.MySQLConnection,
    if_exists: str = 'append',
    index: bool = False,
    method: str = 'multi',
    chunksize: int = 1000
) -> bool:
    """
    Insere um DataFrame no MySQL usando SQLAlchemy.
    
    Args:
        df: DataFrame pandas para inserir
        table_name: Nome da tabela de destino
        connection: Conexão MySQL ativa
        if_exists: Comportamento se tabela existir ('fail', 'replace', 'append')
        index: Se deve incluir o índice do DataFrame
        method: Método de inserção ('multi' para múltiplas linhas)
        chunksize: Tamanho dos chunks para inserção
    
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        # Criar engine SQLAlchemy a partir da conexão MySQL
        engine = create_engine(
            f"mysql+mysqlconnector://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
        )
        
        # Inserir DataFrame
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=index,
            method=method,
            chunksize=chunksize
        )
        
        print(f"Dados inseridos com sucesso na tabela {table_name}")
        return True
        
    except Exception as e:
        print(f"Erro ao inserir DataFrame na tabela {table_name}: {e}")
        return False

def insert_dataframe_mysql_direct(
    df: pd.DataFrame,
    table_name: str,
    connection: mysql.connector.connection.MySQLConnection,
    batch_size: int = 1000
) -> bool:
    """
    Insere um DataFrame no MySQL usando cursor direto (mais rápido para grandes volumes).
    
    Args:
        df: DataFrame pandas para inserir
        table_name: Nome da tabela de destino
        connection: Conexão MySQL ativa
        batch_size: Tamanho do lote para inserção
    
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = connection.cursor()
        
        # Gerar colunas da tabela
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        
        # Query de inserção
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        print(insert_query)
        
        # Função para tratar valores None e strings vazias como NULL
        def clean_value(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            return val
        
        # Converter DataFrame para lista de tuplas, tratando valores None e strings vazias
        data_to_insert = []
        for row in df.values:
            cleaned_row = [clean_value(val) for val in row]
            data_to_insert.append(tuple(cleaned_row))
        
        # Inserir em lotes
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            
            print(f"Inseridos {len(batch)} registros (lote {i//batch_size + 1})")
        
        cursor.close()
        print(f"Total de {len(data_to_insert)} registros inseridos na tabela {table_name}")
        return True
        
    except Exception as e:
        print(f"Erro ao inserir DataFrame na tabela {table_name}: {e}")
        return False

def testar_conexao(connection: mysql.connector.connection.MySQLConnection) -> bool:
    """
    Testa se a conexão MySQL está ativa.
    
    Args:
        connection: Conexão MySQL
    
    Returns:
        True se conexão ativa, False caso contrário
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        return result[0] == 1
    except Exception as e:
        print(f"Erro ao testar conexão: {e}")
        return False
