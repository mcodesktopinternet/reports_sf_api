# ==============================================================================
# 1. IMPORTAÇÕES
# ==============================================================================
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import pytz
# import schedule  ### REMOVIDO ###
import logging
import sys
import os

# Dependências locais
from sf_auth import get_salesforce_token, get_auth_headers
from sf_query import get_all_query_results
from conectar_mysql import conectar_mysql 

# ==============================================================================
# 2. CONFIGURAÇÃO DE LOG (APENAS NO CONSOLE)
# ==============================================================================
# Configura o logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Limpa handlers anteriores se houver (evita duplicação)
if logger.hasHandlers():
    logger.handlers.clear()

# Handler para TELA (Console)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(stream_handler)

# ==============================================================================
# 3. CONFIGURAÇÕES DE BANCO E VARIÁVEIS
# ==============================================================================
DB_HOST = "172.29.5.3"
DB_NAME = "db_Melhoria_continua_operacoes"
DB_USER = "Danilo.Vargas"
DB_PASS = "xO09E6MHhVf&£M{$4"
TABLE_NAME = "service_appointment_history"

MAPA_TRADUCAO_CAMPOS = {
    'MotivoDoCancelamento__c': 'Motivo do Cancelamento',
    'SuspensionReason__c': 'Motivo da Suspensão',
    'Reschedule_Reason_SA__c': 'Motivo de REAGENDAMENTO',
    'RescheduleReasonSAHistory__c': 'Motivo de REAGENDAMENTO',
    'TechnicianName__c': 'Nome do Técnico',
    'SchedStartTime': 'Início Agendado'
}

# ==============================================================================
# 4. FUNÇÃO AUXILIAR DE FORMATAÇÃO
# ==============================================================================
def formatar_data_mista(valor):
    if not isinstance(valor, str):
        return str(valor)

    if len(valor) > 10 and valor[4] == '-' and 'T' in valor:
        try:
            dt = pd.to_datetime(valor)
            if pd.isna(dt):
                return valor
            if dt.tzinfo is not None:
                dt = dt.tz_convert('America/Sao_Paulo')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return valor
    return valor

# ==============================================================================
# 5. FUNÇÃO DE UPSERT (LOGADA)
# ==============================================================================
def upsert_dados_mysql(df, conn):
    if df.empty:
        logging.info("   > [MySQL] DataFrame vazio. Nada a inserir.")
        return True

    cursor = conn.cursor()
    
    sql = f"""
    INSERT INTO {TABLE_NAME} (
        id, appointment_id, appointment_number, created_date, 
        field_name, old_value, new_value, created_by_id, created_by_name
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        appointment_id = VALUES(appointment_id),
        appointment_number = VALUES(appointment_number),
        created_date = VALUES(created_date),
        field_name = VALUES(field_name),
        old_value = VALUES(old_value),
        new_value = VALUES(new_value),
        created_by_id = VALUES(created_by_id),
        created_by_name = VALUES(created_by_name);
    """

    df = df.where(pd.notnull(df), None)
    dados = df.values.tolist()

    try:
        cursor.executemany(sql, dados)
        conn.commit()
        logging.info(f"   > [UPSERT] Sucesso: {cursor.rowcount} registros processados (Inseridos/Atualizados).")
        cursor.close()
        return True
    except Exception as e:
        logging.error(f"   > [ERRO SQL] Falha no Upsert: {e}")
        conn.rollback()
        cursor.close()
        return False

# ==============================================================================
# 6. FUNÇÕES DO ETL
# ==============================================================================

def get_sf_connection():
    domain = "https://desktopsa.my.salesforce.com"
    client_id = "3MVG99gP.VbJma8WslD5o_qMP_J4iVg1FUTtQzWQ4_TxpBoZWi6MyFHtxCZMZFOCk2FsJbw2wNynhmaD7c8Uv"
    client_secret = "E31D29879301886FE62D60D79898EE335B488E0A37E4DDB9874D12F12C29719E"
    username = "plan.relatorios@desktop.net.br"
    password = "PlanRelatorios@2025PwdjW2bnaqa3Z8O7KwuqVoLp"

    logging.info("Autenticando no Salesforce...")
    token_data = get_salesforce_token(domain, client_id, client_secret, username, password)
    return get_auth_headers(token_data)

def processar_dia(data_referencia, headers, conn):
    try:
        start_dt = data_referencia.strftime("%Y-%m-%dT00:00:00.000-03:00")
        end_date_obj = data_referencia + timedelta(days=1)
        end_dt = end_date_obj.strftime("%Y-%m-%dT00:00:00.000-03:00")
        
        logging.info(f"   > [ETL] Buscando dados de {data_referencia}...")

        query = f"""
        SELECT 
            Id, ServiceAppointmentId, ServiceAppointment.AppointmentNumber, 
            CreatedDate, Field, OldValue, NewValue, CreatedById, CreatedBy.Name
        FROM ServiceAppointmentHistory 
        WHERE Field IN ('MotivoDoCancelamento__c', 'SuspensionReason__c', 'Reschedule_Reason_SA__c', 'RescheduleReasonSAHistory__c', 'TechnicianName__c', 'SchedStartTime') 
        AND CreatedDate >= {start_dt} 
        AND CreatedDate < {end_dt}
        """

        resultado = get_all_query_results(instance_url="https://desktopsa.my.salesforce.com", auth_headers=headers, query=query)

        if not resultado:
            logging.info(f"   > [INFO] Nenhum registro encontrado para {data_referencia}.")
            return True 

        df = pd.json_normalize(resultado, sep="_")
        df.fillna("", inplace=True)
        df = df.drop(columns=[col for col in df.columns if 'attributes' in col], errors='ignore')

        df['Field'] = df['Field'].map(MAPA_TRADUCAO_CAMPOS).fillna(df['Field'])

        colunas_de_para = {
            'Id': 'id', 'ServiceAppointmentId': 'appointment_id', 'ServiceAppointment_AppointmentNumber': 'appointment_number',
            'CreatedDate': 'created_date', 'Field': 'field_name', 'OldValue': 'old_value',
            'NewValue': 'new_value', 'CreatedById': 'created_by_id', 'CreatedBy_Name': 'created_by_name'
        }
        df.rename(columns=colunas_de_para, inplace=True)

        if 'created_date' in df.columns:
            df['created_date'] = pd.to_datetime(df['created_date'], utc=True)
            df['created_date'] = df['created_date'].dt.tz_convert('America/Sao_Paulo')
            df['created_date'] = df['created_date'].dt.tz_localize(None)

        if 'old_value' in df.columns: df['old_value'] = df['old_value'].astype(str).apply(formatar_data_mista)
        if 'new_value' in df.columns: df['new_value'] = df['new_value'].astype(str).apply(formatar_data_mista)

        cols_finais = ['id', 'appointment_id', 'appointment_number', 'created_date', 'field_name', 'old_value', 'new_value', 'created_by_id', 'created_by_name']
        cols_para_inserir = [c for c in cols_finais if c in df.columns]
        df_final = df[cols_para_inserir]

        return upsert_dados_mysql(df_final, conn)

    except Exception as e:
        logging.error(f"   [CRITICAL] Falha ao processar dia {data_referencia}: {e}")
        return False

# ==============================================================================
# 7. FUNÇÃO PRINCIPAL (JOB)
# ==============================================================================
def job_etl():
    logging.info("="*60)
    logging.info(f"INICIANDO ROTINA DE EXECUÇÃO ÚNICA: {datetime.now()}")
    logging.info("="*60)
    
    try:
        headers = get_sf_connection()
    except Exception as e:
        logging.critical(f"Erro Autenticação: {e}")
        return

    conn = conectar_mysql(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    if not conn:
        logging.critical("Erro conexão MySQL.")
        return

    hoje = date.today()
    ontem = hoje - timedelta(days=1)
    datas = [ontem, hoje]

    for data_atual in datas:
        logging.info(f"--- Processando data: {data_atual} ---")
        processar_dia(data_atual, headers, conn)
        time.sleep(1)

    conn.close()
    logging.info("ROTINA FINALIZADA")

# ==============================================================================
# 8. EXECUÇÃO ### ALTERADO PARA EXECUÇÃO ÚNICA ###
# ==============================================================================
if __name__ == "__main__":
    logging.info("--- SCRIPT INICIADO ---")
    
    # Executa a função principal uma vez
    job_etl() 
    
    logging.info("--- SCRIPT FINALIZADO ---")