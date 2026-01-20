import os
import sys
import time
import logging
from datetime import datetime, timedelta, date

import pandas as pd
from dotenv import load_dotenv

from sf_auth import get_salesforce_token, get_auth_headers
from sf_query import get_all_query_results
from conectar_mysql import conectar_mysql


# -----------------------------------------------------------------------------
# LOG
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl_sa_history_upsert")


# -----------------------------------------------------------------------------
# CONFIG HELPERS
# -----------------------------------------------------------------------------
def require_env(*keys: str) -> dict:
    values = {}
    missing = []
    for k in keys:
        v = os.getenv(k)
        if v is None or str(v).strip() == "":
            missing.append(k)
        else:
            values[k] = v
    if missing:
        raise RuntimeError(f"Variáveis obrigatórias ausentes no .env: {', '.join(missing)}")
    return values


def get_sf_config() -> dict:
    return require_env(
        "SF_DOMAIN",
        "SF_CLIENT_ID",
        "SF_CLIENT_SECRET",
        "SF_USERNAME",
        "SF_PASSWORD",
    )


def get_mysql_config() -> dict:
    return require_env(
        "MYSQL_HOST",
        "MYSQL_DATABASE",
        "MYSQL_USER",
        "MYSQL_PASSWORD",
    )


# -----------------------------------------------------------------------------
# PARAMETROS (via .env)
# -----------------------------------------------------------------------------
TABLE_NAME = os.getenv("SA_HISTORY_TABLE", "service_appointment_history").strip()
TZ_LOCAL = os.getenv("TZ_LOCAL", "America/Sao_Paulo").strip()

# dias que o job roda (por padrão: ontem e hoje)
PROCESS_TODAY = os.getenv("PROCESS_TODAY", "1").strip()  # "1" ou "0"
PROCESS_YESTERDAY = os.getenv("PROCESS_YESTERDAY", "1").strip()

# sleep entre dias
SLEEP_BETWEEN_DAYS_SEC = float(os.getenv("SLEEP_BETWEEN_DAYS_SEC", "1"))

MAPA_TRADUCAO_CAMPOS = {
    "MotivoDoCancelamento__c": "Motivo do Cancelamento",
    "SuspensionReason__c": "Motivo da Suspensão",
    "Reschedule_Reason_SA__c": "Motivo de REAGENDAMENTO",
    "RescheduleReasonSAHistory__c": "Motivo de REAGENDAMENTO",
    "TechnicianName__c": "Nome do Técnico",
    "SchedStartTime": "Início Agendado",
}

FIELDS_WHITELIST = tuple(MAPA_TRADUCAO_CAMPOS.keys())


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def formatar_data_mista(valor):
    if not isinstance(valor, str):
        return str(valor)

    if len(valor) > 10 and len(valor) >= 5 and valor[4] == "-" and "T" in valor:
        try:
            dt = pd.to_datetime(valor, utc=True, errors="coerce")
            if pd.isna(dt):
                return valor
            # converte pra tz local e remove tz
            dt_local = dt.tz_convert(TZ_LOCAL).tz_localize(None)
            return dt_local.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return valor

    return valor


def upsert_dados_mysql(df: pd.DataFrame, conn) -> bool:
    if df.empty:
        logger.info("   > [MySQL] DataFrame vazio. Nada a inserir.")
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
        logger.info("   > [UPSERT] Sucesso: %s registros processados.", cursor.rowcount)
        return True
    except Exception as e:
        logger.error("   > [ERRO SQL] Falha no Upsert: %s", e)
        conn.rollback()
        return False
    finally:
        cursor.close()


def sf_login() -> tuple[str, dict]:
    sf = get_sf_config()

    logger.info("Autenticando no Salesforce...")
    token_data = get_salesforce_token(
        domain=sf["SF_DOMAIN"],
        client_id=sf["SF_CLIENT_ID"],
        client_secret=sf["SF_CLIENT_SECRET"],
        username=sf["SF_USERNAME"],
        password=sf["SF_PASSWORD"],
    )
    headers = get_auth_headers(token_data)
    instance_url = token_data.get("instance_url") or sf["SF_DOMAIN"]
    return instance_url, headers


def mysql_connect():
    mysql = get_mysql_config()
    logger.info("Conectando no MySQL (%s/%s)...", mysql["MYSQL_HOST"], mysql["MYSQL_DATABASE"])
    conn = conectar_mysql(
        host=mysql["MYSQL_HOST"],
        database=mysql["MYSQL_DATABASE"],
        user=mysql["MYSQL_USER"],
        password=mysql["MYSQL_PASSWORD"],
    )
    if not conn:
        raise RuntimeError("Erro conexão MySQL.")
    return conn


def processar_dia(data_referencia: date, instance_url: str, headers: dict, conn) -> bool:
    try:
        start_dt = data_referencia.strftime("%Y-%m-%dT00:00:00.000-03:00")
        end_dt = (data_referencia + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00.000-03:00")

        logger.info("   > [ETL] Buscando dados de %s...", data_referencia)

        fields_list = ", ".join([f"'{f}'" for f in FIELDS_WHITELIST])
        query = f"""
        SELECT
            Id,
            ServiceAppointmentId,
            ServiceAppointment.AppointmentNumber,
            CreatedDate,
            Field,
            OldValue,
            NewValue,
            CreatedById,
            CreatedBy.Name
        FROM ServiceAppointmentHistory
        WHERE Field IN ({fields_list})
          AND CreatedDate >= {start_dt}
          AND CreatedDate <  {end_dt}
        """

        resultado = get_all_query_results(
            instance_url=instance_url,
            auth_headers=headers,
            query=query,
        )

        if not resultado:
            logger.info("   > [INFO] Nenhum registro encontrado para %s.", data_referencia)
            return True

        df = pd.json_normalize(resultado, sep="_")
        df = df.drop(columns=[c for c in df.columns if "attributes" in c], errors="ignore")

        # traduz campo
        if "Field" in df.columns:
            df["Field"] = df["Field"].map(MAPA_TRADUCAO_CAMPOS).fillna(df["Field"])

        colunas_de_para = {
            "Id": "id",
            "ServiceAppointmentId": "appointment_id",
            "ServiceAppointment_AppointmentNumber": "appointment_number",
            "CreatedDate": "created_date",
            "Field": "field_name",
            "OldValue": "old_value",
            "NewValue": "new_value",
            "CreatedById": "created_by_id",
            "CreatedBy_Name": "created_by_name",
        }
        df = df.rename(columns=colunas_de_para)

        # created_date -> tz local sem tzinfo
        if "created_date" in df.columns:
            dt_utc = pd.to_datetime(df["created_date"], utc=True, errors="coerce")
            df["created_date"] = dt_utc.dt.tz_convert(TZ_LOCAL).dt.tz_localize(None)

        # old/new format
        if "old_value" in df.columns:
            df["old_value"] = df["old_value"].astype(str).apply(formatar_data_mista)
        if "new_value" in df.columns:
            df["new_value"] = df["new_value"].astype(str).apply(formatar_data_mista)

        cols_finais = [
            "id",
            "appointment_id",
            "appointment_number",
            "created_date",
            "field_name",
            "old_value",
            "new_value",
            "created_by_id",
            "created_by_name",
        ]
        for col in cols_finais:
            if col not in df.columns:
                df[col] = None

        df_final = df[cols_finais].where(pd.notna(df[cols_finais]), None)

        return upsert_dados_mysql(df_final, conn)

    except Exception as e:
        logger.error("   [CRITICAL] Falha ao processar dia %s: %s", data_referencia, e)
        return False


def job_etl():
    logger.info("=" * 60)
    logger.info("INICIANDO ROTINA: %s", datetime.now())
    logger.info("=" * 60)

    instance_url, headers = sf_login()
    conn = mysql_connect()

    try:
        hoje = date.today()
        datas = []

        if PROCESS_YESTERDAY == "1":
            datas.append(hoje - timedelta(days=1))
        if PROCESS_TODAY == "1":
            datas.append(hoje)

        for data_atual in datas:
            logger.info("--- Processando data: %s ---", data_atual)
            processar_dia(data_atual, instance_url, headers, conn)
            time.sleep(SLEEP_BETWEEN_DAYS_SEC)

    finally:
        conn.close()
        logger.info("ROTINA FINALIZADA")


if __name__ == "__main__":
    load_dotenv()
    logger.info("--- SCRIPT INICIADO ---")
    job_etl()
    logger.info("--- SCRIPT FINALIZADO ---")
