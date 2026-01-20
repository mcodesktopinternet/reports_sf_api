import os
import logging
from datetime import timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

from sf_auth import get_salesforce_token, get_auth_headers
from conectar_mysql import conectar_mysql, insert_dataframe_mysql_direct


# -----------------------------------------------------------------------------
# LOG
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("etl_sa_cancelados")


# -----------------------------------------------------------------------------
# HELPERS CONFIG
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
# PARAMS (via .env com defaults seguros)
# -----------------------------------------------------------------------------
SF_API_VERSION = os.getenv("SF_API_VERSION", "65.0").strip()
SF_LAST_N_DAYS = int(os.getenv("SA_HISTORY_LAST_N_DAYS", "2"))
BATCH_INSERT = int(os.getenv("BATCH_INSERT", "3000"))

MYSQL_TABLE = os.getenv("SA_CANCEL_TABLE", "service_appointment_cancelamentos").strip()


# -----------------------------------------------------------------------------
# QUERY SALESFORCE
# -----------------------------------------------------------------------------
SOQL_QUERY = f"""
SELECT
    Id,
    CreatedDate,
    ServiceAppointment.AppointmentNumber,
    Field,
    CreatedBy.Name,
    OldValue,
    NewValue
FROM ServiceAppointmentHistory
WHERE Field = 'Status'
  AND CreatedDate = LAST_N_DAYS:{SF_LAST_N_DAYS}
ORDER BY CreatedDate ASC
"""


# -----------------------------------------------------------------------------
# FUNÇÕES
# -----------------------------------------------------------------------------
def converter_data(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Retorna:
      - created_date (timezone Brasil -03:00 removendo tz)
      - created_date_utc (UTC sem tz)
    """
    dt_utc = pd.to_datetime(series, utc=True, errors="coerce")
    created_date = (dt_utc - timedelta(hours=3)).dt.tz_localize(None)
    created_date_utc = dt_utc.dt.tz_localize(None)
    return created_date, created_date_utc


def criar_tabela_mysql(conn, table_name: str):
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id VARCHAR(18) PRIMARY KEY,
            appointment_number VARCHAR(50),
            field_name VARCHAR(50),
            created_by VARCHAR(255),
            old_value VARCHAR(255),
            new_value VARCHAR(255),
            created_date DATETIME,
            created_date_utc DATETIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()


def iterar_salesforce(query: str, instance_url: str, headers: dict, api_version: str):
    """
    Itera em páginas usando REST query endpoint.
    """
    url = f"{instance_url}/services/data/v{api_version}/query"
    params = {"q": query}

    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=1200)
        resp.raise_for_status()
        data = resp.json()

        yield data.get("records", [])

        if not data.get("done", True):
            url = f"{instance_url}{data['nextRecordsUrl']}"
            params = None
        else:
            url = None


def etl_service_appointment_cancelado():
    load_dotenv()

    sf = get_sf_config()
    mysql = get_mysql_config()

    logger.info("Autenticando no Salesforce...")
    token_data = get_salesforce_token(
        domain=sf["SF_DOMAIN"],
        client_id=sf["SF_CLIENT_ID"],
        client_secret=sf["SF_CLIENT_SECRET"],
        username=sf["SF_USERNAME"],
        password=sf["SF_PASSWORD"],
    )
    headers = get_auth_headers(token_data)

    # melhor prática: usar instance_url do token
    instance_url = token_data.get("instance_url") or sf["SF_DOMAIN"]

    logger.info("Conectando ao MySQL (%s/%s)...", mysql["MYSQL_HOST"], mysql["MYSQL_DATABASE"])
    conn = conectar_mysql(
        host=mysql["MYSQL_HOST"],
        database=mysql["MYSQL_DATABASE"],
        user=mysql["MYSQL_USER"],
        password=mysql["MYSQL_PASSWORD"],
    )
    if not conn:
        raise RuntimeError("Erro ao conectar no MySQL")

    criar_tabela_mysql(conn, MYSQL_TABLE)

    total_cancelados = 0

    try:
        logger.info("Iniciando extração (streaming) - LAST_N_DAYS:%s | API v%s", SF_LAST_N_DAYS, SF_API_VERSION)

        for lote_api in iterar_salesforce(SOQL_QUERY, instance_url, headers, SF_API_VERSION):
            if not lote_api:
                continue

            df = pd.json_normalize(lote_api, sep="_")

            # remove colunas técnicas
            df = df.drop(columns=[c for c in df.columns if "attributes" in c], errors="ignore")

            # só cancelado
            if "NewValue" not in df.columns:
                continue

            df = df[df["NewValue"] == "Cancelado"]
            if df.empty:
                continue

            # datas
            df["created_date"], df["created_date_utc"] = converter_data(df["CreatedDate"])

            # renomeia
            df = df.rename(columns={
                "Id": "id",
                "ServiceAppointment_AppointmentNumber": "appointment_number",
                "Field": "field_name",
                "CreatedBy_Name": "created_by",
                "OldValue": "old_value",
                "NewValue": "new_value",
            })

            # garante schema
            wanted = [
                "id",
                "appointment_number",
                "field_name",
                "created_by",
                "old_value",
                "new_value",
                "created_date",
                "created_date_utc",
            ]
            for col in wanted:
                if col not in df.columns:
                    df[col] = None

            df = df[wanted].where(pd.notna(df), None)

            # inserção em lote
            for i in range(0, len(df), BATCH_INSERT):
                chunk = df.iloc[i:i + BATCH_INSERT]
                insert_dataframe_mysql_direct(chunk, MYSQL_TABLE, conn)
                total_cancelados += len(chunk)

            logger.info("Cancelados inseridos até agora: %s", f"{total_cancelados:,}")

            del df

    finally:
        conn.close()
        logger.info("ETL FINALIZADO | TOTAL CANCELADOS: %s", f"{total_cancelados:,}")


if __name__ == "__main__":
    etl_service_appointment_cancelado()
