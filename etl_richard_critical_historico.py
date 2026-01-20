import os
import logging
from dotenv import load_dotenv

import pandas as pd

from sf_auth import get_salesforce_token, get_auth_headers
from sf_query import get_all_query_results
from convert_timestamp_column import convert_timestamp_column
from conectar_mysql import conectar_mysql, insert_dataframe_mysql_direct


# -----------------------------------------------------------------------------
# LOG
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("etl_workorderhistory")


# -----------------------------------------------------------------------------
# HELPERS
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
# SOQL
# -----------------------------------------------------------------------------
SOQL = """
SELECT
    Id,
    WorkOrder.WorkOrderNumber,
    Field,
    CreatedBy.Name,
    CreatedDate,
    OldValue,
    NewValue
FROM WorkOrderHistory
WHERE (Field = 'Priority' OR Field = 'Priority2__c')
  AND CreatedDate >= {CREATED_FROM}
"""


# -----------------------------------------------------------------------------
# TRANSFORM
# -----------------------------------------------------------------------------
TIMESTAMP_COLS = ["CreatedDate"]

COLUNAS_DE_PARA = {
    "Id": "id",
    "WorkOrder_WorkOrderNumber": "numero_ordem_trabalho",
    "Field": "campo",
    "CreatedBy_Name": "criado_por",
    "CreatedDate": "data_criacao",
    "OldValue": "valor_anterior",
    "NewValue": "valor_novo",
}


def normalize_df(records: list[dict]) -> pd.DataFrame:
    df = pd.json_normalize(records, sep="_")

    # remove colunas "attributes"
    df = df.drop(columns=[c for c in df.columns if "attributes_" in c], errors="ignore")

    # timestamps
    for col in TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = convert_timestamp_column(df[col])

    # renomeia
    df = df.rename(columns=COLUNAS_DE_PARA)

    # garante vazio
    df = df.astype(object).fillna("")
    return df


# -----------------------------------------------------------------------------
# ETL
# -----------------------------------------------------------------------------
def etl_workorderhistory_priority():
    load_dotenv()

    sf = get_sf_config()
    mysql = get_mysql_config()

    created_from = os.getenv("SF_WOH_CREATED_FROM", "2025-08-11T00:00:00.000-03:00")
    soql = SOQL.format(CREATED_FROM=created_from)

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

    logger.info("Consultando WorkOrderHistory... (CreatedDate >= %s)", created_from)
    records = get_all_query_results(
        instance_url=instance_url,
        auth_headers=headers,
        query=soql,
    )

    logger.info("Normalizando retorno... (%s registros)", len(records))
    df = normalize_df(records)

    logger.info("Conectando no MySQL (%s/%s)...", mysql["MYSQL_HOST"], mysql["MYSQL_DATABASE"])
    conn = conectar_mysql(
        host=mysql["MYSQL_HOST"],
        database=mysql["MYSQL_DATABASE"],
        user=mysql["MYSQL_USER"],
        password=mysql["MYSQL_PASSWORD"],
    )

    if not conn:
        raise RuntimeError("Não foi possível conectar no MySQL. Verifique credenciais e permissão.")

    try:
        cursor = conn.cursor()

        logger.info("Limpando tabela historico_ordem_servico_casos_criticos...")
        cursor.execute("TRUNCATE TABLE historico_ordem_servico_casos_criticos;")
        conn.commit()

        logger.info("Inserindo dataframe no MySQL... (%s linhas)", len(df))
        ok = insert_dataframe_mysql_direct(df, "historico_ordem_servico_casos_criticos", conn)

        if ok:
            logger.info("ETL finalizado com sucesso ✅")
        else:
            logger.warning("Inserção retornou 'False' — valide o insert_dataframe_mysql_direct.")

    finally:
        conn.close()


if __name__ == "__main__":
    etl_workorderhistory_priority()
