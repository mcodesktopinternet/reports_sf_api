import pandas as pd
from datetime import timedelta
from sf_auth import get_salesforce_token, get_auth_headers
from conectar_mysql import conectar_mysql, insert_dataframe_mysql_direct
import requests

# ==========================
# CONFIGURAÇÕES
# ==========================

DOMAIN = "https://desktopsa.my.salesforce.com"

CLIENT_ID = "3MVG99gP.VbJma8WslD5o_qMP_J4iVg1FUTtQzWQ4_TxpBoZWi6MyFHtxCZMZFOCk2FsJbw2wNynhmaD7c8Uv"
CLIENT_SECRET = "E31D29879301886FE62D60D79898EE335B488E0A37E4DDB9874D12F12C29719E"
USERNAME = "plan.relatorios@desktop.net.br"
PASSWORD = "PlanRelatorios@2026Xo2zP5tjETV0fOLW5V6RZQBb"

MYSQL_CONFIG = {
    "host": "172.29.5.3",
    "database": "db_Melhoria_continua_operacoes",
    "user": "Geovane.Faria",
    "password": "GeovaneDesk2@25"
}

BATCH_INSERT = 3000

# ==========================
# QUERY SALESFORCE
# ==========================

SOQL_QUERY = """
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
AND CreatedDate = LAST_N_DAYS:2
ORDER BY CreatedDate ASC
"""

# ==========================
# FUNÇÕES AUXILIARES
# ==========================

def converter_data(series):
    dt = pd.to_datetime(series, utc=True, errors="coerce")
    return (dt - timedelta(hours=3)).dt.tz_localize(None), dt.dt.tz_localize(None)


def criar_tabela_mysql(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS service_appointment_cancelamentos (
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


# ==========================
# ITERADOR SALESFORCE
# ==========================

def iterar_salesforce(query, instance_url, headers):
    url = f"{instance_url}/services/data/v65.0/query"
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


# ==========================
# ETL PRINCIPAL
# ==========================

def etl_service_appointment_cancelado():

    print("🔐 Autenticando no Salesforce...")
    token_data = get_salesforce_token(
        domain=DOMAIN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        username=USERNAME,
        password=PASSWORD
    )

    headers = get_auth_headers(token_data)

    print("🛢️ Conectando ao MySQL...")
    conn = conectar_mysql(**MYSQL_CONFIG)
    if not conn:
        raise Exception("Erro ao conectar no MySQL")

    criar_tabela_mysql(conn)

    total_cancelados = 0

    try:
        print("📥 Iniciando extração em streaming...")

        for lote_api in iterar_salesforce(SOQL_QUERY, DOMAIN, headers):

            if not lote_api:
                continue

            df = pd.json_normalize(lote_api, sep="_")
            df.fillna("", inplace=True)

            # Remove campos técnicos
            df.drop(columns=[c for c in df.columns if "attributes" in c], inplace=True, errors="ignore")

            # Apenas status cancelado
            df = df[df["NewValue"] == "Cancelado"]

            if df.empty:
                continue

            # Datas
            df["created_date"], df["created_date_utc"] = converter_data(df["CreatedDate"])

            # Renomeia colunas
            df = df.rename(columns={
                "Id": "id",
                "ServiceAppointment_AppointmentNumber": "appointment_number",
                "Field": "field_name",
                "CreatedBy_Name": "created_by",
                "OldValue": "old_value",
                "NewValue": "new_value"
            })

            df = df[
                [
                    "id",
                    "appointment_number",
                    "field_name",
                    "created_by",
                    "old_value",
                    "new_value",
                    "created_date",
                    "created_date_utc"
                ]
            ]

            # Inserção em lote
            for i in range(0, len(df), BATCH_INSERT):
                chunk = df.iloc[i:i + BATCH_INSERT]
                insert_dataframe_mysql_direct(
                    chunk,
                    "service_appointment_cancelamentos",
                    conn
                )
                total_cancelados += len(chunk)

            print(f"✅ Cancelados inseridos até agora: {total_cancelados:,}")

            del df

    finally:
        conn.close()
        print(f"🎯 ETL FINALIZADO | TOTAL CANCELADOS: {total_cancelados:,}")


# ==========================
# EXECUÇÃO
# ==========================

if __name__ == "__main__":
    etl_service_appointment_cancelado()
