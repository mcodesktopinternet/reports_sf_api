import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

import requests
from requests.exceptions import RequestException
from dotenv import load_dotenv

# =============================================================================
# CARREGAR VARIÁVEIS DO .env
# =============================================================================
load_dotenv()

# =============================================================================
# CONFIGURAÇÕES / VARIÁVEIS DE AMBIENTE
# =============================================================================

SALESFORCE_DOMAIN = os.getenv("SF_DOMAIN", "https://desktopsa.my.salesforce.com")
SALESFORCE_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SALESFORCE_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
SALESFORCE_USERNAME = os.getenv("SF_USERNAME")
SALESFORCE_PASSWORD = os.getenv("SF_PASSWORD")

# Diretório fixo para salvar o CSV
OUTPUT_DIR = r"Y:\Acompanhamento Operações\Base modificações suspensão e cancelamento"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# FUNÇÕES DE AUTENTICAÇÃO SALESFORCE
# =============================================================================

def get_salesforce_token() -> Dict[str, Any]:
    """Autentica no Salesforce usando OAuth2 e retorna o token."""
    print(f"[{datetime.now()}] Autenticando no Salesforce...")

    url = f"{SALESFORCE_DOMAIN}/services/oauth2/token"

    data = {
        "grant_type": "password",
        "client_id": SALESFORCE_CLIENT_ID,
        "client_secret": SALESFORCE_CLIENT_SECRET,
        "username": SALESFORCE_USERNAME,
        "password": SALESFORCE_PASSWORD,
    }

    try:
        response = requests.post(url, data=data, timeout=60)

        # DEBUG DO SALESFORCE → mostra erro REAL
        if response.status_code != 200:
            print("\n----- ERRO DETALHADO DO SALESFORCE -----")
            print("Status:", response.status_code)
            print("Resposta:")
            print(response.text)
            print("----------------------------------------\n")

        response.raise_for_status()
        print(f"[{datetime.now()}] Autenticação Salesforce concluída.")
        return response.json()

    except RequestException as e:
        raise RuntimeError(f"Erro ao autenticar no Salesforce: {e}")


def get_auth_headers(token_data: Dict[str, Any]) -> Dict[str, str]:
    """Cria o header de autenticação."""
    return {"Authorization": f"Bearer {token_data['access_token']}"}


# =============================================================================
# CONSULTA COM PAGINAÇÃO
# =============================================================================

def query_salesforce_all(domain: str, headers: Dict[str, str], soql: str) -> List[Dict[str, Any]]:
    """Executa SOQL com paginação automática."""
    full_url = f"{domain}/services/data/v57.0/query?q={soql}"

    registros = []
    print(f"[{datetime.now()}] Executando consulta...")

    try:
        while True:
            response = requests.get(full_url, headers=headers, timeout=120)
            response.raise_for_status()

            data = response.json()
            registros.extend(data.get("records", []))

            if not data.get("done"):
                full_url = f"{domain}{data['nextRecordsUrl']}"
            else:
                break

        print(f"[{datetime.now()}] Total retornado: {len(registros)} registros")
        return registros

    except RequestException as e:
        raise RuntimeError(f"Erro ao consultar Salesforce: {e}")


# =============================================================================
# SOQL DO HISTÓRICO
# =============================================================================

SOQL_HISTORICO = """
    SELECT 
    Id, 
    IsDeleted, 
    ServiceAppointmentId, 
    ServiceAppointment.AppointmentNumber, 
    CreatedById, 
    CreatedDate, 
    Field, 
    DataType, 
    OldValue, 
    NewValue,
    CreatedBy.Name
    FROM ServiceAppointmentHistory 
    WHERE CreatedDate >= 2025-10-31T21:00:00.000-03:00
    AND Field IN ('MotivoDoCancelamento__c', 'SuspensionReason__c', 'Reschedule_Reason_SA__c', 'RescheduleReasonSAHistory__c')
"""


# =============================================================================
# EXPORTAÇÃO CSV
# =============================================================================

def exportar_csv(df: pd.DataFrame, nome_arquivo: str = "relatorio_hxh.csv") -> str:
    """Exporta DataFrame para CSV em pasta fixa."""
    arquivo = os.path.join(OUTPUT_DIR, nome_arquivo)
    df.to_csv(arquivo, index=False, encoding="utf-8-sig")
    print(f"[{datetime.now()}] Arquivo gerado: {arquivo}")
    return arquivo


# =============================================================================
# ETL PRINCIPAL
# =============================================================================

def exportar_historico_wo():
    """Processo completo para exportar o histórico solicitado."""
    print("=" * 80)
    print("   EXPORTAÇÃO DE HISTÓRICO DE WORK ORDERS (SALESFORCE)")
    print("=" * 80)

    token_data = get_salesforce_token()
    headers = get_auth_headers(token_data)

    registros = query_salesforce_all(SALESFORCE_DOMAIN, headers, SOQL_HISTORICO)

    if not registros:
        print(f"[{datetime.now()}] Nenhum registro encontrado.")
        return

    df = pd.json_normalize(registros, sep="_")
    df = df.drop(columns=[c for c in df.columns if "attributes" in c], errors="ignore")

    # =====================================================================
    # FILTRAR APENAS AS COLUNAS PEDIDAS
    # =====================================================================
    colunas_desejadas = {
        "ServiceAppointment_AppointmentNumber": "Compromisso de serviço: Número de compromisso",
        "CreatedDate": "Created Date",
        "Field": "Campo alterado",
        "NewValue": "Valor da nova string",
    }

    df = df[list(colunas_desejadas.keys())]
    df = df.rename(columns=colunas_desejadas)

    # =====================================================================
    # EXPORTAR
    # =====================================================================
    exportar_csv(df)  # nome fixo: relatorio_hxh.csv

    print(f"[{datetime.now()}] Processo concluído com sucesso.")
    print("=" * 80)


if __name__ == "__main__":
    exportar_historico_wo()
