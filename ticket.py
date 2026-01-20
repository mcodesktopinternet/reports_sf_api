import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

from sf_auth import get_salesforce_token, get_auth_headers
from sf_query import get_all_query_results
from conectar_mysql import conectar_mysql, insert_dataframe_mysql_direct


# -----------------------------------------------------------------------------
# LOG
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl_ticket_api_cto")


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


def get_desktop_api_config() -> dict:
    return require_env(
        "DESKTOP_OAUTH_URL",
        "DESKTOP_CLIENT_ID",
        "DESKTOP_CLIENT_SECRET",
        "DESKTOP_API_BASE",
    )


# -----------------------------------------------------------------------------
# PARAMS (via env)
# -----------------------------------------------------------------------------
TZ_LOCAL = os.getenv("TZ_LOCAL", "America/Sao_Paulo").strip()
MYSQL_TABLE = os.getenv("TICKET_TABLE", "ticket").strip()
SF_API_VERSION = os.getenv("SF_API_VERSION", "65.0").strip()

# desliga SSL verify? (apenas se realmente necessário)
DESKTOP_VERIFY_SSL = os.getenv("DESKTOP_VERIFY_SSL", "0").strip()  # 1=verifica, 0=nao verifica

# consulta SF (datas via env)
SF_SCHED_END_FROM = os.getenv("SF_TICKET_SCHED_END_FROM", "2025-11-03T00:00:00.000-03:00").strip()


# -----------------------------------------------------------------------------
# SOQL
# -----------------------------------------------------------------------------
SOQL_QUERY = f"""
SELECT
    Id,
    WorkOrder__r.Asset.SiglaCTO__c,
    WorkOrder__r.Asset.CaixaCTO__c,
    WorkOrder__r.Asset.PortaCTO__c,
    WorkOrder__r.dt_abertura__c,
    WorkOrder__r.LegacyId__c,
    WorkOrder__r.WorkOrderNumber,
    WorkOrder__r.Case.CaseNumber,
    AppointmentNumber,
    FirstScheduleDateTime__c,
    WorkOrder__r.DataAgendamento__c,
    ArrivalWindowStart_Gantt__c,
    ArrivalWindowEnd_Gantt__c,
    ScheduledStart_Gantt__c,
    SchedEndTime_Gantt__c,
    ActualStart_Gantt__c,
    ActualEnd_Gantt__c,
    TechnicianName__c,
    TechniciansCompany__c,
    WorkOrder__r.City,
    WorkOrder__r.Work_Type_WO__c,
    WorkOrder__r.Work_Subtype_WO__c,
    WorkOrder__r.Status,
    WorkOrder__r.CaseReason__c,
    WorkOrder__r.Submotivo__c,
    LowCodeFormula__c,
    WorkOrder__r.ReasonForCancellationWorkOrder__c,
    Reschedule_Reason_SA__c,
    WorkOrder__r.SuspensionReasonWo__c,
    WorkOrder__r.OLT__r.Name,
    WorkOrder__r.CTO__c,
    WorkOrder__r.Asset.Name,
    WorkOrder__r.LastModifiedDate,
    WorkOrder__r.Case.Account.LXD_CPF__c,
    FSL__Pinned__c,
    WorkOrder__r.IsRescheduledWo__c,
    WorkOrder__r.ConvenienciaCliente__c,
    WorkOrder__r.SolicitaAntecipacao__c,
    WorkOrder__r.HowManyTimesWo__c,
    WorkOrder__r.Subject,
    StringPPoeUser__c
FROM ServiceAppointment
WHERE SchedEndTime_Gantt__c >= {SF_SCHED_END_FROM}
  AND WorkOrder__r.Subject LIKE '%INC%'
  AND Status = 'Concluída'
  AND WorkOrder__r.Work_Type_WO__c = 'Manutenção'
"""


# -----------------------------------------------------------------------------
# DESKTOP API CLIENT
# -----------------------------------------------------------------------------
class DesktopAPIClient:
    def __init__(self, oauth_url: str, client_id: str, client_secret: str, api_base: str, verify_ssl: bool):
        self.oauth_url = oauth_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_base = api_base.rstrip("/")

        self.session = requests.Session()
        self.session.verify = verify_ssl  # True/False
        self.token: Optional[str] = None

    def obter_token_oauth(self) -> Optional[str]:
        try:
            logger.info("Obtendo token OAuth da API Desktop...")
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }

            resp = self.session.post(self.oauth_url, data=payload, headers=headers, timeout=600)
            resp.raise_for_status()

            access_token = (resp.json().get("access_token") or "").strip()
            if not access_token:
                logger.error("Token OAuth veio vazio.")
                return None

            self.token = access_token
            logger.info("Token OAuth obtido com sucesso.")
            return self.token

        except requests.exceptions.RequestException as e:
            logger.error("Erro crítico ao obter token OAuth: %s", e)
            return None

    def _headers(self) -> dict:
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token.strip()}"
        return h

    def consultar_cto_positions(self, numero_cto: str, sigla_cto: str) -> Optional[List[dict]]:
        if not numero_cto or not sigla_cto:
            return None

        url = f"{self.api_base}/resource-inventory/v1/ctos/{numero_cto}/positions?group={sigla_cto}"

        if not self.token and not self.obter_token_oauth():
            return None

        resp = self.session.get(url, headers=self._headers(), timeout=600)

        if resp.status_code == 404:
            return None

        if resp.status_code == 401:
            logger.warning("401 na CTO %s-%s. Renovando token e tentando 1x...", sigla_cto, numero_cto)
            if not self.obter_token_oauth():
                return None
            resp = self.session.get(url, headers=self._headers(), timeout=600)

        if resp.status_code in (401, 403):
            logger.error("AUTH ERROR %s CTO %s-%s | body: %s", resp.status_code, sigla_cto, numero_cto, (resp.text or "")[:600])
            return None

        try:
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else None
        except Exception as e:
            logger.warning("Erro lendo JSON CTO %s-%s: %s | body: %s", sigla_cto, numero_cto, e, (resp.text or "")[:500])
            return None


# -----------------------------------------------------------------------------
# TRANSFORM
# -----------------------------------------------------------------------------
COLUNAS_DE_PARA = {
    "Id": "id_service_appointment",
    "WorkOrder__r_Asset_SiglaCTO__c": "sigla_cto",
    "WorkOrder__r_Asset_CaixaCTO__c": "caixa_cto",
    "WorkOrder__r_Asset_PortaCTO__c": "porta_cto",
    "WorkOrder__r_dt_abertura__c": "dt_abertura",
    "WorkOrder__r_LegacyId__c": "codigo_cliente",
    "WorkOrder__r_WorkOrderNumber": "numero_ordem_trabalho",
    "WorkOrder__r_Case_CaseNumber": "caso",
    "AppointmentNumber": "numero_compromisso",
    "FirstScheduleDateTime__c": "data_primeiro_agendamento",
    "WorkOrder__r_DataAgendamento__c": "data_agendamento",
    "ArrivalWindowStart_Gantt__c": "inicio_janela_chegada",
    "ArrivalWindowEnd_Gantt__c": "termino_janela_chegada",
    "ScheduledStart_Gantt__c": "inicio_agendado",
    "SchedEndTime_Gantt__c": "termino_agendado",
    "ActualStart_Gantt__c": "inicio_servico",
    "ActualEnd_Gantt__c": "termino_servico",
    "TechnicianName__c": "nome_tecnico",
    "TechniciansCompany__c": "empresa_tecnico",
    "WorkOrder__r_City": "cidade",
    "WorkOrder__r_Work_Type_WO__c": "tipo_trabalho",
    "WorkOrder__r_Work_Subtype_WO__c": "subtipo_trabalho",
    "WorkOrder__r_Status": "status",
    "WorkOrder__r_CaseReason__c": "motivo_caso",
    "WorkOrder__r_Submotivo__c": "submotivo_caso",
    "LowCodeFormula__c": "codigo_baixa",
    "WorkOrder__r_ReasonForCancellationWorkOrder__c": "motivo_cancelamento",
    "Reschedule_Reason_SA__c": "motivo_reagendamento",
    "WorkOrder__r_SuspensionReasonWo__c": "motivo_suspensao",
    "WorkOrder__r_OLT__r_Name": "olt",
    "WorkOrder__r_CTO__c": "cto",
    "WorkOrder__r_Asset_Name": "ativo",
    "WorkOrder__r_LastModifiedDate": "last_modified_date",
    "WorkOrder__r_Case_Account_LXD_CPF__c": "cpf_cnpj",
    "FSL__Pinned__c": "pinned",
    "WorkOrder__r_IsRescheduledWo__c": "foi_reagendado",
    "WorkOrder__r_ConvenienciaCliente__c": "conveniencia_cliente",
    "WorkOrder__r_SolicitaAntecipacao__c": "solicita_antecipacao",
    "WorkOrder__r_HowManyTimesWo__c": "quantas_vezes",
    "WorkOrder__r_Subject": "subject",
    "StringPPoeUser__c": "pppoe",
}

COLUNAS_DATA = [
    "dt_abertura",
    "data_agendamento",
    "inicio_janela_chegada",
    "termino_janela_chegada",
    "inicio_agendado",
    "termino_agendado",
    "inicio_servico",
    "termino_servico",
    "last_modified_date",
    "ultima_conexao_inicio",
    "ultima_conexao_fim",
]

COLUNAS_NUM = ["quantas_vezes", "pinned", "conveniencia_cliente", "solicita_antecipacao"]


def _parse_api_datetime_br(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d/%m/%Y - %H:%M:%S")
    except Exception:
        return None


def enriquecer_cto(df: pd.DataFrame, desktop_api: DesktopAPIClient) -> pd.DataFrame:
    logger.info("Enriquecendo %s registros via API Desktop (CTO positions)...", len(df))

    status_list: List[str] = []
    inicio_list: List[Optional[datetime]] = []
    fim_list: List[Optional[datetime]] = []
    duracao_list: List[Optional[str]] = []

    now = datetime.now()

    for _, row in df.iterrows():
        sigla = row.get("sigla_cto")
        caixa = row.get("caixa_cto")
        porta_raw = row.get("porta_cto")

        status = "Dados insuficientes"
        inicio = None
        fim = None
        duracao = None

        if sigla and caixa and porta_raw:
            try:
                porta_num = int(float(porta_raw))
            except Exception:
                status = "Porta inválida"
            else:
                resp_api = desktop_api.consultar_cto_positions(str(caixa).strip(), str(sigla).strip())

                if isinstance(resp_api, list):
                    status = "Porta não encontrada"
                    for porta_info in resp_api:
                        if porta_info.get("port_number") == porta_num:
                            status = porta_info.get("status", "Status não informado")

                            inicio = _parse_api_datetime_br(porta_info.get("last_connection_start"))
                            fim = _parse_api_datetime_br(porta_info.get("last_connection_stop"))

                            if inicio and status == "Conectado":
                                secs = (now - inicio).total_seconds()
                                d, r = divmod(secs, 86400)
                                h, r = divmod(r, 3600)
                                m, _ = divmod(r, 60)
                                duracao = f"{int(d)}d {int(h)}h {int(m)}m"

                            break
                else:
                    status = "CTO não encontrada"

        status_list.append(status)
        inicio_list.append(inicio)
        fim_list.append(fim)
        duracao_list.append(duracao)

    df["status_cliente_api"] = status_list
    df["ultima_conexao_inicio"] = inicio_list
    df["ultima_conexao_fim"] = fim_list
    df["tempo_conectado"] = duracao_list

    return df


def ajustar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    # datas: tenta parse e converte para TZ_LOCAL, depois string "YYYY-mm-dd HH:MM:SS"
    for col in COLUNAS_DATA:
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce", utc=True)
            # se for datetime "naive" (caso API BR), cai como NaT no utc=True -> então tenta sem utc
            if dt.isna().all():
                dt = pd.to_datetime(df[col], errors="coerce")
                df[col] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                df[col] = dt.dt.tz_convert(TZ_LOCAL).dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")

    for col in COLUNAS_NUM:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    return df.astype(object).where(pd.notna(df), None)


# -----------------------------------------------------------------------------
# ETL
# -----------------------------------------------------------------------------
def sf_login() -> Tuple[str, dict]:
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


def mysql_prepare() -> Tuple[object, set]:
    mysql = get_mysql_config()
    logger.info("Conectando no MySQL (%s/%s)...", mysql["MYSQL_HOST"], mysql["MYSQL_DATABASE"])

    conn = conectar_mysql(
        host=mysql["MYSQL_HOST"],
        database=mysql["MYSQL_DATABASE"],
        user=mysql["MYSQL_USER"],
        password=mysql["MYSQL_PASSWORD"],
    )
    if not conn:
        raise RuntimeError("Falha na conexão MySQL.")

    colunas_mysql = set()
    with conn.cursor() as cursor:
        logger.info("Lendo colunas de %s...", MYSQL_TABLE)
        cursor.execute(f"SHOW COLUMNS FROM {MYSQL_TABLE};")
        colunas_mysql = {row[0] for row in cursor.fetchall()}

        logger.info("TRUNCATE %s...", MYSQL_TABLE)
        cursor.execute(f"TRUNCATE TABLE {MYSQL_TABLE};")
    conn.commit()

    return conn, colunas_mysql


def etl_base_corrigida():
    load_dotenv()

    desktop_cfg = get_desktop_api_config()
    verify_ssl = DESKTOP_VERIFY_SSL == "1"

    # 1) Login SF
    instance_url, headers_sf = sf_login()

    # 2) Cliente Desktop API
    desktop_api = DesktopAPIClient(
        oauth_url=desktop_cfg["DESKTOP_OAUTH_URL"],
        client_id=desktop_cfg["DESKTOP_CLIENT_ID"],
        client_secret=desktop_cfg["DESKTOP_CLIENT_SECRET"],
        api_base=desktop_cfg["DESKTOP_API_BASE"],
        verify_ssl=verify_ssl,
    )
    if not desktop_api.obter_token_oauth():
        logger.error("Não foi possível obter token da API Desktop. Encerrando.")
        return

    # 3) Extrai do Salesforce
    logger.info("Executando consulta Salesforce...")
    records = get_all_query_results(
        instance_url=instance_url,
        auth_headers=headers_sf,
        query=SOQL_QUERY,
    )
    logger.info("Consulta retornou %s registros.", len(records))
    if not records:
        logger.warning("Nenhum registro encontrado. Encerrando.")
        return

    # 4) Prepara MySQL
    conn, colunas_mysql = mysql_prepare()

    try:
        # 5) Transform
        df = pd.json_normalize(records, sep="_")
        df = df.drop(columns=[c for c in df.columns if "attributes_" in c], errors="ignore")
        df = df.rename(columns=COLUNAS_DE_PARA)

        # 6) Enriquecimento API CTO
        df = enriquecer_cto(df, desktop_api)

        # 7) Ajustes de tipo
        df = ajustar_tipos(df)

        # 8) Carga (somente colunas existentes)
        cols_to_insert = [c for c in df.columns if c in colunas_mysql]
        df_insert = df[cols_to_insert]

        logger.info("Colunas a inserir (%s): %s", len(cols_to_insert), cols_to_insert)
        logger.info("Inserindo %s registros em %s...", len(df_insert), MYSQL_TABLE)

        insert_dataframe_mysql_direct(df_insert, MYSQL_TABLE, conn)
        logger.info("Inserção concluída com sucesso ✅")

    except Exception as e:
        logger.error("Falha crítica durante ETL: %s", e)
        raise
    finally:
        conn.close()
        logger.info("Conexão MySQL fechada. Processo concluído.")


if __name__ == "__main__":
    etl_base_corrigida()
