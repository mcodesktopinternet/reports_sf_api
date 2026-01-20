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
logger = logging.getLogger("etl_geovane")


# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
def require_env(*keys: str) -> dict:
    """Lê variáveis do .env e falha com erro amigável se faltar alguma."""
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
    MicroTerritory__c,
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
    WorkOrder__r.TecnicoHabilitadoIndicarMovel__c,
    WorkOrder__r.ClienteAptoParaChip__c,
    WorkOrder__r.Indicacao_feita_pelo_tecnico__c,
    WorkOrder__r.CreatedBy.Name,
    WorkOrder__r.PontosAdicionais__c,
    WorkOrder__r.PontosAdicionaisTV__c,
    WorkOrder__r.Chip__c,
    WorkOrder__r.Priority,
    ServiceTerritory.Name,
    WorkOrder__r.Agendamento_Fura_Fila__c
FROM ServiceAppointment
WHERE WorkOrder__r.CreatedDate >= {CREATED_FROM}
"""


# -----------------------------------------------------------------------------
# TRANSFORM
# -----------------------------------------------------------------------------
TIMESTAMP_COLS = [
    "ArrivalWindowStart_Gantt__c",
    "ArrivalWindowEnd_Gantt__c",
    "ScheduledStart_Gantt__c",
    "SchedEndTime_Gantt__c",
    "ActualStart_Gantt__c",
    "ActualEnd_Gantt__c",
    "WorkOrder__r_dt_abertura__c",
    "WorkOrder__r_DataAgendamento__c",
    "WorkOrder__r_LastModifiedDate",
]

COLUNAS_DE_PARA = {
    "Id": "id",
    "AppointmentNumber": "numero_compromisso",
    "FirstScheduleDateTime__c": "data_primeiro_agendamento",
    "ArrivalWindowStart_Gantt__c": "inicio_janela_chegada",
    "ArrivalWindowEnd_Gantt__c": "termino_janela_chegada",
    "ScheduledStart_Gantt__c": "inicio_agendado",
    "SchedEndTime_Gantt__c": "termino_agendado",
    "ActualStart_Gantt__c": "inicio_servico",
    "ActualEnd_Gantt__c": "termino_servico",
    "TechnicianName__c": "nome_tecnico",
    "TechniciansCompany__c": "empresa_tecnico",
    "WorkOrder__r_City": "cidade",
    "MicroTerritory__c": "micro_territorio",
    "Reschedule_Reason_SA__c": "motivo_reagendamento",
    "FSL__Pinned__c": "fixado",
    "WorkOrder__r_dt_abertura__c": "dt_abertura",
    "WorkOrder__r_LegacyId__c": "codigo_cliente",
    "WorkOrder__r_WorkOrderNumber": "numero_ordem_trabalho",
    "WorkOrder__r_DataAgendamento__c": "data_agendamento",
    "WorkOrder__r_Work_Type_WO__c": "tipo_trabalho",
    "WorkOrder__r_Work_Subtype_WO__c": "subtipo_trabalho",
    "WorkOrder__r_Status": "status",
    "WorkOrder__r_CaseReason__c": "motivo_caso",
    "WorkOrder__r_Submotivo__c": "submotivo_caso",
    "LowCodeFormula__c": "codigo_baixa",
    "WorkOrder__r_ReasonForCancellationWorkOrder__c": "motivo_cancelamento",
    "WorkOrder__r_SuspensionReasonWo__c": "motivo_suspensao",
    "WorkOrder__r_OLT__r_Name": "olt",
    "WorkOrder__r_CTO__c": "cto",
    "WorkOrder__r_Asset_Name": "ativo",
    "WorkOrder__r_LastModifiedDate": "data_ultima_modificacao",
    "WorkOrder__r_IsRescheduledWo__c": "foi_reagendado",
    "WorkOrder__r_ConvenienciaCliente__c": "conveniencia_cliente",
    "WorkOrder__r_SolicitaAntecipacao__c": "solicita_antecipacao",
    "WorkOrder__r_HowManyTimesWo__c": "quantas_vezes",
    "WorkOrder__r_TecnicoHabilitadoIndicarMovel__c": "tecnico_habilitado_indicar_movel",
    "WorkOrder__r_ClienteAptoParaChip__c": "cliente_aptos_para_chip",
    "WorkOrder__r_Indicacao_feita_pelo_tecnico__c": "indicacao_feita_pelo_tecnico",
    "WorkOrder__r_CreatedBy_Name": "criado_por",
    "WorkOrder__r_PontosAdicionais__c": "pontos_mesh_instalar",
    "WorkOrder__r_PontosAdicionaisTV__c": "pontos_tv_instalar",
    "WorkOrder__r_Chip__c": "chip_entregar",
    "WorkOrder__r_Priority": "prioridade",
    "ServiceTerritory_Name": "territorio_servico",
    "WorkOrder__r_Agendamento_Fura_Fila__c": "agendamento_fura_fila",
    "WorkOrder__r_Case_CaseNumber": "numero_caso",
    "WorkOrder__r_Case_Account_LXD_CPF__c": "cpf_cnpj",
}


def normalize_df(records: list[dict]) -> pd.DataFrame:
    df = pd.json_normalize(records, sep="_")

    # remove "attributes" do Salesforce
    df = df.drop(columns=[c for c in df.columns if "attributes_" in c], errors="ignore")

    # converte timestamps conhecidos
    for col in TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = convert_timestamp_column(df[col])

    # renomeia para padrão do MySQL
    df = df.rename(columns=COLUNAS_DE_PARA)

    # remove colunas relacionais que sobraram (workorder cru)
    df = df.drop(columns=[c for c in df.columns if c.startswith("WorkOrder__r_")], errors="ignore")

    # evita warning / garante vazio string
    df = df.astype(object).fillna("")
    return df


# -----------------------------------------------------------------------------
# ETL
# -----------------------------------------------------------------------------
def etl_geovane_base_original():
    load_dotenv()

    sf = get_sf_config()
    mysql = get_mysql_config()

    created_from = os.getenv("SF_WO_CREATED_FROM", "2025-10-01T00:00:00.000-03:00")
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

    logger.info("Consultando Salesforce... (CreatedDate >= %s)", created_from)
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

        logger.info("Limpando tabela uploadagendamentos_geovane...")
        cursor.execute("TRUNCATE TABLE uploadagendamentos_geovane;")
        conn.commit()

        logger.info("Inserindo dataframe no MySQL... (%s linhas)", len(df))
        ok = insert_dataframe_mysql_direct(df, "uploadagendamentos_geovane", conn)

        logger.info("Executando procedure processar_upload_agendamentos()...")
        cursor.execute("CALL processar_upload_agendamentos();")
        conn.commit()

        if ok:
            logger.info("ETL finalizado com sucesso ✅")
        else:
            logger.warning("Inserção retornou 'False' — valide o insert_dataframe_mysql_direct.")

    finally:
        conn.close()


if __name__ == "__main__":
    etl_geovane_base_original()
