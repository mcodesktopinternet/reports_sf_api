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
logger = logging.getLogger("etl_servicos_tecnicos_criticos")


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
    AppointmentNumber,
    FirstScheduleDateTime__c,
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
    Reschedule_Reason_SA__c,
    FSL__Pinned__c,
    WorkOrder__r.dt_abertura__c,
    WorkOrder__r.LegacyId__c,
    WorkOrder__r.WorkOrderNumber,
    WorkOrder__r.DataAgendamento__c,
    WorkOrder__r.Work_Type_WO__c,
    WorkOrder__r.Work_Subtype_WO__c,
    WorkOrder__r.Status,
    WorkOrder__r.CaseReason__c,
    WorkOrder__r.Submotivo__c,
    LowCodeFormula__c,
    WorkOrder__r.ReasonForCancellationWorkOrder__c,
    WorkOrder__r.SuspensionReasonWo__c,
    WorkOrder__r.OLT__r.Name,
    WorkOrder__r.CTO__c,
    WorkOrder__r.LastModifiedDate,
    WorkOrder__r.IsRescheduledWo__c,
    WorkOrder__r.ConvenienciaCliente__c,
    WorkOrder__r.SolicitaAntecipacao__c,
    WorkOrder__r.HowManyTimesWo__c,
    WorkOrder__r.ReasonOfCriticality__c,
    WorkOrder__r.ObservationOnPriority__c,
    WorkOrder__r.Description,
    WorkOrder__r.Case.QuantidadeChips__c,
    WorkOrder__r.Case.PontosAdicionais__c,
    WorkOrder__r.Case.PontosAdicionaisTV__c,
    WorkOrder__r.Case.CaseNumber,
    WorkOrder__r.Asset.Name,
    WorkOrder__r.Case.Lxd_observation__c,
    WorkOrder__r.Case.Owner.Name,
    WorkOrder__r.Case.Area_de_atendimento__c,
    WorkOrder__r.Case.Account.LXD_CPF__c,
    WorkOrder__r.Criada_critica__c,
    WorkOrder__r.Priority2__c,
    WorkOrder__r.Tornou_se_critica__c,
    WorkOrder__r.ReasonOfCriticality2__c,
    WorkOrder__r.Prioridade_por_Encaixe__c,
    WorkOrder__r.Periodo_solicita_encaixe__c,
    WorkOrder__r.ObservationOnPriority2__c,
    WorkOrder__r.Data_solicita_o_encaixe__c,
    WorkOrder__r.Agendamento_Fura_Fila__c
FROM ServiceAppointment
WHERE WorkOrder__r.CreatedDate >= {CREATED_FROM}
  AND (WorkOrder__r.Priority = 'Critical' OR WorkOrder__r.Priority2__c = 'Critical')
  AND WorkOrder__r.Case.Account.LXD_CPF__c != ''
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
    "WorkOrder__r_Data_solicita_o_encaixe__c",
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
    "WorkOrder__r_LastModifiedDate": "data_ultima_modificacao",
    "WorkOrder__r_IsRescheduledWo__c": "foi_reagendado",
    "WorkOrder__r_ConvenienciaCliente__c": "conveniencia_cliente",
    "WorkOrder__r_SolicitaAntecipacao__c": "solicita_antecipacao",
    "WorkOrder__r_HowManyTimesWo__c": "quantas_vezes",
    "WorkOrder__r_ReasonOfCriticality__c": "motivo_criticidade",
    "WorkOrder__r_ObservationOnPriority__c": "observacao_prioridade",
    "WorkOrder__r_Description": "descricao",
    "WorkOrder__r_Case_QuantidadeChips__c": "quantidade_chips",
    "WorkOrder__r_Case_PontosAdicionais__c": "pontos_mesh_instalar",
    "WorkOrder__r_Case_PontosAdicionaisTV__c": "pontos_tv_instalar",
    "WorkOrder__r_Case_CaseNumber": "numero_caso",
    "WorkOrder__r_Asset_Name": "ativo",
    "WorkOrder__r_Case_Lxd_observation__c": "observacao",
    "WorkOrder__r_Case_Owner_Name": "proprietario_caso",
    "WorkOrder__r_Case_Area_de_atendimento__c": "grupo",
    "WorkOrder__r_Case_Account_LXD_CPF__c": "cpf_cnpj",
    "WorkOrder__r_Criada_critica__c": "criada_critica",
    "WorkOrder__r_Priority2__c": "prioridade2",
    "WorkOrder__r_Tornou_se_critica__c": "tornou_se_critica",
    "WorkOrder__r_ReasonOfCriticality2__c": "motivo_criticidade2",
    "WorkOrder__r_Prioridade_por_Encaixe__c": "prioridade_encaixe",
    "WorkOrder__r_Periodo_solicita_encaixe__c": "periodo_encaixe2",
    "WorkOrder__r_ObservationOnPriority2__c": "observacao_prioridade2",
    "WorkOrder__r_Data_solicita_o_encaixe__c": "data_encaixe2",
    "WorkOrder__r_Agendamento_Fura_Fila__c": "agendamento_fura_fila",
}

MYSQL_TABLE = "servicos_tecnicos"


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

    # garante todas as colunas esperadas (mesma estrutura do MySQL)
    expected_cols = list(COLUNAS_DE_PARA.values())
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # ordena exatamente no padrão esperado
    df = df[expected_cols]

    # limpa NaN
    df = df.where(pd.notna(df), None)

    return df


# -----------------------------------------------------------------------------
# ETL
# -----------------------------------------------------------------------------
def etl_servicos_tecnicos_criticos():
    load_dotenv()

    sf = get_sf_config()
    mysql = get_mysql_config()

    created_from = os.getenv("SF_CRITICAL_CREATED_FROM", "2025-08-11T03:00:00.000Z")
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

    logger.info("Consultando ServiceAppointment... (CreatedDate >= %s | Critical)", created_from)
    records = get_all_query_results(
        instance_url=instance_url,
        auth_headers=headers,
        query=soql,
    )

    if not records:
        logger.warning("Nenhum dado retornado do Salesforce.")
        return

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

        logger.info("Limpando tabela %s...", MYSQL_TABLE)
        cursor.execute(f"TRUNCATE TABLE {MYSQL_TABLE};")
        conn.commit()

        logger.info("Inserindo dataframe no MySQL... (%s linhas)", len(df))
        ok = insert_dataframe_mysql_direct(df, MYSQL_TABLE, conn)

        if ok:
            logger.info("ETL finalizado com sucesso ✅")
        else:
            logger.warning("Inserção retornou 'False' — valide o insert_dataframe_mysql_direct.")

    finally:
        conn.close()


if __name__ == "__main__":
    etl_servicos_tecnicos_criticos()
