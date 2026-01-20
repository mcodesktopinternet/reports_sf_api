from sf_auth import get_salesforce_token, get_auth_headers
from sf_query import get_all_query_results
import pandas as pd
from convert_timestamp_column import convert_timestamp_column
from conectar_mysql import conectar_mysql
from conectar_mysql import insert_dataframe_mysql_direct


def etl_richard_critical():

    # =====================================================
    # SALESFORCE AUTH
    # =====================================================
    domain = "https://desktopsa.my.salesforce.com"
    client_id = "3MVG99gP.VbJma8WslD5o_qMP_J4iVg1FUTtQzWQ4_TxpBoZWi6MyFHtxCZMZFOCk2FsJbw2wNynhmaD7c8Uv"
    client_secret = "E31D29879301886FE62D60D79898EE335B488E0A37E4DDB9874D12F12C29719E"
    username = "plan.relatorios@desktop.net.br"
    password = "PlanRelatorios@2026Xo2zP5tjETV0fOLW5V6RZQBb"

    token_data = get_salesforce_token(
        domain=domain,
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password
    )

    headers = get_auth_headers(token_data)

    # =====================================================
    # SOQL (FORMATO CORRETO)
    # =====================================================
    query = """
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
    WHERE
        WorkOrder__r.CreatedDate >= 2025-08-11T03:00:00.000Z
        AND (
            WorkOrder__r.Priority = 'Critical'
            OR WorkOrder__r.Priority2__c = 'Critical'
        )
        AND WorkOrder__r.Case.Account.LXD_CPF__c != ''
    """

    resultado = get_all_query_results(
        instance_url=domain,
        auth_headers=headers,
        query=query
    )

    if not resultado:
        print("Nenhum dado retornado do Salesforce.")
        return

    # =====================================================
    # DATAFRAME
    # =====================================================
    df = pd.json_normalize(resultado, sep="_")

    # remove colunas attributes
    df.drop(columns=[c for c in df.columns if "attributes_" in c], inplace=True)

    # =====================================================
    # CONVERSÃO DE DATAS
    # =====================================================
    colunas_timestamp = [
        'ArrivalWindowStart_Gantt__c',
        'ArrivalWindowEnd_Gantt__c',
        'ScheduledStart_Gantt__c',
        'SchedEndTime_Gantt__c',
        'ActualStart_Gantt__c',
        'ActualEnd_Gantt__c',
        'WorkOrder__r_dt_abertura__c',
        'WorkOrder__r_DataAgendamento__c',
        'WorkOrder__r_LastModifiedDate',
        'WorkOrder__r_Data_solicita_o_encaixe__c'
    ]

    for col in colunas_timestamp:
        if col in df.columns:
            df[col] = convert_timestamp_column(df[col])

    # =====================================================
    # DE / PARA (EXATAMENTE IGUAL AO MYSQL)
    # =====================================================
    colunas_de_para = {
        'Id': 'id',
        'AppointmentNumber': 'numero_compromisso',
        'FirstScheduleDateTime__c': 'data_primeiro_agendamento',
        'ArrivalWindowStart_Gantt__c': 'inicio_janela_chegada',
        'ArrivalWindowEnd_Gantt__c': 'termino_janela_chegada',
        'ScheduledStart_Gantt__c': 'inicio_agendado',
        'SchedEndTime_Gantt__c': 'termino_agendado',
        'ActualStart_Gantt__c': 'inicio_servico',
        'ActualEnd_Gantt__c': 'termino_servico',
        'TechnicianName__c': 'nome_tecnico',
        'TechniciansCompany__c': 'empresa_tecnico',
        'WorkOrder__r_City': 'cidade',
        'MicroTerritory__c': 'micro_territorio',
        'Reschedule_Reason_SA__c': 'motivo_reagendamento',
        'FSL__Pinned__c': 'fixado',
        'WorkOrder__r_dt_abertura__c': 'dt_abertura',
        'WorkOrder__r_LegacyId__c': 'codigo_cliente',
        'WorkOrder__r_WorkOrderNumber': 'numero_ordem_trabalho',
        'WorkOrder__r_DataAgendamento__c': 'data_agendamento',
        'WorkOrder__r_Work_Type_WO__c': 'tipo_trabalho',
        'WorkOrder__r_Work_Subtype_WO__c': 'subtipo_trabalho',
        'WorkOrder__r_Status': 'status',
        'WorkOrder__r_CaseReason__c': 'motivo_caso',
        'WorkOrder__r_Submotivo__c': 'submotivo_caso',
        'LowCodeFormula__c': 'codigo_baixa',
        'WorkOrder__r_ReasonForCancellationWorkOrder__c': 'motivo_cancelamento',
        'WorkOrder__r_SuspensionReasonWo__c': 'motivo_suspensao',
        'WorkOrder__r_OLT__r_Name': 'olt',
        'WorkOrder__r_CTO__c': 'cto',
        'WorkOrder__r_LastModifiedDate': 'data_ultima_modificacao',
        'WorkOrder__r_IsRescheduledWo__c': 'foi_reagendado',
        'WorkOrder__r_ConvenienciaCliente__c': 'conveniencia_cliente',
        'WorkOrder__r_SolicitaAntecipacao__c': 'solicita_antecipacao',
        'WorkOrder__r_HowManyTimesWo__c': 'quantas_vezes',
        'WorkOrder__r_ReasonOfCriticality__c': 'motivo_criticidade',
        'WorkOrder__r_ObservationOnPriority__c': 'observacao_prioridade',
        'WorkOrder__r_Description': 'descricao',
        'WorkOrder__r_Case_QuantidadeChips__c': 'quantidade_chips',
        'WorkOrder__r_Case_PontosAdicionais__c': 'pontos_mesh_instalar',
        'WorkOrder__r_Case_PontosAdicionaisTV__c': 'pontos_tv_instalar',
        'WorkOrder__r_Case_CaseNumber': 'numero_caso',
        'WorkOrder__r_Asset_Name': 'ativo',
        'WorkOrder__r_Case_Lxd_observation__c': 'observacao',
        'WorkOrder__r_Case_Owner_Name': 'proprietario_caso',
        'WorkOrder__r_Case_Area_de_atendimento__c': 'grupo',
        'WorkOrder__r_Case_Account_LXD_CPF__c': 'cpf_cnpj',
        'WorkOrder__r_Criada_critica__c': 'criada_critica',
        'WorkOrder__r_Priority2__c': 'prioridade2',
        'WorkOrder__r_Tornou_se_critica__c': 'tornou_se_critica',
        'WorkOrder__r_ReasonOfCriticality2__c': 'motivo_criticidade2',
        'WorkOrder__r_Prioridade_por_Encaixe__c': 'prioridade_encaixe',
        'WorkOrder__r_Periodo_solicita_encaixe__c': 'periodo_encaixe2',
        'WorkOrder__r_ObservationOnPriority2__c': 'observacao_prioridade2',
        'WorkOrder__r_Data_solicita_o_encaixe__c': 'data_encaixe2',
        'WorkOrder__r_Agendamento_Fura_Fila__c': 'agendamento_fura_fila'
    }

    df.rename(columns=colunas_de_para, inplace=True)

    # garante todas as colunas do MySQL
    for col in colunas_de_para.values():
        if col not in df.columns:
            df[col] = None

    df = df[list(colunas_de_para.values())]

    # remove colunas inválidas e NaN
    df = df.loc[:, df.columns.notna()]
    df.columns = df.columns.astype(str)
    df = df.where(pd.notna(df), None)

    # =====================================================
    # MYSQL
    # =====================================================
    conn = conectar_mysql(
        host="172.29.5.3",
        database="db_Melhoria_continua_operacoes",
        user="Geovane.Faria",
        password="GeovaneDesk2@25"
    )

    if conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE servicos_tecnicos;")
        conn.commit()

        insert_dataframe_mysql_direct(df, 'servicos_tecnicos', conn)
        conn.close()


etl_richard_critical()
