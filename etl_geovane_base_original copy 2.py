from sf_auth import get_salesforce_token, get_auth_headers
from sf_query import get_all_query_results
import pandas as pd
from convert_timestamp_column import convert_timestamp_column
from conectar_mysql import conectar_mysql
from conectar_mysql import insert_dataframe_mysql_direct


def etl_geovane_base_original():

    domain = "https://desktopsa.my.salesforce.com"
    client_id = "3MVG99gP.VbJma8WslD5o_qMP_J4iVg1FUTtQzWQ4_TxpBoZWi6MyFHtxCZMZFOCk2FsJbw2wNynhmaD7c8Uv"
    client_secret = "E31D29879301886FE62D60D79898EE335B488E0A37E4DDB9874D12F12C29719E"
    username = "plan.relatorios@desktop.net.br"
    password = "PlanRelatorios@2025PwdjW2bnaqa3Z8O7KwuqVoLp"

    token_data = get_salesforce_token(
        domain=domain,
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password
    )

    headers = get_auth_headers(token_data)

    query = """
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
    WHERE WorkOrder__r.CreatedDate >= 2025-10-01T03:00:00.000Z
    LIMIT 10
    """

    resultado = get_all_query_results(
        instance_url="https://desktopsa.my.salesforce.com",
        auth_headers=headers,
        query=query
    )

    all_results = pd.json_normalize(resultado, sep="_")
    all_results.fillna("", inplace=True)

    # Remove colunas attributes
    all_results = all_results.drop(
        columns=[c for c in all_results.columns if 'attributes_' in c],
        errors='ignore'
    )

    colunas_timestamp = [
        'ArrivalWindowStart_Gantt__c',
        'ArrivalWindowEnd_Gantt__c',
        'ScheduledStart_Gantt__c',
        'SchedEndTime_Gantt__c',
        'ActualStart_Gantt__c',
        'ActualEnd_Gantt__c',
        'WorkOrder__r_dt_abertura__c',
        'WorkOrder__r_DataAgendamento__c',
        'WorkOrder__r_LastModifiedDate'
    ]

    for coluna in colunas_timestamp:
        if coluna in all_results.columns:
            all_results[coluna] = convert_timestamp_column(all_results[coluna])

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
        'WorkOrder__r_Case_CaseNumber': 'numero_caso',
        'WorkOrder__r_Asset_Name': 'ativo',
        'WorkOrder__r_Case_Account_LXD_CPF__c': 'cpf_cnpj',
        'WorkOrder__r_TecnicoHabilitadoIndicarMovel__c': 'tecnico_habilitado_indicar_movel',
        'WorkOrder__r_ClienteAptoParaChip__c': 'cliente_aptos_para_chip',
        'WorkOrder__r_Indicacao_feita_pelo_tecnico__c': 'indicacao_feita_pelo_tecnico',
        'WorkOrder__r_CreatedBy_Name': 'criado_por',
        'WorkOrder__r_PontosAdicionais__c': 'pontos_mesh_instalar',
        'WorkOrder__r_PontosAdicionaisTV__c': 'pontos_tv_instalar',
        'WorkOrder__r_Chip__c': 'chip_entregar',
        'WorkOrder__r_Priority': 'prioridade',
        'ServiceTerritory_Name': 'territorio_servico',
        'WorkOrder__r_Agendamento_Fura_Fila__c': 'agendamento_fura_fila'
    }

    df_renomeado = all_results.rename(columns=colunas_de_para)

    # REMOVE DEFINITIVAMENTE A COLUNA FANTASMA DO SALESFORCE
    if 'WorkOrder__r_OLT__r' in df_renomeado.columns:
        df_renomeado = df_renomeado.drop(columns=['WorkOrder__r_OLT__r'])

    conn = conectar_mysql(
        host="172.29.5.3",
        database="db_Melhoria_continua_operacoes",
        user="Geovane.Faria",
        password="GeovaneDesk2@25"
    )

    if conn:
        print("Conexão estabelecida com sucesso!")
        df_renomeado.fillna("", inplace=True)

        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE uploadagendamentos_geovane;")
        conn.commit()

        sucesso = insert_dataframe_mysql_direct(
            df_renomeado,
            'uploadagendamentos_geovane',
            conn
        )

        cursor.execute("CALL processar_upload_agendamentos();")
        conn.commit()

        if sucesso:
            print("Dados inseridos com sucesso!")

        conn.close()


etl_geovane_base_original()
