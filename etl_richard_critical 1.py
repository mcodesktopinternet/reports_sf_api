from sf_auth import get_salesforce_token , get_auth_headers
from sf_query import get_all_query_results
import pandas as pd
# import json
from convert_timestamp_column import convert_timestamp_column
from conectar_mysql import conectar_mysql
from conectar_mysql import insert_dataframe_mysql_direct


def etl_richard_critical():


    domain="https://desktopsa.my.salesforce.com"
    client_id="3MVG99gP.VbJma8WslD5o_qMP_J4iVg1FUTtQzWQ4_TxpBoZWi6MyFHtxCZMZFOCk2FsJbw2wNynhmaD7c8Uv"
    client_secret="E31D29879301886FE62D60D79898EE335B488E0A37E4DDB9874D12F12C29719E"
    username="plan.relatorios@desktop.net.br"
    password="PlanRelatorios@2025PwdjW2bnaqa3Z8O7KwuqVoLp"

    # domain="https://desktopsa--partial.sandbox.my.salesforce.com"
    # client_id="3MVG96WMoUG6yB9I08P9CSL6cWR7Z8a1BeYJ4eTuaTZsGdqfcQf4jEI1gKO8b9xEeJ_TqbMuZ4hI4102covli"
    # client_secret="1D2CD4C19998FD5FFF3CB2EABD6424455B0D38DDB3F176BCD15A607299F51715"
    # username="danilo.silva@desktop.tec.br"
    # password="Desktop@2025T2D1mKr2AfuVPh9i8I9chw9u"

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
        WorkOrder__r.OLT__r.Name ,
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
        WorkOrder__r.Agendamento_Fura_Fila__c
    FROM ServiceAppointment
    WHERE WorkOrder__r.CreatedDate >=2025-08-11T00:00:00.000-03:00 and WorkOrder__r.Priority = 'Critical' and WorkOrder__r.Case.Account.LXD_CPF__c != ''
    """
        


    resultado = get_all_query_results(instance_url="https://desktopsa.my.salesforce.com" , auth_headers=headers, query=query)

    all_results = pd.json_normalize(resultado, sep="_")

    all_results.fillna("", inplace=True)

    #quero que remova todas as colunas que tem o nome attributes
    all_results = all_results.drop(columns=[col for col in all_results.columns if 'attributes_' in col])

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
        # Adicionado um if para evitar erro caso a consulta falhe e a coluna não exista
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
        'WorkOrder__r_ReasonOfCriticality__c': 'motivo_criticidade',
        'WorkOrder__r_ObservationOnPriority__c': 'observacao_prioridade',
        'WorkOrder__r_Description': 'descricao',
        'WorkOrder__r_Case_QuantidadeChips__c': 'quantidade_chips',
        'WorkOrder__r_Case_PontosAdicionais__c': 'pontos_mesh_instalar',
        'WorkOrder__r_Case_PontosAdicionaisTV__c': 'pontos_tv_instalar',
        'WorkOrder__r_Case_CaseNumber': 'numero_caso',
        'WorkOrder__r_Asset_Name': 'ativo',
        'WorkOrder__r_Case_Lxd_observation__c': 'observacao',
        'WorkOrder__r_Case_Owner_Name': 'proprietario_caso', # <-- CORREÇÃO APLICADA AQUI
        'WorkOrder__r_Case_Area_de_atendimento__c': 'grupo',
        'WorkOrder__r_Case_Account_LXD_CPF__c': 'cpf_cnpj',
        'WorkOrder__r_Agendamento_Fura_Fila__c': 'agendamento_fura_fila'
    }

    # Renomeie as colunas do DataFrame
    df_renomeado = all_results.rename(columns=colunas_de_para)

    # df_renomeado.to_csv('richard.csv', index=False, encoding='utf-8' , sep=';')
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
        query = "TRUNCATE TABLE servicos_tecnicos;"
        cursor.execute(query)
        conn.commit()

        # Inserir DataFrame
        sucesso = insert_dataframe_mysql_direct(df_renomeado, 'servicos_tecnicos', conn)
        
        if sucesso:
            print("Dados inseridos com sucesso!")
        
        conn.close()
    
etl_richard_critical()