import pandas as pd
import requests
import json
import math
from datetime import datetime
from typing import Optional, List, Dict, Any
import warnings

# Módulos locais
from sf_auth import get_salesforce_token, get_auth_headers
from sf_query import get_all_query_results
from conectar_mysql import conectar_mysql, insert_dataframe_mysql_direct

# Desabilita avisos de InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

# ==============================================================================
# FUNÇÕES DE CONSULTA À API DESKTOP (Unificadas e melhoradas)
# ==============================================================================

def obter_token_oauth() -> Optional[str]:
    """Obtém token de acesso OAuth2 da API Desktop."""
    try:
        print(f"[{datetime.now()}] Iniciando obtenção do token OAuth da API Desktop...")
        url = "https://oauth.desktop.com.br/v2/master/token"
        payload = {"grant_type": "client_credentials", "client_id": "smartomni", "client_secret": "XwrP64b9FlEf6FfVYRqgT3JUPUzw72N1"}
        headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
        response = requests.post(url, verify=False, data=payload, headers=headers, timeout=600)
        response.raise_for_status()
        access_token = response.json().get('access_token')
        if access_token:
            print(f"[{datetime.now()}] Token OAuth obtido com sucesso.")
            return access_token
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] ERRO CRÍTICO ao obter token OAuth: {e}")
    return None

def consultar_cto_positions(numero_cto: str, sigla_cto: str, token: str) -> Optional[List[dict]]:
    """Consulta posições de uma CTO na API Desktop."""
    if not all([numero_cto, sigla_cto, token]): return None
    try:
        url = f"https://api-v2.desktop.com.br/resource-inventory/v1/ctos/{numero_cto}/positions?group={sigla_cto}"
        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
        response = requests.get(url, verify=False, headers=headers, timeout=600)
        if response.status_code == 404: # CTO não encontrada é comum, não é um erro crítico
            return None
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] AVISO: Falha na requisição para CTO {sigla_cto}-{numero_cto}: {e}")
        return None

# ==============================================================================
# FUNÇÃO PRINCIPAL DO ETL
# ==============================================================================

def etl_base_corrigida():
    # 1. AUTENTICAÇÃO E CONFIGURAÇÃO
    # =================================================
    domain = "https://desktopsa.my.salesforce.com"
    client_id = "3MVG99gP.VbJma8WslD5o_qMP_J4iVg1FUTtQzWQ4_TxpBoZWi6MyFHtxCZMZFOCk2FsJbw2wNynhmaD7c8Uv"
    client_secret = "E31D29879301886FE62D60D79898EE335B488E0A37E4DDB9874D12F12C29719E"
    username = "plan.relatorios@desktop.net.br"
    password = "PlanRelatorios@2025PwdjW2bnaqa3Z8O7KwuqVoLp"

    print(f"[{datetime.now()}] Iniciando autenticação com o Salesforce...")
    token_data = get_salesforce_token(domain, client_id, client_secret, username, password)
    headers = get_auth_headers(token_data)
    print(f"[{datetime.now()}] Autenticação com Salesforce bem-sucedida.")

    token_api_desktop = obter_token_oauth()
    if not token_api_desktop:
        print(f"[{datetime.now()}] CRÍTICO: Não foi possível obter o token da API Desktop. Encerrando.")
        return

    # 2. EXTRAÇÃO DE DADOS DO SALESFORCE
    # =================================================
    query = """
    SELECT Id, WorkOrder__r.Asset.SiglaCTO__c, WorkOrder__r.Asset.CaixaCTO__c, WorkOrder__r.Asset.PortaCTO__c,
           WorkOrder__r.dt_abertura__c, WorkOrder__r.LegacyId__c, WorkOrder__r.WorkOrderNumber, WorkOrder__r.Case.CaseNumber,
           AppointmentNumber, FirstScheduleDateTime__c, WorkOrder__r.DataAgendamento__c, ArrivalWindowStart_Gantt__c,
           ArrivalWindowEnd_Gantt__c, ScheduledStart_Gantt__c, SchedEndTime_Gantt__c, ActualStart_Gantt__c, ActualEnd_Gantt__c,
           TechnicianName__c, TechniciansCompany__c, WorkOrder__r.City, WorkOrder__r.Work_Type_WO__c, WorkOrder__r.Work_Subtype_WO__c,
           WorkOrder__r.Status, WorkOrder__r.CaseReason__c, WorkOrder__r.Submotivo__c, LowCodeFormula__c,
           WorkOrder__r.ReasonForCancellationWorkOrder__c, Reschedule_Reason_SA__c, WorkOrder__r.SuspensionReasonWo__c,
           WorkOrder__r.OLT__r.Name, WorkOrder__r.CTO__c, WorkOrder__r.Asset.Name, WorkOrder__r.LastModifiedDate,
           WorkOrder__r.Case.Account.LXD_CPF__c, FSL__Pinned__c, WorkOrder__r.IsRescheduledWo__c,
           WorkOrder__r.ConvenienciaCliente__c, WorkOrder__r.SolicitaAntecipacao__c, WorkOrder__r.HowManyTimesWo__c, WorkOrder__r.Subject
    FROM ServiceAppointment 
    WHERE SchedEndTime_Gantt__c >= 2025-11-03T00:00:00.000-03:00 
      AND WorkOrder__r.Subject LIKE '%INC%' AND Status = 'Concluída' AND WorkOrder__r.Work_Type_WO__c  = 'Manutenção'
    """
    
    print(f"[{datetime.now()}] Executando a consulta no Salesforce...")
    resultado = get_all_query_results(domain, headers, query)
    total_registros = len(resultado)
    print(f"[{datetime.now()}] Consulta retornou {total_registros} registros.")
    if not total_registros:
        print(f"[{datetime.now()}] Nenhum registro encontrado. Encerrando.")
        return

    # 3. PREPARAÇÃO DO BANCO DE DADOS
    # =================================================
    conn = conectar_mysql(host="172.29.5.3", database="db_Melhoria_continua_operacoes", user="Geovane.Faria", password="GeovaneDesk2@25")
    if not conn:
        print(f"[{datetime.now()}] CRÍTICO: Falha na conexão com MySQL. Encerrando.")
        return
    
    colunas_mysql = set()
    try:
        with conn.cursor() as cursor:
            print(f"[{datetime.now()}] Conexão com MySQL estabelecida. Verificando colunas...")
            cursor.execute("SHOW COLUMNS FROM ticket;")
            colunas_mysql = {row[0] for row in cursor.fetchall()}
            
            print(f"[{datetime.now()}] Limpando a tabela 'ticket' (TRUNCATE)...")
            cursor.execute("TRUNCATE TABLE ticket;")
        conn.commit()
        print(f"[{datetime.now()}] Tabela 'ticket' limpa.")
    except Exception as e:
        print(f"[{datetime.now()}] ERRO durante preparação do banco: {e}")
        conn.close()
        return

    # 4. TRANSFORMAÇÃO E PROCESSAMENTO EM LOTES
    # =================================================
    df_total = pd.json_normalize(resultado, sep="_")
    df_total = df_total.drop(columns=[c for c in df_total.columns if 'attributes_' in c], errors='ignore')
    
    # Mapeamento de Colunas (única vez)
    colunas_de_para = {
        'Id': 'id_service_appointment', 'WorkOrder__r_Asset_SiglaCTO__c': 'sigla_cto', 'WorkOrder__r_Asset_CaixaCTO__c': 'caixa_cto', 'WorkOrder__r_Asset_PortaCTO__c': 'porta_cto',
        'WorkOrder__r_dt_abertura__c': 'dt_abertura', 'WorkOrder__r_LegacyId__c': 'codigo_cliente', 'WorkOrder__r_WorkOrderNumber': 'numero_ordem_trabalho',
        'WorkOrder__r_Case_CaseNumber': 'caso', 'AppointmentNumber': 'numero_compromisso', 'FirstScheduleDateTime__c': 'data_primeiro_agendamento',
        'WorkOrder__r_DataAgendamento__c': 'data_agendamento', 'ArrivalWindowStart_Gantt__c': 'inicio_janela_chegada', 'ArrivalWindowEnd_Gantt__c': 'termino_janela_chegada',
        'ScheduledStart_Gantt__c': 'inicio_agendado', 'SchedEndTime_Gantt__c': 'termino_agendado', 'ActualStart_Gantt__c': 'inicio_servico',
        'ActualEnd_Gantt__c': 'termino_servico', 'TechnicianName__c': 'nome_tecnico', 'TechniciansCompany__c': 'empresa_tecnico',
        'WorkOrder__r_City': 'cidade', 'WorkOrder__r_Work_Type_WO__c': 'tipo_trabalho', 'WorkOrder__r_Work_Subtype_WO__c': 'subtipo_trabalho',
        'WorkOrder__r_Status': 'status', 'WorkOrder__r_CaseReason__c': 'motivo_caso', 'WorkOrder__r_Submotivo__c': 'submotivo_caso', 'LowCodeFormula__c': 'codigo_baixa',
        'WorkOrder__r_ReasonForCancellationWorkOrder__c': 'motivo_cancelamento', 'Reschedule_Reason_SA__c': 'motivo_reagendamento',
        'WorkOrder__r_SuspensionReasonWo__c': 'motivo_suspensao', 'WorkOrder__r_OLT__r_Name': 'olt', 'WorkOrder__r.CTO__c': 'cto',
        'WorkOrder__r_Asset_Name': 'ativo', 'WorkOrder__r_LastModifiedDate': 'last_modified_date', 'WorkOrder__r_Case_Account_LXD_CPF__c': 'cpf_cnpj',
        'FSL__Pinned__c': 'pinned', 'WorkOrder__r_IsRescheduledWo__c': 'foi_reagendado', 'WorkOrder__r_ConvenienciaCliente__c': 'conveniencia_cliente',
        'WorkOrder__r_SolicitaAntecipacao__c': 'solicita_antecipacao', 'WorkOrder__r_HowManyTimesWo__c': 'quantas_vezes', 'WorkOrder__r_Subject': 'subject'
    }
    df_total.rename(columns=colunas_de_para, inplace=True)

    # Enriquecimento com API
    print(f"[{datetime.now()}] Buscando dados de conexão na API para {total_registros} registros...")
    api_data = []
    for _, row in df_total.iterrows():
        sigla, caixa, porta_str = row.get('sigla_cto'), row.get('caixa_cto'), row.get('porta_cto')
        status, inicio, fim, duracao = 'Dados insuficientes', None, None, None
        
        if sigla and caixa and porta_str:
            try: porta_num = int(float(porta_str))
            except (ValueError, TypeError): status = 'Porta inválida'
            else:
                resp_api = consultar_cto_positions(caixa, sigla, token_api_desktop)
                if isinstance(resp_api, list):
                    status = 'Porta não encontrada'
                    for porta_info in resp_api:
                        # !!! ATENÇÃO !!! Verifique se a chave correta é 'port_number' ou 'position'
                        if porta_info.get('port_number') == porta_num: # ou porta_info.get('position')
                            status = porta_info.get('status', 'Status não informado')
                            inicio_str, fim_str = porta_info.get('last_connection_start'), porta_info.get('last_connection_stop')
                            try:
                                if inicio_str:
                                    dt_inicio = datetime.strptime(inicio_str, '%d/%m/%Y - %H:%M:%S')
                                    inicio = dt_inicio
                                    if status == 'Conectado':
                                        d, r = divmod((datetime.now() - dt_inicio).total_seconds(), 86400)
                                        h, r = divmod(r, 3600)
                                        m, _ = divmod(r, 60)
                                        duracao = f"{int(d)}d {int(h)}h {int(m)}m"
                                if fim_str: 
                                    dt_fim = datetime.strptime(fim_str, '%d/%m/%Y - %H:%M:%S')
                                    fim = dt_fim
                            except (ValueError, TypeError): pass
                            break
                else: status = 'CTO não encontrada'
        api_data.append([status, inicio, fim, duracao])
    
    df_total[['status_cliente_api', 'ultima_conexao_inicio', 'ultima_conexao_fim', 'tempo_conectado']] = api_data
    print(f"[{datetime.now()}] Busca de dados na API concluída.")

    # Tratamento de Tipos de Dados (Forma Segura)
    colunas_data = [
        'dt_abertura', 'data_agendamento', 'inicio_janela_chegada', 'termino_janela_chegada', 
        'inicio_agendado', 'termino_agendado', 'inicio_servico', 'termino_servico', 
        'last_modified_date', 'ultima_conexao_inicio', 'ultima_conexao_fim'
    ]
    for col in colunas_data:
        if col in df_total.columns:
            # Converte para datetime, tratando erros
            df_total[col] = pd.to_datetime(df_total[col], errors='coerce', utc=True)
            # Converte para o fuso de São Paulo
            df_total[col] = df_total[col].dt.tz_convert('America/Sao_Paulo')
            # Formata como string para inserção segura no MySQL
            df_total[col] = df_total[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    colunas_numericas = ['quantas_vezes', 'pinned', 'conveniencia_cliente', 'solicita_antecipacao']
    for col in colunas_numericas:
        if col in df_total.columns:
            df_total[col] = pd.to_numeric(df_total[col], errors='coerce').fillna(0).astype(int)

    # Converte todos os nulos restantes (NaN, NaT, etc.) para None
    df_final = df_total.astype(object).where(pd.notnull(df_total), None)

    # 5. CARGA NO MYSQL
    # =================================================
    df_para_inserir = df_final[[c for c in df_final.columns if c in colunas_mysql]]
    
    print(f"[{datetime.now()}] Colunas a serem inseridas: {list(df_para_inserir.columns)}")
    print(f"[{datetime.now()}] Iniciando inserção de {len(df_para_inserir)} registros...")
    
    try:
        insert_dataframe_mysql_direct(df_para_inserir, 'ticket', conn)
        print(f"[{datetime.now()}] Inserção concluída com sucesso.")
    except Exception as e:
        # !!! IMPORTANTE !!! Esta mensagem mostrará o erro exato do banco de dados.
        print(f"[{datetime.now()}] FALHA CRÍTICA DURANTE A INSERÇÃO: {e}")
    finally:
        conn.close()
        print(f"[{datetime.now()}] Conexão com o MySQL fechada. Processo concluído.")

if __name__ == "__main__":
    etl_base_corrigida()