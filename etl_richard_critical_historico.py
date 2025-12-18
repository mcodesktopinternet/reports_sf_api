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
    SELECT Id , WorkOrder.WorkOrderNumber, Field , CreatedBy.Name,  CreatedDate, OldValue, NewValue FROM WorkOrderHistory where (Field = 'Priority' or Field = 'Priority2__c') and CreatedDate >=2025-08-11T00:00:00.000-03:00
    """
        


    resultado = get_all_query_results(instance_url="https://desktopsa.my.salesforce.com" , auth_headers=headers, query=query)

    all_results = pd.json_normalize(resultado, sep="_")

    all_results.fillna("", inplace=True)

    #quero que remova todas as colunas que tem o nome attributes
    all_results = all_results.drop(columns=[col for col in all_results.columns if 'attributes_' in col])

    colunas_timestamp = [
        'CreatedDate',
    ]

    for coluna in colunas_timestamp:
            all_results[coluna] = convert_timestamp_column(all_results[coluna])



    colunas_de_para = {
        'Id': 'id',
        'WorkOrder_WorkOrderNumber': 'numero_ordem_trabalho',
        'Field': 'campo',
        'CreatedBy_Name': 'criado_por',
        'OldValue': 'valor_anterior',
        'NewValue': 'valor_novo',
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
        query = "TRUNCATE TABLE historico_ordem_servico_casos_criticos;"
        cursor.execute(query)
        conn.commit()

        # Inserir DataFrame
        sucesso = insert_dataframe_mysql_direct(df_renomeado, 'historico_ordem_servico_casos_criticos', conn)
        
        if sucesso:
            print("Dados inseridos com sucesso!")
        
        conn.close()
    
etl_richard_critical()