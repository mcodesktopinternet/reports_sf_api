import pandas as pd
import datetime
def convert_timestamp_column(df):
    """
    Converte valores de timestamp em milissegundos ou datas para formato de data brasileira.
    
    Args:
        df: Coluna de DataFrame contendo timestamps em milissegundos ou datas
        
    Returns:
        pandas.Series: Coluna convertida para formato de data brasileira (dd/mm/aaaa hh:mm:ss)
    """
    # Verifica se os valores são apenas datas ou timestamps completos
    
    try:
       # quero"2025-08-12T10:00:00.000+0000"
       
        df = pd.to_datetime(df)


        # return (df - datetime.timedelta(hours=3)).dt.strftime("%d/%m/%Y %H:%M:%S")
        return (df - datetime.timedelta(hours=3)).dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Erro ao converter timestamp: {e}")
        # Se ainda falhar, retorna a série original
        return df