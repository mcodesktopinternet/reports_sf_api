import os
import time
import win32com.client

def log_etapa(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def esperar_excel_pronto(excel, timeout=300, intervalo=10):
    log_etapa("Verificando se o Excel está pronto...")
    tempo_inicio = time.time()
    while time.time() - tempo_inicio < timeout:
        try:
            _ = excel.ActiveWorkbook.Name
            log_etapa("Excel está pronto para aceitar comandos.")
            return True
        except Exception:
            log_etapa(f"Excel ainda ocupado. Aguardando {intervalo} segundos...")
            time.sleep(intervalo)
    log_etapa("Timeout: Excel não ficou pronto dentro do tempo limite.")
    return False

def atualizar_excel():
    excel = None
    wb = None
    try:
        os.system("taskkill /f /im excel.exe >nul 2>&1")
        log_etapa("Encerrando processos Excel remanescentes antes de iniciar.")

        excel_path = r"C:\COD\ARQUIVOS\COMPROMISSO_D0.xlsx"

        if not os.path.exists(excel_path):
            log_etapa(f"Erro: O arquivo {excel_path} não foi encontrado.")
            return

        timestamp_antes = os.path.getmtime(excel_path)
        log_etapa(f"Timestamp do arquivo antes da atualização: {time.ctime(timestamp_antes)}")

        log_etapa("Iniciando Excel...")
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = True
        excel.DisplayAlerts = False
        excel.AskToUpdateLinks = False  # 🔥 ESSENCIAL PARA ELIMINAR POPUP

        # --- ABRE O ARQUIVO SEM ATUALIZAR VÍNCULOS ---
        log_etapa("Abrindo o arquivo SEM atualizar vínculos externos...")
        wb = excel.Workbooks.Open(
        Filename=excel_path,
        UpdateLinks=3,              # 3 = atualiza links externos e conexões automaticamente
        ReadOnly=False,             # abre para leitura/escrita
        IgnoreReadOnlyRecommended=True,
        Notify=False,               # não pergunta nada
        CorruptLoad=0,              # xlNormalLoad (comportamento normal)
        # O parâmetro mais importante aqui é UpdateLinks=3
        )

        # --- DESATIVA ATUALIZAÇÕES DE LINKS EXTERNOS ---
        log_etapa("Desativando atualizações de vínculos externos...")

        try:
            wb.UpdateLinks = 0
        except:
            pass

        # --- DESATIVA ATUALIZAÇÃO AUTOMÁTICA DE CONEXÕES ---
        log_etapa("Desativando atualização automática das conexões...")
        for conn in wb.Connections:
            try:
                conn.RefreshOnFileOpen = False

                if hasattr(conn, "ODBCConnection"):
                    conn.ODBCConnection.BackgroundQuery = False

                log_etapa(f"Conexão '{conn.Name}' configurada corretamente.")
            except Exception as e:
                log_etapa(f"Falha ao ajustar conexão '{conn.Name}': {e}")

        # --- ATUALIZA MANUALMENTE TODAS AS CONEXÕES ---
        log_etapa("Atualizando manualmente todas as conexões...")
        for connection in wb.Connections:
            try:
                connection.Refresh()
                log_etapa(f"Conexão '{connection.Name}' atualizada.")
            except Exception as e:
                log_etapa(f"Erro atualizando '{connection.Name}': {e}")
            time.sleep(2)

        # Atualizar tudo
        log_etapa("Executando RefreshAll...")
        excel.Application.CommandBars.ExecuteMso("RefreshAll")

        log_etapa("Aguardando 10 segundos pela atualização inicial...")
        time.sleep(10)

        if not esperar_excel_pronto(excel):
            raise Exception("Excel não ficou pronto após atualizações.")

        # --- VALIDAR ALTERAÇÃO ---
        try:
            sheet = wb.Worksheets("Dados")
            valor_antes = sheet.Range("A1").Value
            log_etapa(f"Valor A1 antes: {valor_antes}")

            excel.Application.CommandBars.ExecuteMso("RefreshAll")
            time.sleep(5)

            valor_depois = sheet.Range("A1").Value
            log_etapa(f"Valor A1 depois: {valor_depois}")

            if valor_antes == valor_depois:
                log_etapa("Aviso: A validação indicou que o valor não mudou.")
            else:
                log_etapa("Validação OK: valor alterado!")
        except:
            log_etapa("Aviso: Não foi possível validar A1 (aba pode não existir).")

        log_etapa("Aguardando 5 segundos adicionais antes de salvar...")
        time.sleep(5)

        log_etapa("Salvando o arquivo...")
        wb.Save()

        timestamp_depois = os.path.getmtime(excel_path)
        log_etapa(f"Timestamp após salvar: {time.ctime(timestamp_depois)}")

    except Exception as e:
        log_etapa(f"Erro geral: {e}")

    finally:
        if wb:
            try:
                wb.Close(SaveChanges=True)
                log_etapa("Arquivo fechado.")
            except:
                log_etapa("Erro ao fechar o arquivo.")

        if excel:
            try:
                excel.Quit()
                log_etapa("Excel encerrado.")
            except:
                log_etapa("Erro ao encerrar Excel.")

        os.system("taskkill /f /im excel.exe >nul 2>&1")
        log_etapa("Processos Excel forçados a encerrar.")

# EXECUTA
if __name__ == "__main__":
    atualizar_excel()
