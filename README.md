# 🚀 reports_sf_api — ETL Salesforce ↔ MySQL

Este repositório centraliza as rotinas de **Extração, Transformação e Carga (ETL)** integrando dados do **Salesforce**, enriquecimento via **API Desktop** e armazenamento estruturado em **MySQL**. O projeto foi desenhado para ser modular, seguro e escalável, utilizando as melhores práticas de engenharia de dados com Python e Pandas.

---

## 📋 Visão Geral

O sistema automatiza a coleta de dados operacionais críticos, permitindo que o time de negócios tenha acesso a informações atualizadas sobre:
- **Agendamentos e Compromissos:** Gestão de `ServiceAppointment`.
- **Histórico de Operações:** Auditoria de mudanças em `WorkOrder` e `ServiceAppointment`.
- **Casos Críticos:** Monitoramento de prioridades e incidentes graves.
- **Enriquecimento de Rede:** Integração com a API Desktop para dados técnicos de CTO (Portas, Status, Conectividade).

---

## ⚙️ Arquitetura e Fluxo de Dados

O fluxo segue uma lógica linear de processamento, garantindo a integridade dos dados desde a origem até o destino final.

![Fluxo ETL](https://private-us-east-1.manuscdn.com/sessionFile/IWnwzHEvpkIzE0WuMIaFBo/sandbox/xxRCcpGKzxk0NKpQtLa9IW-images_1768917550246_na1fn_L2hvbWUvdWJ1bnR1L2Zsb3djaGFydA.png?Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cHM6Ly9wcml2YXRlLXVzLWVhc3QtMS5tYW51c2Nkbi5jb20vc2Vzc2lvbkZpbGUvSVdud3pIRXZwa0l6RTBXdU1JYUZCby9zYW5kYm94L3h4UkNjcEdLenhrME5LcFF0TGE5SVctaW1hZ2VzXzE3Njg5MTc1NTAyNDZfbmExZm5fTDJodmJXVXZkV0oxYm5SMUwyWnNiM2RqYUdGeWRBLnBuZyIsIkNvbmRpdGlvbiI6eyJEYXRlTGVzc1RoYW4iOnsiQVdTOkVwb2NoVGltZSI6MTc5ODc2MTYwMH19fV19&Key-Pair-Id=K2HSFNDJXOU9YS&Signature=vvxI3wyG4oA5D5JZfmf0AiEVshyWjraE~87pIU7gK~j7NxZAIoVORdTGIBGkWpgQPa93fDyy3P8cpN~pz-FrsGLg2oH62tHQ1Ko5ahBI5PblMkrBQ5UjdOMam-ut1KIte6Nc4jqWo4bwZiPk71QomCU7MFV3toLeMs9BGzmf0MowdMmduBbxViZ~bV~uSTVgfpzhYk2l-FNvsYzxRXZYeJPfEAlZZhO5nV0JWlEoaqxrOo1eppDFw0QFTnm-NJ0HaUyYjAPVegYfsNeK8j8vzOrT~WdvEGjek1PAVdl9R8v5MD6yhKS34sjSaPbZBrvEuTj-yF06nO5f1sSJIVFHXQ__)

### Etapas do Processo:
1.  **Configuração:** Carregamento de variáveis de ambiente via `.env`.
2.  **Autenticação:** Handshake OAuth2 com Salesforce e Desktop API.
3.  **Extração:** Consultas SOQL otimizadas com suporte a paginação.
4.  **Transformação:** 
    - Normalização de JSON aninhado.
    - Limpeza de metadados (`attributes_*`).
    - Padronização de tipos (Datas, Timestamps, Numéricos).
5.  **Enriquecimento (Opcional):** Cruzamento de dados com APIs externas.
6.  **Carga:** Persistência no MySQL utilizando estratégias de `TRUNCATE` (Snapshot) ou `UPSERT` (Incremental).

---

## 📂 Estrutura do Projeto

A organização do código separa as responsabilidades de conexão, utilitários e lógica de negócio:

| Arquivo | Descrição |
| :--- | :--- |
| `sf_auth.py` | Gerenciamento de tokens e autenticação Salesforce. |
| `sf_query.py` | Motor de execução SOQL e tratamento de paginação. |
| `conectar_mysql.py` | Abstração de conexão e métodos de inserção em lote. |
| `convert_timestamp_column.py` | Utilitário para padronização de fusos horários e formatos de data. |
| `etl_*.py` | Scripts específicos para cada pipeline de dados. |

---

## 🚀 Scripts de ETL Disponíveis

Abaixo, o detalhamento das rotinas implementadas:

| Script | Fonte (Salesforce) | Destino (MySQL) | Estratégia de Carga |
| :--- | :--- | :--- | :--- |
| **Agendamentos** | `ServiceAppointment` | `uploadagendamentos_geovane` | `TRUNCATE + INSERT` |
| **Casos Críticos** | `Priority = 'Critical'` | `servicos_tecnicos` | `TRUNCATE + INSERT` |
| **Histórico WO** | `WorkOrderHistory` | `historico_ordem_servico` | `TRUNCATE + INSERT` |
| **Cancelamentos** | `SA History (Status)` | `service_appointment_cancel` | `BATCH INSERT` |
| **Auditoria SA** | `ServiceAppointmentHistory`| `service_appointment_history` | `UPSERT (ID)` |
| **Tickets/CTO** | `SA + Desktop API` | `ticket` | `TRUNCATE + INSERT` |

---

## 🛠️ Configuração e Instalação

### Pré-requisitos
- **Python 3.10+**
- Acesso de rede às APIs e ao Banco de Dados.

### Instalação
1. Clone o repositório e acesse a pasta.
2. Crie e ative um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   .\venv\Scripts\Activate.ps1 # Windows
   ```
3. Instale as dependências:
   ```bash
   pip install pandas requests python-dotenv mysql-connector-python
   ```

### Variáveis de Ambiente (`.env`)
Crie um arquivo `.env` baseado no modelo abaixo para garantir o funcionamento das rotinas:

```env
# Configurações Gerais
LOG_LEVEL="INFO"
TZ_LOCAL="America/Sao_Paulo"

# Salesforce
SF_DOMAIN="https://sua-instancia.salesforce.com"
SF_CLIENT_ID="seu_client_id"
SF_CLIENT_SECRET="seu_client_secret"
SF_USERNAME="usuario@empresa.com"
SF_PASSWORD="senha_com_token"

# Desktop API
DESKTOP_OAUTH_URL="https://oauth.desktop.com.br/v2/token"
DESKTOP_CLIENT_ID="seu_id"
DESKTOP_CLIENT_SECRET="seu_secret"

# MySQL
MYSQL_HOST="127.0.0.1"
MYSQL_DATABASE="db_reports"
MYSQL_USER="admin"
MYSQL_PASSWORD="password"
```

---

## 🛡️ Segurança e Boas Práticas

- **Credenciais:** Nunca commite o arquivo `.env`. Utilize o `.env.example` como referência.
- **Tratamento de Nulos:** O sistema utiliza `astype(object)` e `where(pd.notna(...), None)` para garantir que valores nulos do Pandas sejam interpretados como `NULL` no MySQL.
- **Logs:** Monitore a execução através das saídas de log para identificar falhas de autenticação ou timeouts de API.

---

## ❓ Troubleshooting

- **Erro 403 no Git:** Verifique suas permissões de escrita ou utilize o fluxo de Fork.
- **Senha Expirada (SF):** O erro `INVALID_OPERATION_WITH_EXPIRED_PASSWORD` exige o reset da senha no portal Salesforce.
- **Conexão MySQL:** Certifique-se de que o IP da máquina de execução está liberado no firewall do banco de dados.

---
> **Nota:** Este projeto é de uso interno. Para suporte, entre em contato com o administrador do sistema.
