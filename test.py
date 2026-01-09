# database.py - Módulo de conexão e operações com a base de dados SQL Server

import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os
from typing import Optional, Tuple, List, Dict, Any
import json

# Variável global para a conexão
_connection = None
_connection_info = None


# ============================================================================
# CONFIGURAÇÃO DE CONEXÃO
# ============================================================================

def connect_to_db(server: str, database: str, username: str = None, password: str = None, port: str = None) -> Tuple[
    bool, str, dict]:
    """
    Estabelece conexão com a base de dados SQL Server

    Args:
        server: Nome/IP do servidor
        database: Nome da base de dados
        username: Nome de utilizador (opcional para Windows Authentication)
        password: Password (opcional para Windows Authentication)
        port: Porta (opcional, default 1433)

    Returns:
        Tuple: (success, message, connection_info)
    """
    global _connection, _connection_info

    try:
        # Construir string de conexão
        if username and password:
            # SQL Server Authentication
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server}"
            if port:
                conn_str += f",{port}"
            conn_str += f";DATABASE={database};UID={username};PWD={password}"
        else:
            # Windows Authentication
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server}"
            if port:
                conn_str += f",{port}"
            conn_str += f";DATABASE={database};Trusted_Connection=yes"

        # Tentar conexão
        _connection = pyodbc.connect(conn_str)
        _connection.autocommit = False

        # Testar conexão
        cursor = _connection.cursor()
        cursor.execute("SELECT @@VERSION")
        sql_version = cursor.fetchone()[0]

        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM sys.tables")
        table_count = cursor.fetchone()[0]

        cursor.close()

        _connection_info = {
            'server': server,
            'database_name': db_name,
            'sql_version': sql_version.split('\n')[0],
            'table_count': table_count
        }

        return True, "Conexão estabelecida com sucesso!", _connection_info

    except pyodbc.InterfaceError as e:
        return False, f"Erro de interface ODBC: {str(e)}", None
    except pyodbc.OperationalError as e:
        return False, f"Erro operacional: {str(e)}", None
    except pyodbc.DatabaseError as e:
        return False, f"Erro de base de dados: {str(e)}", None
    except Exception as e:
        return False, f"Erro inesperado: {str(e)}", None


def get_connection():
    """Retorna a conexão atual ou estabelece uma nova se necessário"""
    global _connection
    if _connection is None:
        # Tentar reconectar com parâmetros padrão
        # Ajuste conforme sua configuração
        server = "INESTRIPECA\\SQLEXPRESS"
        database = "NEO_Monitoring"
        success, message, info = connect_to_db(server, database)
        if not success:
            print(f"Falha ao reconectar: {message}")
            return None
    return _connection


def close_connection():
    """Fecha a conexão com a base de dados"""
    global _connection
    if _connection:
        try:
            _connection.close()
            _connection = None
            print("Conexão fechada.")
        except:
            pass


# ============================================================================
# FUNÇÕES DE CONSULTA E METADADOS
# ============================================================================

def get_all_tables() -> List[str]:
    """Retorna lista de todas as tabelas na base de dados"""
    try:
        conn = get_connection()
        if not conn:
            return []

        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)

        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables

    except Exception as e:
        print(f"Erro ao obter tabelas: {str(e)}")
        return []


def get_table_structure(table_name: str) -> List[Dict[str, Any]]:
    """Retorna a estrutura (colunas) de uma tabela"""
    try:
        conn = get_connection()
        if not conn:
            return []

        cursor = conn.cursor()

        # Obter informações das colunas
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IsIdentity
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, table_name)

        columns = []
        for row in cursor.fetchall():
            col_name, data_type, char_length, nullable, is_identity = row

            # Determinar tipo SQL
            if char_length and char_length > 0:
                sql_type = f"{data_type}({char_length})"
            else:
                sql_type = data_type

            columns.append({
                'name': col_name,
                'type': sql_type,
                'nullable': nullable == 'YES',
                'identity': bool(is_identity)
            })

        cursor.close()
        return columns

    except Exception as e:
        print(f"Erro ao obter estrutura da tabela {table_name}: {str(e)}")
        return []


def get_primary_key(table_name: str) -> Optional[str]:
    """Retorna o nome da chave primária de uma tabela"""
    try:
        conn = get_connection()
        if not conn:
            return None

        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = ?
              AND CONSTRAINT_NAME IN (
                  SELECT CONSTRAINT_NAME 
                  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
                  WHERE TABLE_NAME = ? 
                    AND CONSTRAINT_TYPE = 'PRIMARY KEY'
              )
        """, table_name, table_name)

        result = cursor.fetchone()
        cursor.close()

        return result[0] if result else None

    except Exception as e:
        print(f"Erro ao obter chave primária: {str(e)}")
        return None


# ============================================================================
# OPERAÇÕES CRUD
# ============================================================================

def create_table_in_db(table_name: str, columns: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """Cria uma nova tabela na base de dados"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Construir comando CREATE TABLE
        create_sql = f"CREATE TABLE {table_name} (\n"

        column_defs = []
        for col in columns:
            col_name = col['name_entry'].get().strip()
            col_type = col['type_combo'].get()
            nullable = "NULL" if col['nullable_check'].get() else "NOT NULL"

            column_defs.append(f"    {col_name} {col_type} {nullable}")

        create_sql += ",\n".join(column_defs)
        create_sql += "\n)"

        # Executar comando
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()

        return True, f"Tabela '{table_name}' criada com sucesso!"

    except Exception as e:
        return False, f"Erro ao criar tabela: {str(e)}"


def insert_record_into_table(table_name: str, record_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Insere um novo registo na tabela"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Preparar dados
        fields = record_data['fields']
        columns = []
        values = []
        params = []

        for field in fields:
            field_name = field['name']
            field_value = field['entry'].get().strip()

            # Ignorar campos vazios (serão NULL)
            if field_value == "":
                continue

            columns.append(field_name)
            values.append("?")
            params.append(field_value)

        if not columns:
            return False, "Nenhum dado para inserir"

        # Construir e executar INSERT
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        cursor.execute(sql, params)

        conn.commit()
        cursor.close()

        return True, "Registo inserido com sucesso!"

    except Exception as e:
        return False, f"Erro ao inserir registo: {str(e)}"


def load_record_for_update_from_db(table_name: str, primary_key: str, record_id: str) -> Tuple[bool, Any]:
    """Carrega um registo específico para edição"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Obter todos os campos da tabela
        cursor.execute(f"SELECT TOP 1 * FROM {table_name} WHERE {primary_key} = ?", record_id)

        # Obter nomes das colunas
        columns = [column[0] for column in cursor.description]
        record = cursor.fetchone()

        cursor.close()

        if not record:
            return False, f"Registo com {primary_key}={record_id} não encontrado"

        # Converter para dicionário
        record_dict = {}
        for i, col_name in enumerate(columns):
            record_dict[col_name] = record[i]

        return True, record_dict

    except Exception as e:
        return False, f"Erro ao carregar registo: {str(e)}"


def update_record_in_table(table_name: str, update_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Atualiza um registo existente"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Preparar dados
        fields = update_data['fields']
        primary_key = update_data['primary_key']
        record_id = update_data['record_id']

        set_clauses = []
        params = []

        for field in fields:
            field_name = field['name']
            field_value = field['entry'].get().strip()

            # Atualizar apenas se o campo não estiver vazio
            if field_value != "":
                set_clauses.append(f"{field_name} = ?")
                params.append(field_value)

        if not set_clauses:
            return False, "Nenhum campo para atualizar"

        # Adicionar WHERE clause
        params.append(record_id)

        # Construir e executar UPDATE
        sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {primary_key} = ?"
        cursor.execute(sql, params)

        conn.commit()
        cursor.close()

        rows_affected = cursor.rowcount
        if rows_affected == 0:
            return False, f"Registo com {primary_key}={record_id} não encontrado"

        return True, f"Registo atualizado com sucesso! (Linhas afetadas: {rows_affected})"

    except Exception as e:
        return False, f"Erro ao atualizar registo: {str(e)}"


def delete_record_from_table(table_name: str, primary_key: str, record_id: str) -> Tuple[bool, str]:
    """Elimina um registo da tabela"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Executar DELETE
        sql = f"DELETE FROM {table_name} WHERE {primary_key} = ?"
        cursor.execute(sql, record_id)

        conn.commit()
        cursor.close()

        rows_affected = cursor.rowcount
        if rows_affected == 0:
            return False, f"Registo com {primary_key}={record_id} não encontrado"

        return True, f"Registo eliminado com sucesso! (Linhas afetadas: {rows_affected})"

    except Exception as e:
        return False, f"Erro ao eliminar registo: {str(e)}"


def query_table_with_filters(table_name: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[bool, Any]:
    """Consulta dados de uma tabela com filtros opcionais"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Construir query básica
        sql = f"SELECT * FROM {table_name}"
        params = []

        # Adicionar filtros se existirem
        if filters and 'column' in filters and 'value' in filters:
            column = filters['column']
            operator = filters.get('operator', '=')
            value = filters['value']

            # Para operador LIKE
            if operator.upper() == 'LIKE':
                sql += f" WHERE {column} LIKE ?"
                params.append(f"%{value}%")
            else:
                sql += f" WHERE {column} {operator} ?"
                params.append(value)

        # Executar query
        cursor.execute(sql, params)

        # Obter nomes das colunas
        columns = [column[0] for column in cursor.description]

        # Obter todos os registos
        records = cursor.fetchall()

        cursor.close()

        return True, (columns, records)

    except Exception as e:
        return False, f"Erro ao consultar tabela: {str(e)}"


# ============================================================================
# FUNÇÕES DE TRIGGERS
# ============================================================================

def setup_triggers() -> Tuple[bool, str]:
    """
    Configura os triggers a partir do ficheiro triggers.txt
    """
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        # Verificar se o ficheiro existe
        if not os.path.exists('triggers.txt'):
            return False, "Ficheiro triggers.txt não encontrado"

        # Ler o ficheiro
        with open('triggers.txt', 'r', encoding='utf-8') as file:
            sql_script = file.read()

        # Separar os comandos por GO (ou ponto e vírgula se não houver GO)
        if 'GO' in sql_script:
            commands = [cmd.strip() for cmd in sql_script.split('GO') if cmd.strip()]
        else:
            commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]

        cursor = conn.cursor()
        errors = []

        for i, command in enumerate(commands):
            try:
                if command:  # Ignorar comandos vazios
                    cursor.execute(command)
                    print(f"✓ Comando {i + 1} executado: {command[:50]}...")
            except Exception as e:
                error_msg = f"Erro no comando {i + 1}: {str(e)}"
                errors.append(error_msg)
                print(f"✗ {error_msg}")

        if errors:
            conn.rollback()
            return False, f"Erros encontrados:\n" + "\n".join(errors[:5])
        else:
            conn.commit()
            cursor.close()
            return True, "Triggers configurados com sucesso!"

    except Exception as e:
        return False, f"Erro ao configurar triggers: {str(e)}"


def check_triggers_exist() -> Tuple[bool, str]:
    """Verifica se os triggers necessários existem"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Verificar triggers específicos do sistema NEO
        cursor.execute("""
            SELECT 
                t.name AS trigger_name,
                OBJECT_NAME(t.parent_id) AS table_name,
                t.create_date,
                t.is_disabled,
                CASE WHEN t.is_disabled = 1 THEN 'Desativado' ELSE 'Ativo' END AS status
            FROM sys.triggers t
            WHERE t.name LIKE 'trg_%'
            ORDER BY t.name
        """)

        triggers = cursor.fetchall()
        cursor.close()

        if not triggers:
            return False, "Nenhum trigger configurado. Execute 'Configurar Triggers' primeiro."

        # Listar triggers encontrados
        trigger_list = "\n".join([f"• {t[0]} em {t[1]} ({t[4]})" for t in triggers])

        return True, f"Triggers encontrados ({len(triggers)}):\n{trigger_list}"

    except Exception as e:
        return False, f"Erro ao verificar triggers: {str(e)}"


def get_all_triggers() -> List[Dict[str, Any]]:
    """Retorna lista de todos os triggers"""
    try:
        conn = get_connection()
        if not conn:
            return []

        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                name,
                OBJECT_NAME(parent_id) AS table_name,
                create_date,
                is_disabled
            FROM sys.triggers
            WHERE parent_class = 1  -- Apenas triggers de tabela
            ORDER BY name
        """)

        triggers = []
        for row in cursor.fetchall():
            triggers.append({
                'name': row[0],
                'table': row[1],
                'created': row[2],
                'disabled': row[3] == 1
            })

        cursor.close()
        return triggers

    except Exception as e:
        print(f"Erro ao obter triggers: {str(e)}")
        return []


def enable_disable_trigger(trigger_name: str, enable: bool = True) -> Tuple[bool, str]:
    """Ativa ou desativa um trigger"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        if enable:
            cursor.execute(f"ENABLE TRIGGER {trigger_name} ON ALL SERVER")
            action = "ativado"
        else:
            cursor.execute(f"DISABLE TRIGGER {trigger_name} ON ALL SERVER")
            action = "desativado"

        conn.commit()
        cursor.close()

        return True, f"Trigger '{trigger_name}' {action} com sucesso!"

    except Exception as e:
        return False, f"Erro ao modificar trigger: {str(e)}"


def drop_trigger(trigger_name: str) -> Tuple[bool, str]:
    """Elimina um trigger"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Primeiro precisamos saber em qual tabela está o trigger
        cursor.execute("""
            SELECT OBJECT_NAME(parent_id) 
            FROM sys.triggers 
            WHERE name = ?
        """, trigger_name)

        result = cursor.fetchone()
        if not result:
            return False, f"Trigger '{trigger_name}' não encontrado"

        table_name = result[0]

        # Eliminar o trigger
        cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")

        conn.commit()
        cursor.close()

        return True, f"Trigger '{trigger_name}' eliminado da tabela '{table_name}'!"

    except Exception as e:
        return False, f"Erro ao eliminar trigger: {str(e)}"


# ============================================================================
# FUNÇÕES DE VIEWS
# ============================================================================

def setup_views() -> Tuple[bool, str]:
    """
    Configura as views a partir do ficheiro queries.txt
    """
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        # Verificar se o ficheiro existe
        if not os.path.exists('queries.txt'):
            return False, "Ficheiro queries.txt não encontrado"

        # Ler o ficheiro
        with open('queries.txt', 'r', encoding='utf-8') as file:
            sql_script = file.read()

        # Separar os comandos
        if 'GO' in sql_script:
            commands = [cmd.strip() for cmd in sql_script.split('GO') if cmd.strip()]
        else:
            commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]

        cursor = conn.cursor()
        errors = []
        views_created = 0

        for i, command in enumerate(commands):
            try:
                if command and command.upper().startswith('CREATE VIEW'):
                    cursor.execute(command)
                    views_created += 1
                    print(f"✓ View {i + 1} criada: {command[:50]}...")
            except Exception as e:
                error_msg = f"Erro no comando {i + 1}: {str(e)}"
                errors.append(error_msg)
                print(f"✗ {error_msg}")

        if errors:
            conn.rollback()
            return False, f"Erros encontrados:\n" + "\n".join(errors[:5])
        else:
            conn.commit()
            cursor.close()
            return True, f"{views_created} views configuradas com sucesso!"

    except Exception as e:
        return False, f"Erro ao configurar views: {str(e)}"


def execute_view(view_name: str) -> Tuple[bool, Any]:
    """Executa uma view e retorna os resultados"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Verificar se a view existe
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.VIEWS 
            WHERE TABLE_NAME = ?
        """, view_name)

        if not cursor.fetchone():
            return False, f"View '{view_name}' não encontrada"

        # Executar view
        cursor.execute(f"SELECT * FROM {view_name}")

        # Obter nomes das colunas
        columns = [column[0] for column in cursor.description]

        # Obter todos os registos
        records = cursor.fetchall()

        cursor.close()

        return True, (columns, records)

    except Exception as e:
        return False, f"Erro ao executar view: {str(e)}"


# ============================================================================
# FUNÇÕES DE ALERTAS
# ============================================================================

def get_active_alerts(filters: Optional[Dict[str, Any]] = None) -> Tuple[bool, Any]:
    """
    Obtém alertas ativos com filtros opcionais
    """
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Query base
        query = """
        SELECT 
            a.alert_id,
            ast.full_name AS asteroid_name,
            a.alert_date,
            a.priority_level,
            a.description,
            a.is_active
        FROM Alert a
        INNER JOIN Asteroid ast ON a.asteroid_id = ast.asteroid_id
        WHERE a.is_active = 1
        """

        params = []

        # Aplicar filtros
        if filters:
            if 'priority' in filters and filters['priority'] != 'Todas':
                priority_map = {
                    'Alta': [3, 4],  # Níveis 3 e 4
                    'Média': [2],  # Nível 2
                    'Baixa': [1]  # Nível 1
                }

                priority_levels = priority_map.get(filters['priority'], [])
                if priority_levels:
                    placeholders = ','.join(['?'] * len(priority_levels))
                    query += f" AND a.priority_level IN ({placeholders})"
                    params.extend(priority_levels)

        query += " ORDER BY a.priority_level DESC, a.alert_date DESC"

        cursor.execute(query, params)
        records = cursor.fetchall()

        # Obter nomes das colunas
        columns = [column[0] for column in cursor.description]

        cursor.close()

        return True, (columns, records)

    except Exception as e:
        return False, f"Erro ao obter alertas: {str(e)}"


def check_new_high_priority_alerts() -> Tuple[bool, Any]:
    """Verifica se há novos alertas de alta prioridade (níveis 3 e 4) nas últimas 24h"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        query = """
        SELECT COUNT(*) 
        FROM Alert 
        WHERE priority_level IN (3, 4) 
          AND is_active = 1
          AND alert_date > DATEADD(HOUR, -24, GETDATE())
        """

        cursor.execute(query)
        count = cursor.fetchone()[0]

        cursor.close()

        return True, count

    except Exception as e:
        return False, f"Erro ao verificar alertas: {str(e)}"


# ============================================================================
# FUNÇÕES DE NOTIFICAÇÕES
# ============================================================================

def create_notification_table() -> Tuple[bool, str]:
    """Cria a tabela de configurações de notificação se não existir"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Verificar se a tabela já existe
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'NotificationSettings'
        """)

        if cursor.fetchone():
            cursor.close()
            return True, "Tabela de notificações já existe"

        # Criar tabela
        cursor.execute("""
            CREATE TABLE NotificationSettings (
                setting_id INT IDENTITY(1,1) PRIMARY KEY,
                email NVARCHAR(255) NOT NULL UNIQUE,
                high_priority BIT DEFAULT 1,
                medium_priority BIT DEFAULT 0,
                low_priority BIT DEFAULT 0,
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE()
            )
        """)

        # Criar índice no email
        cursor.execute("CREATE INDEX idx_email ON NotificationSettings(email)")

        conn.commit()
        cursor.close()

        return True, "Tabela de notificações criada com sucesso!"

    except Exception as e:
        return False, f"Erro ao criar tabela de notificações: {str(e)}"


def get_notification_settings() -> Tuple[bool, Any]:
    """Obtém todas as configurações de notificação"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                email,
                high_priority,
                medium_priority,
                low_priority,
                updated_at
            FROM NotificationSettings
            ORDER BY updated_at DESC
        """)

        records = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

        cursor.close()

        return True, (columns, records)

    except Exception as e:
        return False, f"Erro ao obter configurações: {str(e)}"


def load_notification_settings_for_email(email: str) -> Tuple[bool, Any]:
    """Carrega as configurações de notificação para um email específico"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                high_priority,
                medium_priority,
                low_priority
            FROM NotificationSettings
            WHERE email = ?
        """, email)

        result = cursor.fetchone()
        cursor.close()

        if result:
            return True, {
                'high_priority': bool(result[0]),
                'medium_priority': bool(result[1]),
                'low_priority': bool(result[2])
            }
        else:
            # Retornar configurações padrão se o email não existir
            return True, {
                'high_priority': True,
                'medium_priority': False,
                'low_priority': False
            }

    except Exception as e:
        return False, f"Erro ao carregar configurações: {str(e)}"


def update_notification_settings(email: str, high_priority: bool = True,
                                 medium_priority: bool = False,
                                 low_priority: bool = False) -> Tuple[bool, str]:
    """Atualiza as configurações de notificação para um email"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Verificar se o email já existe
        cursor.execute("SELECT 1 FROM NotificationSettings WHERE email = ?", email)

        if cursor.fetchone():
            # Atualizar existente
            cursor.execute("""
                UPDATE NotificationSettings
                SET high_priority = ?,
                    medium_priority = ?,
                    low_priority = ?,
                    updated_at = GETDATE()
                WHERE email = ?
            """, high_priority, medium_priority, low_priority, email)
        else:
            # Inserir novo
            cursor.execute("""
                INSERT INTO NotificationSettings 
                (email, high_priority, medium_priority, low_priority)
                VALUES (?, ?, ?, ?)
            """, email, high_priority, medium_priority, low_priority)

        conn.commit()
        cursor.close()

        return True, "Configurações de notificação salvas com sucesso!"

    except Exception as e:
        return False, f"Erro ao salvar configurações: {str(e)}"


# ============================================================================
# FUNÇÕES DE ESTATÍSTICAS
# ============================================================================

def get_statistics() -> Tuple[bool, Dict[str, Any]]:
    """Obtém estatísticas completas do sistema"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        stats = {}

        # 1. Estatísticas gerais
        cursor = conn.cursor()

        # Total de asteroides
        cursor.execute("SELECT COUNT(*) FROM Asteroid")
        stats['total_asteroides'] = cursor.fetchone()[0]

        # Total de NEOs
        cursor.execute("SELECT COUNT(*) FROM Asteroid WHERE neo = 'Y'")
        stats['total_neos'] = cursor.fetchone()[0]

        # Total de PHAs
        cursor.execute("SELECT COUNT(*) FROM Asteroid WHERE pha = 'Y'")
        stats['total_phas'] = cursor.fetchone()[0]

        # Total de PHAs com diâmetro > 100m
        cursor.execute("SELECT COUNT(*) FROM Asteroid WHERE pha = 'Y' AND diameter > 0.1")
        stats['total_phas_100m'] = cursor.fetchone()[0]

        # 2. Estatísticas de alertas
        cursor.execute("SELECT COUNT(*) FROM Alert WHERE is_active = 1")
        stats['alertas_ativos'] = cursor.fetchone()[0]

        # Alertas por nível
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN priority_level = 4 THEN 'alertas_vermelhos'
                    WHEN priority_level = 3 THEN 'alertas_laranja'
                    WHEN priority_level = 2 THEN 'alertas_amarelos'
                    WHEN priority_level = 1 THEN 'alertas_verdes'
                END AS tipo,
                COUNT(*) as quantidade
            FROM Alert 
            WHERE is_active = 1
            GROUP BY priority_level
        """)

        for row in cursor.fetchall():
            stats[row[0]] = row[1]

        # 3. Próximo evento crítico (menos de 5 LD)
        cursor.execute("""
            SELECT TOP 1 
                a.full_name,
                op.moid_ld,
                op.epoch
            FROM Orbital_Parameters op
            INNER JOIN Asteroid a ON op.asteroid_id = a.asteroid_id
            WHERE op.moid_ld < 5
            ORDER BY op.moid_ld ASC
        """)

        evento = cursor.fetchone()
        if evento:
            stats['proximo_evento_critico'] = {
                'asteroide': evento[0],
                'distancia_ld': float(evento[1]) if evento[1] else 0,
                'data': evento[2].strftime('%Y-%m-%d') if evento[2] else 'Desconhecida'
            }

        # 4. Novos NEOs no último mês
        cursor.execute("""
            SELECT COUNT(*) 
            FROM Asteroid 
            WHERE neo = 'Y' 
              AND (SELECT MAX(epoch) FROM Orbital_Parameters op 
                   WHERE op.asteroid_id = Asteroid.asteroid_id) > DATEADD(MONTH, -1, GETDATE())
        """)
        stats['novos_neos_ultimo_mes'] = cursor.fetchone()[0] or 0

        # 5. Evolução da precisão (RMS médio por ano)
        cursor.execute("""
            SELECT 
                YEAR(epoch) as ano,
                AVG(rms) as rms_medio,
                COUNT(*) as qtd_calculos
            FROM Orbital_Parameters 
            WHERE epoch IS NOT NULL AND rms IS NOT NULL
            GROUP BY YEAR(epoch)
            ORDER BY ano
        """)

        evolucao = []
        for row in cursor.fetchall():
            evolucao.append({
                'ano': row[0],
                'rms_medio': float(row[1]) if row[1] else 0,
                'qtd_calculos': row[2]
            })
        stats['evolucao_precisao'] = evolucao

        # 6. Distribuição por classificação
        cursor.execute("""
            SELECT 
                COALESCE(class, 'Não classificada') as classificacao,
                COUNT(*) as quantidade
            FROM Asteroid
            GROUP BY class
            ORDER BY quantidade DESC
        """)

        classificacoes = {}
        for row in cursor.fetchall():
            classificacoes[row[0]] = row[1]
        stats['classificacoes'] = classificacoes

        # 7. Distribuição por tamanho
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN diameter < 0.01 THEN 'Pequenos (<10m)'
                    WHEN diameter BETWEEN 0.01 AND 0.05 THEN 'Médios (10-50m)'
                    WHEN diameter BETWEEN 0.05 AND 0.1 THEN 'Grandes (50-100m)'
                    WHEN diameter > 0.1 THEN 'Muito Grandes (>100m)'
                    ELSE 'Desconhecido'
                END as categoria,
                COUNT(*) as quantidade
            FROM Asteroid
            GROUP BY 
                CASE 
                    WHEN diameter < 0.01 THEN 'Pequenos (<10m)'
                    WHEN diameter BETWEEN 0.01 AND 0.05 THEN 'Médios (10-50m)'
                    WHEN diameter BETWEEN 0.05 AND 0.1 THEN 'Grandes (50-100m)'
                    WHEN diameter > 0.1 THEN 'Muito Grandes (>100m)'
                    ELSE 'Desconhecido'
                END
        """)

        distribuicao_tamanhos = {}
        for row in cursor.fetchall():
            distribuicao_tamanhos[row[0]] = row[1]
        stats['distribuicao_tamanhos'] = distribuicao_tamanhos

        cursor.close()
        return True, stats

    except Exception as e:
        return False, f"Erro ao obter estatísticas: {str(e)}"


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def execute_custom_query(query: str, params: List[Any] = None) -> Tuple[bool, Any]:
    """Executa uma query personalizada"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Se for SELECT, retornar resultados
        if query.strip().upper().startswith('SELECT'):
            columns = [column[0] for column in cursor.description]
            records = cursor.fetchall()
            result = (columns, records)
        else:
            # Para outros comandos (INSERT, UPDATE, DELETE)
            conn.commit()
            result = f"Comando executado. Linhas afetadas: {cursor.rowcount}"

        cursor.close()
        return True, result

    except Exception as e:
        return False, f"Erro na query: {str(e)}"


def backup_database(backup_path: str) -> Tuple[bool, str]:
    """Cria um backup da base de dados"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão à base de dados"

        cursor = conn.cursor()

        # Obter nome da base de dados
        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]

        # Criar comando BACKUP
        backup_command = f"""
        BACKUP DATABASE [{db_name}]
        TO DISK = '{backup_path}'
        WITH FORMAT, MEDIANAME = 'SQLServerBackups', NAME = 'Full Backup of {db_name}'
        """

        cursor.execute(backup_command)
        conn.commit()
        cursor.close()

        return True, f"Backup criado com sucesso em: {backup_path}"

    except Exception as e:
        return False, f"Erro ao criar backup: {str(e)}"


def test_connection() -> Tuple[bool, str]:
    """Testa a conexão com a base de dados"""
    try:
        conn = get_connection()
        if not conn:
            return False, "Sem conexão"

        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()

        if result and result[0] == 1:
            return True, "Conexão ativa"
        else:
            return False, "Conexão inativa"

    except Exception as e:
        return False, f"Erro de conexão: {str(e)}"


# ============================================================================
# INICIALIZAÇÃO
# ============================================================================

if __name__ == "__main__":
    print("Módulo database.py - Teste de conexão")

    # Testar conexão com configuração padrão
    success, message, info = connect_to_db(
        server="INESTRIPECA\\SQLEXPRESS",
        database="NEO_Monitoring"
    )

    if success:
        print(f"✓ Conexão estabelecida: {message}")
        print(f"  Base de dados: {info['database_name']}")
        print(f"  Tabelas: {info['table_count']}")

        # Listar tabelas
        tables = get_all_tables()
        print(f"  Lista de tabelas: {', '.join(tables[:5])}...")
    else:
        print(f"✗ Falha na conexão: {message}")

    close_connection()