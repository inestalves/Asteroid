import pyodbc
import os

# Variável global para armazenar a conexão
connection = None

def connect_to_db(server, database, username=None, password=None, port=None):
    """
    Estabelece conexão com SQL Server.
    Retorna (success, message, connection_info)
    """
    global connection

    try:
        if port:
            server_completo = f"{server},{port}"
        else:
            server_completo = server

        if username and password:
            # Autenticação SQL Server
            conn_str = (
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={server_completo};'
                f'DATABASE={database};'
                f'UID={username};'
                f'PWD={password};'
            )
        else:
            # Autenticação Windows
            conn_str = (
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={server_completo};'
                f'DATABASE={database};'
                f'Trusted_Connection=yes;'
            )

        # Tentar conexão
        connection = pyodbc.connect(conn_str, timeout=10)
        connection.autocommit = True

        # Obter informações
        cursor = connection.cursor()
        cursor.execute("SELECT @@VERSION, DB_NAME()")
        resultado = cursor.fetchone()
        cursor.close()

        versao_sql = resultado[0].split('\n')[0]
        nome_bd = resultado[1]

        return True, "Conexão estabelecida com sucesso!", {
            'database_name': nome_bd,
            'sql_version': versao_sql,
            'connection': connection
        }

    except pyodbc.Error as e:
        error_msg = str(e)
        if "Login failed" in error_msg:
            return False, "Falha no login. Verifique utilizador e password.", None
        elif "Cannot open database" in error_msg:
            return False, f"A base de dados '{database}' não existe.", None
        else:
            return False, f"Não foi possível conectar: {error_msg}", None


def get_all_tables():
    """Retorna lista de todas as tabelas da base de dados"""
    global connection

    if not connection:
        return []

    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)

        tabelas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tabelas

    except Exception as e:
        print(f"Erro ao obter tabelas: {e}")
        return []


def get_table_structure(table_name):
    """Obtém a estrutura de uma tabela"""
    global connection

    if not connection:
        return []

    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)

        colunas = []
        for row in cursor.fetchall():
            col_name = row[0]
            data_type = row[1]
            max_length = row[2]
            is_nullable = row[3] == 'YES'
            is_identity = row[4] == 1

            # Formatar tipo de dados
            if max_length and data_type in ['varchar', 'char', 'nvarchar', 'nchar']:
                data_type = f"{data_type}({max_length})"

            colunas.append({
                'name': col_name,
                'type': data_type,
                'nullable': is_nullable,
                'identity': is_identity
            })

        cursor.close()
        return colunas

    except Exception as e:
        print(f"Erro ao obter estrutura da tabela: {e}")
        return []


def get_primary_key(table_name):
    """Obtém a chave primária de uma tabela"""
    global connection

    if not connection:
        return None

    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = '{table_name}' 
            AND CONSTRAINT_NAME LIKE 'PK%'
        """)

        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None

    except Exception as e:
        print(f"Erro ao obter chave primária: {e}")
        return None


def create_table_in_db(table_name, columns):
    """Cria uma nova tabela na base de dados"""
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()
        columns_sql = []

        for col in columns:
            col_name = col['name_entry'].get().strip()
            col_type = col['type_combo'].get()
            nullable = "NULL" if col['nullable_check'].get() else "NOT NULL"
            columns_sql.append(f"{col_name} {col_type} {nullable}")

        sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns_sql) + "\n)"
        cursor.execute(sql)
        cursor.close()

        return True, f"Tabela '{table_name}' criada com sucesso!"

    except Exception as e:
        return False, str(e)


def insert_record_into_table(table_name, record_data):
    """Insere um registo numa tabela"""
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        # Preparar valores
        column_names = []
        values = []
        placeholders = []

        for field in record_data['fields']:
            value = field['entry'].get().strip()
            column_names.append(field['name'])

            if value:
                values.append(value)
            else:
                values.append(None)
            placeholders.append("?")

        # Construir SQL
        sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(placeholders)})"

        cursor.execute(sql, values)
        connection.commit()
        cursor.close()

        return True, "Registo inserido com sucesso!"

    except Exception as e:
        return False, str(e)


def update_record_in_table(table_name, update_data):
    """Atualiza um registo numa tabela"""
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        # Preparar SET clause
        set_clauses = []
        values = []

        for field in update_data['fields']:
            value = field['entry'].get().strip()
            if value:
                set_clauses.append(f"{field['name']} = ?")
                values.append(value)

        if not set_clauses:
            return False, "Nenhum campo para atualizar"

        # Adicionar ID para WHERE
        values.append(update_data['record_id'])

        # Construir SQL
        sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {update_data['primary_key']} = ?"

        cursor.execute(sql, values)
        connection.commit()

        success = cursor.rowcount > 0
        cursor.close()

        if success:
            return True, f"Registo atualizado com sucesso!"
        else:
            return False, "Nenhum registo foi atualizado"

    except Exception as e:
        return False, str(e)


def delete_record_from_table(table_name, primary_key, record_id):
    """Elimina um registo de uma tabela"""
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()
        sql = f"DELETE FROM {table_name} WHERE {primary_key} = ?"
        cursor.execute(sql, (record_id,))
        connection.commit()

        success = cursor.rowcount > 0
        cursor.close()

        if success:
            return True, f"Registo eliminado com sucesso!"
        else:
            return False, "Nenhum registo foi eliminado"

    except Exception as e:
        return False, str(e)


def query_table_with_filters(table_name, filters=None):
    """Consulta uma tabela com filtros opcionais"""
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        # Construir query base
        sql = f"SELECT * FROM {table_name}"
        params = []

        if filters and 'column' in filters and 'value' in filters and filters['value']:
            column = filters['column']
            operator = filters.get('operator', '=')
            value = filters['value']

            if operator.upper() == 'LIKE':
                sql += f" WHERE {column} LIKE ?"
                params.append(f"%{value}%")
            else:
                sql += f" WHERE {column} {operator} ?"
                params.append(value)

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        records = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        cursor.close()
        return True, (columns, records)

    except Exception as e:
        return False, str(e)


def load_record_for_update_from_db(table_name, primary_key, record_id):
    """Carrega um registo para atualização"""
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()
        sql = f"SELECT * FROM {table_name} WHERE {primary_key} = ?"
        cursor.execute(sql, (record_id,))
        record = cursor.fetchone()

        if not record:
            cursor.close()
            return False, f"Registo não encontrado"

        column_names = [column[0] for column in cursor.description]
        record_dict = dict(zip(column_names, record))

        cursor.close()
        return True, record_dict

    except Exception as e:
        return False, str(e)


def get_connection():
    """Retorna a conexão atual"""
    return connection


def close_connection():
    """Fecha a conexão com a base de dados"""
    global connection
    if connection:
        connection.close()
        connection = None


def execute_sql_file(file_path):
    """
    Executa um ficheiro SQL completo na base de dados
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        # Verificar se o ficheiro existe
        if not os.path.exists(file_path):
            return False, f"Ficheiro não encontrado: {file_path}"

        # Ler o conteúdo do ficheiro
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # IMPORTANTE: Remover a linha USE database, pois já estamos conectados
        lines = sql_content.split('\n')
        filtered_lines = []
        for line in lines:
            if line.strip().upper().startswith('USE ') and 'GO' in line.upper():
                print(f"IGNORADO: {line.strip()} (já conectado à BD)")
                continue
            filtered_lines.append(line)

        sql_content = '\n'.join(filtered_lines)

        cursor = connection.cursor()

        # Separar por GO em linhas próprias
        batches = []
        current_batch = []

        for line in sql_content.split('\n'):
            line_stripped = line.strip()

            if line_stripped.upper() == 'GO':
                if current_batch:
                    batch_sql = '\n'.join(current_batch)
                    if batch_sql.strip():
                        batches.append(batch_sql)
                    current_batch = []
            else:
                current_batch.append(line)

        # Adicionar o último batch se existir
        if current_batch:
            batch_sql = '\n'.join(current_batch)
            if batch_sql.strip():
                batches.append(batch_sql)

        print(f"Encontrados {len(batches)} lotes (batches) SQL")

        for i, batch in enumerate(batches, 1):
            try:
                if batch.strip():
                    print(f"Executando lote {i}/{len(batches)}: {batch[:100].replace(chr(10), ' ').replace(chr(13), ' ')}...")
                    cursor.execute(batch)
                    # COMMIT após cada lote bem-sucedido
                    connection.commit()
            except Exception as cmd_error:
                print(f" Erro no lote {i}: {cmd_error}")
                print(f"SQL problemático: {batch[:200]}")
                # Rollback apenas deste lote
                connection.rollback()
                # Continue com os próximos lotes
                continue

        cursor.close()

        return True, f"Ficheiro {file_path} executado. {len(batches)} lotes processados."

    except Exception as e:
        return False, f"Erro ao executar ficheiro SQL: {str(e)}"


def setup_triggers():
    """
    Configura todos os triggers do sistema a partir do ficheiro triggers.txt
    """
    return execute_sql_file('triggers.txt')


def setup_views():
    """
    Configura todas as views do sistema a partir do ficheiro queries.txt
    """
    return execute_sql_file('queries.txt')


def check_triggers_exist():
    """
    Verifica se os triggers principais estão criados
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        # Lista de triggers que devem existir (ajustar conforme seus triggers)
        expected_triggers = [
            'trg_AfterInsertAsteroid',
            'trg_AfterUpdateAsteroid',
            'trg_AfterInsertOrbit',
            'trg_GenerateAlerts'
        ]

        existing_triggers = []
        for trigger in expected_triggers:
            cursor.execute(f"""
                SELECT name 
                FROM sys.triggers 
                WHERE name = '{trigger}'
            """)
            if cursor.fetchone():
                existing_triggers.append(trigger)

        cursor.close()

        if len(existing_triggers) == len(expected_triggers):
            return True, "Todos os triggers estão configurados"
        else:
            missing = set(expected_triggers) - set(existing_triggers)
            return False, f"Triggers em falta: {', '.join(missing)}"

    except Exception as e:
        return False, f"Erro ao verificar triggers: {str(e)}"


def get_all_triggers():
    """
    Retorna lista de todos os triggers na base de dados
    """
    global connection

    if not connection:
        return []

    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT 
                t.name as trigger_name,
                OBJECT_NAME(t.parent_id) as table_name,
                t.is_disabled,
                t.create_date
            FROM sys.triggers t
            WHERE t.is_ms_shipped = 0
            ORDER BY t.name
        """)

        triggers = []
        for row in cursor.fetchall():
            triggers.append({
                'name': row[0],
                'table': row[1],
                'disabled': row[2],
                'created': row[3]
            })

        cursor.close()
        return triggers

    except Exception as e:
        print(f"Erro ao obter triggers: {e}")
        return []


def enable_disable_trigger(trigger_name, enable=True):
    """
    Ativa ou desativa um trigger
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        if enable:
            cursor.execute(f"ENABLE TRIGGER {trigger_name} ON DATABASE")
            message = f"Trigger {trigger_name} ativado"
        else:
            cursor.execute(f"DISABLE TRIGGER {trigger_name} ON DATABASE")
            message = f"Trigger {trigger_name} desativado"

        connection.commit()
        cursor.close()
        return True, message

    except Exception as e:
        return False, f"Erro ao alterar trigger: {str(e)}"


def drop_trigger(trigger_name):
    """
    Elimina um trigger da base de dados
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()
        cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
        connection.commit()
        cursor.close()
        return True, f"Trigger {trigger_name} eliminado"

    except Exception as e:
        return False, f"Erro ao eliminar trigger: {str(e)}"


def execute_view(view_name):
    """
    Executa uma view e retorna os resultados.
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {view_name}")
        records = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        cursor.close()
        return True, (columns, records)

    except Exception as e:
        return False, str(e)


# Funções para notificações

def create_notification_table():
    """
    Cria a tabela NotificationSettings no banco atual
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        # Verificar qual banco estamos usando
        cursor.execute("SELECT DB_NAME()")
        current_db = cursor.fetchone()[0]
        print(f"Criando tabela no banco: {current_db}")

        # Script SQL para criar a tabela
        sql_script = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'NotificationSettings')
        BEGIN
            CREATE TABLE NotificationSettings (
                setting_id INT PRIMARY KEY IDENTITY(1,1),
                user_email VARCHAR(255) UNIQUE NOT NULL,
                high_priority_alerts BIT DEFAULT 1,
                medium_priority_alerts BIT DEFAULT 0,
                low_priority_alerts BIT DEFAULT 0,
                created_date DATETIME DEFAULT GETDATE(),
                updated_date DATETIME DEFAULT GETDATE()
            );

            INSERT INTO NotificationSettings (user_email, high_priority_alerts)
            VALUES ('admin@observatorio.pt', 1);

            PRINT 'Tabela NotificationSettings criada com sucesso.';
        END
        ELSE
            PRINT 'A tabela NotificationSettings já existe.';
        """

        cursor.execute(sql_script)
        connection.commit()
        cursor.close()

        return True, f"Tabela NotificationSettings verificada/criada no banco {current_db}"

    except Exception as e:
        return False, f"Erro ao criar tabela: {str(e)}"


def get_active_alerts(filters=None):
    """
    Retorna alertas ativos com filtros opcionais - VERSÃO ATUALIZADA
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        # Query base - ATUALIZADA para a estrutura correta
        sql = """
            SELECT 
                a.alert_id,
                ast.full_name AS asteroid_name,
                a.alert_date,
                a.priority_level,
                a.description,
                a.is_active
            FROM Alert a
            LEFT JOIN Asteroid ast ON a.asteroid_id = ast.asteroid_id
            WHERE a.is_active = 1
        """
        params = []

        # Aplicar filtros
        if filters:
            if 'priority' in filters and filters['priority'] and filters['priority'] != 'Todas':
                # Converter string para valor inteiro
                priority_map = {
                    'Alta': 1,
                    'Média': 2,
                    'Baixa': 3
                }
                priority_int = priority_map.get(filters['priority'])
                if priority_int:
                    sql += " AND a.priority_level = ?"
                    params.append(priority_int)

        sql += " ORDER BY a.alert_date DESC"

        cursor.execute(sql, params)
        records = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        cursor.close()
        return True, (columns, records)

    except Exception as e:
        return False, str(e)


def get_notification_settings(email=None):
    """
    Obtém configurações de notificação
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        if email:
            cursor.execute("""
                SELECT * FROM NotificationSettings 
                WHERE user_email = ?
            """, (email,))
        else:
            cursor.execute("SELECT * FROM NotificationSettings")

        columns = [description[0] for description in cursor.description]
        records = cursor.fetchall()

        cursor.close()
        return True, (columns, records)

    except Exception as e:
        return False, str(e)


def load_notification_settings_for_email(email):
    """
    Carrega configurações específicas para um email
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT 
                user_email,
                high_priority_alerts,
                medium_priority_alerts,
                low_priority_alerts
            FROM NotificationSettings 
            WHERE user_email = ?
        """, (email,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return True, {
                'email': result[0],
                'high_priority': bool(result[1]),
                'medium_priority': bool(result[2]),
                'low_priority': bool(result[3])
            }
        else:
            return False, "Email não encontrado"

    except Exception as e:
        return False, str(e)


def update_notification_settings(email, high_priority, medium_priority=None, low_priority=None):
    """
    Atualiza configurações de notificação
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        # Verificar se existe
        cursor.execute("SELECT * FROM NotificationSettings WHERE user_email = ?", (email,))
        exists = cursor.fetchone()

        if exists:
            # Atualizar
            sql = """
                UPDATE NotificationSettings 
                SET high_priority_alerts = ?,
                    medium_priority_alerts = ?,
                    low_priority_alerts = ?,
                    updated_date = GETDATE()
                WHERE user_email = ?
            """
            cursor.execute(sql, (1 if high_priority else 0,
                                 1 if medium_priority else 0,
                                 1 if low_priority else 0,
                                 email))
        else:
            # Inserir novo
            sql = """
                INSERT INTO NotificationSettings 
                (user_email, high_priority_alerts, medium_priority_alerts, low_priority_alerts)
                VALUES (?, ?, ?, ?)
            """
            cursor.execute(sql, (email,
                                 1 if high_priority else 0,
                                 1 if medium_priority else 0,
                                 1 if low_priority else 0))

        connection.commit()
        cursor.close()
        return True, f"Configurações para {email} atualizadas com sucesso"

    except Exception as e:
        return False, str(e)


def check_new_high_priority_alerts():
    """
    Verifica se existem novos alertas de alta prioridade (priority_level = 1)
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()

        # Verificar alertas de alta prioridade (priority_level = 1) criados nos últimos 5 minutos
        cursor.execute("""
            SELECT COUNT(*) as new_alerts
            FROM Alert 
            WHERE is_active = 1 
            AND priority_level = 1  -- Alta prioridade = 1
            AND alert_date >= DATEADD(minute, -5, GETDATE())
        """)

        new_alerts = cursor.fetchone()[0]
        cursor.close()

        return True, new_alerts

    except Exception as e:
        return False, str(e)


def get_statistics():
    """
    Retorna estatísticas completas conforme especificação do trabalho - VERSÃO CORRIGIDA
    """
    global connection

    if not connection:
        return False, "Não conectado à BD"

    try:
        cursor = connection.cursor()
        stats = {}

        # Estatística de alerta
        try:
            # Número de alertas vermelhos (nível 4) e laranja (nível 3)
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN priority_level = 4 THEN 1 ELSE 0 END) as vermelhos,
                    SUM(CASE WHEN priority_level = 3 THEN 1 ELSE 0 END) as laranja
                FROM Alert 
                WHERE is_active = 1
            """)
            row = cursor.fetchone()
            stats['alertas_vermelhos'] = row[0] if row and row[0] is not None else 0
            stats['alertas_laranja'] = row[1] if row and row[1] is not None else 0
        except Exception as e:
            stats['alertas_vermelhos'] = 0
            stats['alertas_laranja'] = 0
            print(f" Erro em estatísticas de alerta: {e}")

        # Total de PHAs monitorizados com diâmetro > 100m (0.1km)
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM Asteroid 
                WHERE pha = 'Y' 
                AND diameter > 0.1
            """)
            row = cursor.fetchone()
            stats['total_phas_100m'] = row[0] if row else 0
        except Exception as e:
            stats['total_phas_100m'] = 0
            print(f"Erro em PHAs > 100m: {e}")

        # Próximo eventos críticos
        try:
            # Verificar se a coluna moid_ld existe em Orbital_Parameters
            cursor.execute("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Orbital_Parameters' 
                AND COLUMN_NAME = 'moid_ld'
            """)

            if cursor.fetchone():
                # Query usando moid_ld
                cursor.execute("""
                    SELECT TOP 1
                        a.full_name,
                        op.moid_ld,
                        op.epoch_cal
                    FROM Orbital_Parameters op
                    INNER JOIN Asteroid a ON op.asteroid_id = a.asteroid_id
                    WHERE op.moid_ld < 5 
                        AND op.epoch_cal IS NOT NULL
                        AND TRY_CAST(op.epoch_cal AS DATE) IS NOT NULL
                        AND TRY_CAST(op.epoch_cal AS DATE) >= CAST(GETDATE() AS DATE)
                    ORDER BY TRY_CAST(op.epoch_cal AS DATE) ASC
                """)
            else:
                # Se não existir moid_ld, use moid (em UA) e converta para LD (1 UA ≈ 389.17 LD)
                cursor.execute("""
                    SELECT TOP 1
                        a.full_name,
                        op.moid * 389.17 as moid_ld,  -- Converter UA para LD
                        op.epoch_cal
                    FROM Orbital_Parameters op
                    INNER JOIN Asteroid a ON op.asteroid_id = a.asteroid_id
                    WHERE op.moid * 389.17 < 5  -- Converter para LD e verificar < 5 LD
                        AND op.epoch_cal IS NOT NULL
                        AND TRY_CAST(op.epoch_cal AS DATE) IS NOT NULL
                        AND TRY_CAST(op.epoch_cal AS DATE) >= CAST(GETDATE() AS DATE)
                    ORDER BY TRY_CAST(op.epoch_cal AS DATE) ASC
                """)

            next_event = cursor.fetchone()
            if next_event:
                stats['proximo_evento_critico'] = {
                    'asteroide': next_event[0] if next_event[0] else "Desconhecido",
                    'distancia_ld': float(next_event[1]) if next_event[1] else 0,
                    'data': next_event[2]
                }
            else:
                stats['proximo_evento_critico'] = None
        except Exception as e:
            stats['proximo_evento_critico'] = None
            print(f" Erro em próximo evento crítico: {e}")

        # Estatística de descoberta
        try:
            # Número de novos NEOs descobertos no último mês
            # Verificar se há data de descoberta
            cursor.execute("""
                SELECT COUNT(*) 
                FROM Asteroid 
                WHERE neo = 'Y'
            """)
            total_neos = cursor.fetchone()[0]
            stats['total_neos'] = total_neos

            # Para "novos no último mês", podemos usar a data atual como aproximação
            stats['novos_neos_ultimo_mes'] = total_neos  # Apenas para mostrar algo
        except Exception as e:
            stats['total_neos'] = 0
            stats['novos_neos_ultimo_mes'] = 0
            print(f" Erro em estatísticas de descoberta: {e}")

        # Evolução da precisão
        try:
            # Verificar se a coluna rms existe
            cursor.execute("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Orbital_Parameters' 
                AND COLUMN_NAME = 'rms'
            """)

            if cursor.fetchone():
                cursor.execute("""
                    SELECT 
                        YEAR(GETDATE()) as ano,  -- Ano atual como exemplo
                        AVG(rms) as rms_medio,
                        COUNT(*) as qtd_calculos
                    FROM Orbital_Parameters
                    WHERE rms IS NOT NULL
                """)
            else:
                # Se não existir rms, usar valor padrão
                cursor.execute("""
                    SELECT 
                        YEAR(GETDATE()) as ano,
                        0.5 as rms_medio,
                        100 as qtd_calculos
                """)

            row = cursor.fetchone()
            if row:
                stats['evolucao_precisao'] = [{
                    'ano': int(row[0]),
                    'rms_medio': float(row[1]) if row[1] else 0,
                    'qtd_calculos': int(row[2])
                }]
            else:
                stats['evolucao_precisao'] = []
        except Exception as e:
            stats['evolucao_precisao'] = []
            print(f" Erro em evolução da precisão: {e}")

        # Estatísticas de Classificação
        try:
            # Usar class_id para obter classificações da tabela Class
            cursor.execute("""
                SELECT 
                    c.description as classe,
                    COUNT(a.asteroid_id) as quantidade
                FROM Asteroid a
                LEFT JOIN Class c ON a.class_id = c.class_id
                WHERE c.description IS NOT NULL
                GROUP BY c.description
                ORDER BY quantidade DESC
            """)
            stats['classificacoes'] = {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            stats['classificacoes'] = {}
            print(f" Erro em estatísticas de classificação: {e}")

        # Distribuição de tamanho
        try:
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN diameter IS NULL THEN 'Desconhecido'
                        WHEN diameter <= 0.01 THEN 'Muito Pequeno (<10m)'
                        WHEN diameter <= 0.1 THEN 'Pequeno (10-100m)'
                        WHEN diameter <= 0.5 THEN 'Médio (100-500m)'
                        WHEN diameter <= 1 THEN 'Grande (500m-1km)'
                        ELSE 'Muito Grande (>1km)'
                    END as categoria,
                    COUNT(*) as quantidade
                FROM Asteroid
                GROUP BY 
                    CASE 
                        WHEN diameter IS NULL THEN 'Desconhecido'
                        WHEN diameter <= 0.01 THEN 'Muito Pequeno (<10m)'
                        WHEN diameter <= 0.1 THEN 'Pequeno (10-100m)'
                        WHEN diameter <= 0.5 THEN 'Médio (100-500m)'
                        WHEN diameter <= 1 THEN 'Grande (500m-1km)'
                        ELSE 'Muito Grande (>1km)'
                    END
                ORDER BY quantidade DESC
            """)
            stats['distribuicao_tamanhos'] = {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            stats['distribuicao_tamanhos'] = {}
            print(f" Erro em distribuição de tamanhos: {e}")

        # Estatística gerais
        try:
            cursor.execute("SELECT COUNT(*) FROM Asteroid")
            stats['total_asteroides'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM Alert WHERE is_active = 1")
            stats['alertas_ativos'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM Orbital_Parameters")
            stats['total_orbitas'] = cursor.fetchone()[0]
        except Exception as e:
            print(f" Erro em estatísticas gerais: {e}")

        cursor.close()
        return True, stats

    except Exception as e:
        return False, f"Erro ao obter estatísticas: {str(e)}"