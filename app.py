import customtkinter
from tkinter import messagebox, ttk
import tkinter as tk
from database import (
    connect_to_db, get_all_tables, get_table_structure, get_primary_key,
    create_table_in_db, insert_record_into_table, update_record_in_table,
    delete_record_from_table, query_table_with_filters, load_record_for_update_from_db,
    get_connection, setup_triggers, setup_views, check_triggers_exist,
    get_all_triggers, enable_disable_trigger, drop_trigger,
    execute_view,
    get_active_alerts, get_notification_settings, update_notification_settings,
    get_statistics, check_new_high_priority_alerts, load_notification_settings_for_email,
    create_notification_table
)

customtkinter.set_appearance_mode("system")
customtkinter.set_default_color_theme("dark-blue")

app = customtkinter.CTk()
app.geometry("1100x850")
app.title("Asteroid Database Manager")

# Variáveis globais
current_table = None
insert_fields_cache = []
update_fields_cache = []

# Criar sistema de abas
tabview = customtkinter.CTkTabview(app)
tabview.pack(padx=20, pady=20, fill="both", expand=True)

# Adicionar abas
tabview.add("Conexão BD")
tabview.add("Criar Tabela")
tabview.add("CRUD - Operações")
tabview.add("Consultar Dados")
tabview.add("Alertas e Monitorização")
tabview.add("Estatísticas e Gráficos")
tabview.add("Consultas Específicas")
tabview.add("Gestão de Triggers")

# função mudança de aba
def on_tab_changed():
    current_tab = tabview.get()
    if current_tab == "Estatísticas e Gráficos":
        # Carregar estatísticas na sub-aba numérica
        load_statistics()
        # Carregar gráfico padrão na sub-aba de gráficos
        app.after(100, lambda: criar_grafico_selecionado())

# Configurar evento de mudança de aba
tabview.configure(command=on_tab_changed)

# Conexão de Base de Dados
connection_tab = tabview.tab("Conexão BD")

frame_principal = customtkinter.CTkFrame(connection_tab)
frame_principal.pack(padx=20, pady=20, fill="both", expand=True)

# Título
label_titulo = customtkinter.CTkLabel(
    frame_principal,
    text="Aplicação Cliente para SQL Server",
    font=("Arial", 16, "bold")
)
label_titulo.pack(pady=10)

# Frame de campos
frame_campos = customtkinter.CTkFrame(frame_principal)
frame_campos.pack(pady=10, padx=10, fill="x")

# Servidor
label_server = customtkinter.CTkLabel(
    frame_campos,
    text="Servidor (IP/Nome):",
    font=("Arial", 12)
)
label_server.grid(row=0, column=0, padx=5, pady=5, sticky="w")

entry_server = customtkinter.CTkEntry(
    frame_campos,
    placeholder_text="Ex: .\\SQLEXPRESS",
    width=250
)
entry_server.grid(row=0, column=1, padx=5, pady=5)

# Porta
label_port = customtkinter.CTkLabel(
    frame_campos,
    text="Porta (opcional):",
    font=("Arial", 12)
)
label_port.grid(row=1, column=0, padx=5, pady=5, sticky="w")

entry_port = customtkinter.CTkEntry(
    frame_campos,
    placeholder_text="1433",
    width=100
)
entry_port.grid(row=1, column=1, padx=5, pady=5, sticky="w")

# Utilizador
label_user = customtkinter.CTkLabel(
    frame_campos,
    text="Utilizador:",
    font=("Arial", 12)
)
label_user.grid(row=2, column=0, padx=5, pady=5, sticky="w")

entry_user = customtkinter.CTkEntry(
    frame_campos,
    placeholder_text="Username",
    width=250
)
entry_user.grid(row=2, column=1, padx=5, pady=5)

# Password
label_password = customtkinter.CTkLabel(
    frame_campos,
    text="Password:",
    font=("Arial", 12)
)
label_password.grid(row=3, column=0, padx=5, pady=5, sticky="w")

entry_password = customtkinter.CTkEntry(
    frame_campos,
    placeholder_text="Password",
    show="*",
    width=250
)
entry_password.grid(row=3, column=1, padx=5, pady=5)

# Base de Dados
label_database = customtkinter.CTkLabel(
    frame_campos,
    text="Base de Dados:",
    font=("Arial", 12)
)
label_database.grid(row=4, column=0, padx=5, pady=5, sticky="w")

entry_database = customtkinter.CTkEntry(
    frame_campos,
    placeholder_text="Nome da base de dados",
    width=250
)
entry_database.grid(row=4, column=1, padx=5, pady=5)

# Botão Ligar à BD
btn_ligar = customtkinter.CTkButton(
    frame_principal,
    text="Ligar à BD",
    command=None,
    fg_color="blue",
    hover_color="dark blue",
    width=150,
    height=35
)
btn_ligar.pack(pady=20)

# Label de status da conexão
connection_status = customtkinter.CTkLabel(
    frame_principal,
    text="Não conectado",
    text_color="red",
    font=("Arial", 12)
)
connection_status.pack(pady=5)

# Configuração padrão inicial
entry_server.insert(0, "INESTRIPECA\\SQLEXPRESS")

# Criar Tabela
create_tab = tabview.tab("Criar Tabela")

label_create_title = customtkinter.CTkLabel(
    create_tab,
    text="Criar Nova Tabela",
    font=("Arial", 16, "bold")
)
label_create_title.pack(pady=10)

# Frame para nome da tabela
table_name_frame = customtkinter.CTkFrame(create_tab)
table_name_frame.pack(pady=10, padx=20, fill="x")

label_table_name = customtkinter.CTkLabel(
    table_name_frame,
    text="Nome da Tabela:",
    font=("Arial", 12)
)
label_table_name.pack(side="left", padx=10)

entry_table_name = customtkinter.CTkEntry(
    table_name_frame,
    placeholder_text="Ex: Asteroides",
    width=200
)
entry_table_name.pack(side="left", padx=10)

# Frame para adicionar colunas
columns_frame = customtkinter.CTkScrollableFrame(create_tab)
columns_frame.pack(pady=10, padx=20, fill="both", expand=True)

# Lista para armazenar colunas
columns_list = []

def add_column_field():
    col_frame = customtkinter.CTkFrame(columns_frame)
    col_frame.pack(pady=5, fill="x", padx=10)

    col_name_entry = customtkinter.CTkEntry(
        col_frame,
        placeholder_text="Nome da Coluna",
        width=150
    )
    col_name_entry.pack(side="left", padx=5)

    col_type_var = customtkinter.StringVar(value="varchar(100)")
    col_type_combo = customtkinter.CTkComboBox(
        col_frame,
        values=["varchar(50)", "varchar(100)", "varchar(255)", "int", "float", "decimal(10,2)", "date", "datetime", "bit"],
        variable=col_type_var,
        width=150
    )
    col_type_combo.pack(side="left", padx=5)

    nullable_var = customtkinter.BooleanVar(value=True)
    nullable_check = customtkinter.CTkCheckBox(
        col_frame,
        text="NULL",
        variable=nullable_var
    )
    nullable_check.pack(side="left", padx=5)

    remove_btn = customtkinter.CTkButton(
        col_frame,
        text="Remover",
        width=80,
        command=lambda f=col_frame: remove_column_field(f)
    )
    remove_btn.pack(side="left", padx=5)

    columns_list.append({
        'frame': col_frame,
        'name_entry': col_name_entry,
        'type_combo': col_type_combo,
        'nullable_check': nullable_check
    })

def remove_column_field(frame):
    for col in columns_list:
        if col['frame'] == frame:
            columns_list.remove(col)
            frame.destroy()
            break

# Botão para adicionar primeira coluna
add_first_column_btn = customtkinter.CTkButton(
    create_tab,
    text="+ Adicionar Coluna",
    command=add_column_field,
    width=150
)
add_first_column_btn.pack(pady=10)

# Botão para criar tabela
create_table_btn = customtkinter.CTkButton(
    create_tab,
    text="CRIAR TABELA",
    command=None,
    width=200,
    height=35
)
create_table_btn.pack(pady=20)

# Label de informação
create_info_label = customtkinter.CTkLabel(
    create_tab,
    text="",
    text_color="green",
    font=("Arial", 12)
)
create_info_label.pack(pady=5)

# CRUD - Operações
crud_tab = tabview.tab("CRUD - Operações")

# Título
crud_title = customtkinter.CTkLabel(
    crud_tab,
    text="Operações CRUD",
    font=("Arial", 16, "bold")
)
crud_title.pack(pady=10)

# Frame para seleção de tabela
table_select_frame = customtkinter.CTkFrame(crud_tab)
table_select_frame.pack(pady=10, padx=20, fill="x")

label_select_table = customtkinter.CTkLabel(
    table_select_frame,
    text="Selecionar Tabela:",
    font=("Arial", 12)
)
label_select_table.pack(side="left", padx=10)

table_combo = customtkinter.CTkComboBox(
    table_select_frame,
    values=[],
    width=200,
    state="readonly"
)
table_combo.pack(side="left", padx=10)

refresh_tables_btn = customtkinter.CTkButton(
    table_select_frame,
    text="Atualizar",
    width=100,
    command=None
)
refresh_tables_btn.pack(side="left", padx=10)

# Frame para operações
operations_frame = customtkinter.CTkFrame(crud_tab)
operations_frame.pack(pady=10, padx=20, fill="both", expand=True)

# Notebook para diferentes operações
operations_notebook = customtkinter.CTkTabview(operations_frame)
operations_notebook.pack(pady=10, padx=10, fill="both", expand=True)

# Sub-aba: INSERT
insert_tab = operations_notebook.add("Inserir")
insert_fields_frame = customtkinter.CTkScrollableFrame(insert_tab)
insert_fields_frame.pack(pady=10, padx=10, fill="both", expand=True)

insert_btn = customtkinter.CTkButton(
    insert_tab,
    text="INSERIR REGISTO",
    width=150,
    height=35,
    fg_color="green",
    hover_color="dark green"
)
insert_btn.pack(pady=10)

# Sub-aba: UPDATE
update_tab = operations_notebook.add("Atualizar")

# Frame para ID no topo
update_id_frame = customtkinter.CTkFrame(update_tab)
update_id_frame.pack(pady=10, padx=10, fill="x")

label_update_id = customtkinter.CTkLabel(
    update_id_frame,
    text="ID do registo a atualizar:",
    font=("Arial", 12)
)
label_update_id.pack(side="left", padx=10)

update_id_entry = customtkinter.CTkEntry(
    update_id_frame,
    width=100,
    placeholder_text="Ex: 1"
)
update_id_entry.pack(side="left", padx=10)

load_for_update_btn = customtkinter.CTkButton(
    update_id_frame,
    text="Carregar",
    width=80,
    fg_color="orange",
    hover_color="dark orange"
)
load_for_update_btn.pack(side="left", padx=10)

# Frame para campos de update
update_fields_frame = customtkinter.CTkScrollableFrame(update_tab)
update_fields_frame.pack(pady=10, padx=10, fill="both", expand=True)

update_btn = customtkinter.CTkButton(
    update_tab,
    text="ATUALIZAR REGISTO",
    width=150,
    height=35,
    fg_color="blue",
    hover_color="dark blue"
)
update_btn.pack(pady=10)

# Sub-aba: DELETE
delete_tab = operations_notebook.add("Eliminar")
delete_fields_frame = customtkinter.CTkFrame(delete_tab)
delete_fields_frame.pack(pady=10, padx=10, fill="both", expand=True)

label_delete_id = customtkinter.CTkLabel(
    delete_fields_frame,
    text="ID do registo a eliminar:",
    font=("Arial", 12)
)
label_delete_id.pack(pady=10)

delete_id_entry = customtkinter.CTkEntry(
    delete_fields_frame,
    width=100,
    placeholder_text="Ex: 1"
)
delete_id_entry.pack(pady=10)

delete_btn = customtkinter.CTkButton(
    delete_tab,
    text="ELIMINAR REGISTO",
    fg_color="red",
    hover_color="dark red",
    width=150,
    height=35
)
delete_btn.pack(pady=10)

# Consultar Dados
query_tab = tabview.tab("Consultar Dados")

# Título
query_title = customtkinter.CTkLabel(
    query_tab,
    text="Consultar Dados",
    font=("Arial", 16, "bold")
)
query_title.pack(pady=10)

# Frame para seleção de tabela na consulta
query_table_frame = customtkinter.CTkFrame(query_tab)
query_table_frame.pack(pady=10, padx=20, fill="x")

label_query_table = customtkinter.CTkLabel(
    query_table_frame,
    text="Tabela:",
    font=("Arial", 12)
)
label_query_table.pack(side="left", padx=10)

query_table_combo = customtkinter.CTkComboBox(
    query_table_frame,
    values=[],
    width=200,
    state="readonly"
)
query_table_combo.pack(side="left", padx=10)

query_refresh_btn = customtkinter.CTkButton(
    query_table_frame,
    text="Atualizar",
    width=100
)
query_refresh_btn.pack(side="left", padx=10)

query_all_btn = customtkinter.CTkButton(
    query_table_frame,
    text="Consultar Todos",
    width=120
)
query_all_btn.pack(side="left", padx=10)

# Frame para filtros
filter_frame = customtkinter.CTkFrame(query_tab)
filter_frame.pack(pady=10, padx=20, fill="x")

label_filter = customtkinter.CTkLabel(
    filter_frame,
    text="Filtros:",
    font=("Arial", 12)
)
label_filter.pack(side="left", padx=10)

filter_column_combo = customtkinter.CTkComboBox(
    filter_frame,
    values=[],
    width=150
)
filter_column_combo.pack(side="left", padx=5)

filter_operator_combo = customtkinter.CTkComboBox(
    filter_frame,
    values=["=", "!=", ">", "<", ">=", "<=", "LIKE"],
    width=100
)
filter_operator_combo.pack(side="left", padx=5)

filter_value_entry = customtkinter.CTkEntry(
    filter_frame,
    placeholder_text="Valor",
    width=150
)
filter_value_entry.pack(side="left", padx=5)

apply_filter_btn = customtkinter.CTkButton(
    filter_frame,
    text="Aplicar Filtro",
    width=120
)
apply_filter_btn.pack(side="left", padx=5)

clear_filter_btn = customtkinter.CTkButton(
    filter_frame,
    text="Limpar",
    width=80
)
clear_filter_btn.pack(side="left", padx=5)

# Frame para resultados
results_frame = customtkinter.CTkFrame(query_tab)
results_frame.pack(pady=10, padx=20, fill="both", expand=True)

# Criar Treeview para mostrar resultados
tree_frame = customtkinter.CTkFrame(results_frame)
tree_frame.pack(pady=10, padx=10, fill="both", expand=True)

# Treeview
tree_scroll_y = tk.Scrollbar(tree_frame)
tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

tree_scroll_x = tk.Scrollbar(tree_frame, orient=tk.HORIZONTAL)
tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)

results_tree = ttk.Treeview(
    tree_frame,
    yscrollcommand=tree_scroll_y.set,
    xscrollcommand=tree_scroll_x.set,
    selectmode="extended"
)
results_tree.pack(fill="both", expand=True)

tree_scroll_y.config(command=results_tree.yview)
tree_scroll_x.config(command=results_tree.xview)

# Label para estatísticas
stats_label = customtkinter.CTkLabel(
    query_tab,
    text="Total de registos: 0",
    font=("Arial", 12)
)
stats_label.pack(pady=5)

# Alertas e Monitorização
alerts_tab = tabview.tab("Alertas e Monitorização")

# Título
alerts_title = customtkinter.CTkLabel(
    alerts_tab,
    text="Alertas e Monitorização",
    font=("Arial", 16, "bold")
)
alerts_title.pack(pady=10)

# Frame para notificações
notifications_frame = customtkinter.CTkFrame(alerts_tab)
notifications_frame.pack(pady=10, padx=20, fill="x")

label_notifications = customtkinter.CTkLabel(
    notifications_frame,
    text="Configurações de Notificação:",
    font=("Arial", 12, "bold")
)
label_notifications.pack(anchor="w", padx=10, pady=5)

# Email para notificações
email_frame = customtkinter.CTkFrame(notifications_frame)
email_frame.pack(fill="x", padx=10, pady=5)

label_email = customtkinter.CTkLabel(
    email_frame,
    text="Email para notificações:",
    font=("Arial", 11)
)
label_email.pack(side="left", padx=5)

email_entry = customtkinter.CTkEntry(
    email_frame,
    placeholder_text="seu@email.com",
    width=250
)
email_entry.pack(side="left", padx=5)

# Checkboxes para tipos de alerta
alerts_frame = customtkinter.CTkFrame(notifications_frame)
alerts_frame.pack(fill="x", padx=10, pady=5)

high_priority_var = customtkinter.BooleanVar(value=True)
high_priority_check = customtkinter.CTkCheckBox(
    alerts_frame,
    text="Alertas de Alta Prioridade (Nível 1)",
    variable=high_priority_var,
    font=("Arial", 11)
)
high_priority_check.pack(anchor="w", padx=5, pady=2)

medium_priority_var = customtkinter.BooleanVar(value=False)
medium_priority_check = customtkinter.CTkCheckBox(
    alerts_frame,
    text="Alertas de Média Prioridade (Nível 2)",
    variable=medium_priority_var,
    font=("Arial", 11)
)
medium_priority_check.pack(anchor="w", padx=5, pady=2)

low_priority_var = customtkinter.BooleanVar(value=False)
low_priority_check = customtkinter.CTkCheckBox(
    alerts_frame,
    text="Alertas de Baixa Prioridade (Nível 3)",
    variable=low_priority_var,
    font=("Arial", 11)
)
low_priority_check.pack(anchor="w", padx=5, pady=2)

# Botão para salvar configurações
save_notifications_btn = customtkinter.CTkButton(
    notifications_frame,
    text="Salvar Configurações",
    width=200,
    command=lambda: save_notification_settings()
)
save_notifications_btn.pack(pady=10)

# Separador
separator = customtkinter.CTkFrame(alerts_tab, height=2, fg_color="gray")
separator.pack(fill="x", padx=20, pady=10)

# Frame para filtros de alertas
filters_frame = customtkinter.CTkFrame(alerts_tab)
filters_frame.pack(pady=10, padx=20, fill="x")

label_filter = customtkinter.CTkLabel(
    filters_frame,
    text="Filtrar Alertas:",
    font=("Arial", 12, "bold")
)
label_filter.pack(anchor="w", padx=10, pady=5)

# Filtro por prioridade
priority_filter_frame = customtkinter.CTkFrame(filters_frame)
priority_filter_frame.pack(fill="x", padx=10, pady=5)

label_priority = customtkinter.CTkLabel(
    priority_filter_frame,
    text="Prioridade:",
    font=("Arial", 11)
)
label_priority.pack(side="left", padx=5)

priority_combo = customtkinter.CTkComboBox(
    priority_filter_frame,
    values=["Todas", "Alta", "Média", "Baixa"],
    width=120,
    state="readonly"
)
priority_combo.pack(side="left", padx=5)
priority_combo.set("Todas")

# Botões de ação
buttons_frame = customtkinter.CTkFrame(filters_frame)
buttons_frame.pack(fill="x", padx=10, pady=10)

load_alerts_btn = customtkinter.CTkButton(
    buttons_frame,
    text="Carregar Alertas",
    width=150,
    command=lambda: load_active_alerts()
)
load_alerts_btn.pack(side="left", padx=5)

clear_filters_btn = customtkinter.CTkButton(
    buttons_frame,
    text="Limpar Filtros",
    width=150,
    command=lambda: clear_alert_filters()
)
clear_filters_btn.pack(side="left", padx=5)

# Frame para lista de alertas
alerts_list_frame = customtkinter.CTkFrame(alerts_tab)
alerts_list_frame.pack(pady=10, padx=20, fill="both", expand=True)

# Treeview para alertas
alerts_tree_scroll_y = tk.Scrollbar(alerts_list_frame)
alerts_tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

alerts_tree_scroll_x = tk.Scrollbar(alerts_list_frame, orient=tk.HORIZONTAL)
alerts_tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)

alerts_tree = ttk.Treeview(
    alerts_list_frame,
    yscrollcommand=alerts_tree_scroll_y.set,
    xscrollcommand=alerts_tree_scroll_x.set,
    selectmode="extended",
    columns=("ID", "Asteroide", "Data", "Prioridade", "Descrição", "Ativo")
)
alerts_tree.pack(fill="both", expand=True)

alerts_tree_scroll_y.config(command=alerts_tree.yview)
alerts_tree_scroll_x.config(command=alerts_tree.xview)

# Configurar colunas ATUALIZADAS
alerts_tree.heading("#0", text="")
alerts_tree.heading("ID", text="ID")
alerts_tree.heading("Asteroide", text="Asteroide")
alerts_tree.heading("Data", text="Data")
alerts_tree.heading("Prioridade", text="Prioridade")
alerts_tree.heading("Descrição", text="Descrição")
alerts_tree.heading("Ativo", text="Ativo")

alerts_tree.column("#0", width=0, stretch=False)
alerts_tree.column("ID", width=50, minwidth=50)
alerts_tree.column("Asteroide", width=150, minwidth=100)
alerts_tree.column("Data", width=120, minwidth=100)
alerts_tree.column("Prioridade", width=80, minwidth=80)
alerts_tree.column("Descrição", width=250, minwidth=200)
alerts_tree.column("Ativo", width=60, minwidth=60)

# Label para estatísticas
alerts_stats_label = customtkinter.CTkLabel(
    alerts_tab,
    text="Total de alertas ativos: 0",
    font=("Arial", 11)
)
alerts_stats_label.pack(pady=5)

# Estatísticas e Gráficos
stats_tab = tabview.tab("Estatísticas e Gráficos")

# Criar um notebook dentro desta aba para separar estatísticas numéricas e gráficos
stats_notebook = customtkinter.CTkTabview(stats_tab)
stats_notebook.pack(padx=20, pady=20, fill="both", expand=True)

# Adicionar sub-abas no notebook
stats_notebook.add("Estatísticas Numéricas")
stats_notebook.add("Gráficos")

# Sub-aba 1 - Estatísticas Numéricas
stats_numericas_tab = stats_notebook.tab("Estatísticas Numéricas")

# Título da sub-aba
stats_numeric_title = customtkinter.CTkLabel(
    stats_numericas_tab,
    text="Estatísticas do Sistema",
    font=("Arial", 16, "bold")
)
stats_numeric_title.pack(pady=10)

# Botão para carregar estatísticas
stats_button_frame = customtkinter.CTkFrame(stats_numericas_tab)
stats_button_frame.pack(pady=10, padx=20, fill="x")

load_stats_btn = customtkinter.CTkButton(
    stats_button_frame,
    text="Atualizar Estatísticas",
    width=200,
    command=lambda: load_statistics()
)
load_stats_btn.pack(pady=10)

# Frame para mostrar estatísticas numéricas
stats_numeric_frame = customtkinter.CTkScrollableFrame(stats_numericas_tab)
stats_numeric_frame.pack(pady=10, padx=20, fill="both", expand=True)

# Sub-aba 2: Gráficos
graficos_tab = stats_notebook.tab("Gráficos")

# Título da sub-aba de gráficos
graficos_title = customtkinter.CTkLabel(
    graficos_tab,
    text="Visualização Gráfica de Estatísticas",
    font=("Arial", 16, "bold")
)
graficos_title.pack(pady=10)

# Frame para controles dos gráficos
graficos_controls_frame = customtkinter.CTkFrame(graficos_tab)
graficos_controls_frame.pack(pady=10, padx=20, fill="x")

# Combobox para selecionar tipo de gráfico
label_grafico = customtkinter.CTkLabel(
    graficos_controls_frame,
    text="Tipo de Gráfico:",
    font=("Arial", 12)
)
label_grafico.pack(side="left", padx=10)

grafico_combo = customtkinter.CTkComboBox(
    graficos_controls_frame,
    values=["Alertas por Prioridade", "Distribuição por Tamanho", "Classificação de Asteroides", "Evolução da Precisão"],
    width=200,
    state="readonly"
)
grafico_combo.pack(side="left", padx=10)
grafico_combo.set("Alertas por Prioridade")

# Botão para gerar gráfico
gerar_grafico_btn = customtkinter.CTkButton(
    graficos_controls_frame,
    text="Gerar Gráfico",
    width=150,
    command=lambda: criar_grafico_selecionado()
)
gerar_grafico_btn.pack(side="left", padx=10)

# Botão para atualizar dados dos gráficos
atualizar_dados_btn = customtkinter.CTkButton(
    graficos_controls_frame,
    text="Atualizar Dados",
    width=150,
    command=lambda: criar_grafico_selecionado()
)
atualizar_dados_btn.pack(side="left", padx=10)

# Frame para mostrar o gráfico
grafico_frame = customtkinter.CTkFrame(graficos_tab)
grafico_frame.pack(pady=10, padx=20, fill="both", expand=True)

# Label inicial para instruções
instrucoes_label = customtkinter.CTkLabel(
    grafico_frame,
    text="Selecione um tipo de gráfico e clique em 'Gerar Gráfico' para visualizar.\n\nCertifique-se de ter o matplotlib instalado: pip install matplotlib",
    font=("Arial", 12),
    text_color="gray",
    justify="center"
)
instrucoes_label.pack(pady=50)

# Consultas específicas
consultas_tab = tabview.tab("Consultas Específicas")

# Título
consultas_title = customtkinter.CTkLabel(
    consultas_tab,
    text="Consultas Específicas (Views)",
    font=("Arial", 16, "bold")
)
consultas_title.pack(pady=10)

# Frame para seleção de view
view_select_frame = customtkinter.CTkFrame(consultas_tab)
view_select_frame.pack(pady=10, padx=20, fill="x")

label_view = customtkinter.CTkLabel(
    view_select_frame,
    text="Selecionar View:",
    font=("Arial", 12)
)
label_view.pack(side="left", padx=10)

# Lista das views disponíveis
views_list = [
    "vw_Ranking_Maiores_PHA",
    "vw_Proximos_Eventos_Criticos",
    "vw_Estatisticas_Centros",
    "vw_Evolucao_Precisao",
    "vw_Alertas_Ativos",
    "vw_Estatisticas_Alertas",
    "vw_Novos_NEOs_Ultimo_Mes"
]

view_combo = customtkinter.CTkComboBox(
    view_select_frame,
    values=views_list,
    width=250,
    state="readonly"
)
view_combo.pack(side="left", padx=10)

execute_view_btn = customtkinter.CTkButton(
    view_select_frame,
    text="Executar View",
    width=120,
    command=lambda: executar_view_selecionada(view_combo.get())
)
execute_view_btn.pack(side="left", padx=10)

# Frame para resultados da view
view_results_frame = customtkinter.CTkFrame(consultas_tab)
view_results_frame.pack(pady=10, padx=20, fill="both", expand=True)

# Treeview para mostrar resultados da view
view_tree_scroll_y = tk.Scrollbar(view_results_frame)
view_tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

view_tree_scroll_x = tk.Scrollbar(view_results_frame, orient=tk.HORIZONTAL)
view_tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)

view_tree = ttk.Treeview(
    view_results_frame,
    yscrollcommand=view_tree_scroll_y.set,
    xscrollcommand=view_tree_scroll_x.set,
    selectmode="extended"
)
view_tree.pack(fill="both", expand=True)

view_tree_scroll_y.config(command=view_tree.yview)
view_tree_scroll_x.config(command=view_tree.xview)

# Label para estatísticas da view
view_stats_label = customtkinter.CTkLabel(
    consultas_tab,
    text="",
    font=("Arial", 12)
)
view_stats_label.pack(pady=5)

# Gestões de Triggers
triggers_tab = tabview.tab("Gestão de Triggers")

# Título
triggers_title = customtkinter.CTkLabel(
    triggers_tab,
    text="Gestão de Triggers do Sistema",
    font=("Arial", 16, "bold")
)
triggers_title.pack(pady=10)

# Frame para configuração
config_frame = customtkinter.CTkFrame(triggers_tab)
config_frame.pack(pady=10, padx=20, fill="x")

# Botão para configurar triggers
setup_triggers_btn = customtkinter.CTkButton(
    config_frame,
    text="Configurar Triggers (triggers.txt)",
    command=lambda: configurar_triggers(),
    fg_color="purple4",
    hover_color="purple3",
    width=250
)
setup_triggers_btn.pack(pady=10)

# Botão para configurar views
setup_views_btn = customtkinter.CTkButton(
    config_frame,
    text="Configurar Views (queries.txt)",
    command=lambda: configurar_views(),
    fg_color="blue",
    hover_color="dark blue",
    width=250
)
setup_views_btn.pack(pady=10)

# Botão para criar tabela de notificações
create_notif_table_btn = customtkinter.CTkButton(
    config_frame,
    text="Criar Tabela Notificações",
    command=lambda: create_notification_table_wrapper(),
    fg_color="green",
    hover_color="dark green",
    width=250
)
create_notif_table_btn.pack(pady=10)

# Botão para verificar triggers
check_triggers_btn = customtkinter.CTkButton(
    config_frame,
    text="Verificar Triggers",
    command=lambda: verificar_triggers(),
    width=200
)
check_triggers_btn.pack(pady=10)

# Frame para lista de triggers
list_frame = customtkinter.CTkFrame(triggers_tab)
list_frame.pack(pady=10, padx=20, fill="both", expand=True)

# Treeview para mostrar triggers
triggers_tree_scroll_y = tk.Scrollbar(list_frame)
triggers_tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

triggers_tree_scroll_x = tk.Scrollbar(list_frame, orient=tk.HORIZONTAL)
triggers_tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)

triggers_tree = ttk.Treeview(
    list_frame,
    yscrollcommand=triggers_tree_scroll_y.set,
    xscrollcommand=triggers_tree_scroll_x.set,
    selectmode="extended",
    columns=("Nome", "Tabela", "Estado", "Criado em")
)
triggers_tree.pack(fill="both", expand=True)

triggers_tree_scroll_y.config(command=triggers_tree.yview)
triggers_tree_scroll_x.config(command=triggers_tree.xview)

# Configurar colunas do treeview
triggers_tree.heading("#0", text="")
triggers_tree.heading("Nome", text="Nome do Trigger")
triggers_tree.heading("Tabela", text="Tabela Associada")
triggers_tree.heading("Estado", text="Estado")
triggers_tree.heading("Criado em", text="Criado em")

triggers_tree.column("#0", width=0, stretch=False)
triggers_tree.column("Nome", width=200, minwidth=100)
triggers_tree.column("Tabela", width=150, minwidth=100)
triggers_tree.column("Estado", width=100, minwidth=80)
triggers_tree.column("Criado em", width=150, minwidth=100)

# Frame para ações
actions_frame = customtkinter.CTkFrame(triggers_tab)
actions_frame.pack(pady=10, padx=20, fill="x")

# Botão para atualizar lista
refresh_triggers_btn = customtkinter.CTkButton(
    actions_frame,
    text="Atualizar Lista",
    command=lambda: atualizar_lista_triggers(),
    width=120
)
refresh_triggers_btn.pack(side="left", padx=5)

# Botão para ativar trigger
enable_trigger_btn = customtkinter.CTkButton(
    actions_frame,
    text="Ativar Trigger",
    command=lambda: ativar_trigger_selecionado(),
    fg_color="green",
    hover_color="dark green",
    width=120
)
enable_trigger_btn.pack(side="left", padx=5)

# Botão para desativar trigger
disable_trigger_btn = customtkinter.CTkButton(
    actions_frame,
    text="Desativar Trigger",
    command=lambda: desativar_trigger_selecionado(),
    fg_color="orange",
    hover_color="dark orange",
    width=120
)
disable_trigger_btn.pack(side="left", padx=5)

# Botão para eliminar trigger
delete_trigger_btn = customtkinter.CTkButton(
    actions_frame,
    text="Eliminar Trigger",
    command=lambda: eliminar_trigger_selecionado(),
    fg_color="red",
    hover_color="dark red",
    width=120
)
delete_trigger_btn.pack(side="left", padx=5)

# Label de status
triggers_status = customtkinter.CTkLabel(
    triggers_tab,
    text="",
    font=("Arial", 12)
)
triggers_status.pack(pady=5)

# Funções de interface

def ligar_bd():
    servidor = entry_server.get().strip()
    database = entry_database.get().strip()

    if not servidor:
        messagebox.showerror("Erro", "Por favor, insira o nome do servidor")
        return

    if not database:
        messagebox.showerror("Erro", "Por favor, insira o nome da base de dados")
        return

    porta = entry_port.get().strip()
    usuario = entry_user.get().strip()
    password = entry_password.get()

    success, message, connection_info = connect_to_db(servidor, database, usuario, password, porta)

    if success:
        connection_status.configure(
            text=f"Conectado a: {connection_info['database_name']}",
            text_color="green"
        )

        messagebox.showinfo(
            "Conexão Estabelecida",
            f"Conectado com sucesso!\n\n"
            f"Base de Dados: {connection_info['database_name']}\n"
            f"SQL Server: {connection_info['sql_version']}"
        )

        # Atualizar lista de tabelas após conectar
        atualizar_lista_tabelas()
        add_column_field()

        # Criar tabela de notificações se não existir
        success_notif, message_notif = create_notification_table()
        if success_notif:
            print(f"{message_notif}")
        else:
            print(f"{message_notif}")

        # Carregar configurações de notificação se existir email
        if email_entry.get().strip():
            carregar_configuracoes_notificacao()

        # Iniciar verificação periódica de alertas (após 5 segundos)
        app.after(5000, check_new_alerts_periodic)

    else:
        connection_status.configure(text="Não conectado", text_color="red")
        messagebox.showerror("Erro de Conexão", message)

def atualizar_lista_tabelas():
    try:
        tabelas = get_all_tables()

        # Atualizar comboboxes
        table_combo.configure(values=tabelas)
        query_table_combo.configure(values=tabelas)

        if tabelas:
            messagebox.showinfo("Sucesso", f"Foram encontradas {len(tabelas)} tabelas na base de dados.")
        else:
            messagebox.showinfo("Informação", "Não foram encontradas tabelas na base de dados.")
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao obter tabelas: {str(e)}")

def create_table():
    table_name = entry_table_name.get().strip()
    if not table_name:
        messagebox.showerror("Erro", "Por favor, insira um nome para a tabela")
        return

    if len(columns_list) == 0:
        messagebox.showerror("Erro", "Adicione pelo menos uma coluna")
        return

    success, message = create_table_in_db(table_name, columns_list)

    if success:
        create_info_label.configure(text=message, text_color="green")

        # Limpar campos
        entry_table_name.delete(0, 'end')
        for col in columns_list[:]:
            col['frame'].destroy()
        columns_list.clear()

        # Atualizar lista de tabelas
        atualizar_lista_tabelas()
        add_column_field()
    else:
        create_info_label.configure(text=f"Erro: {message}", text_color="red")

def load_table_for_crud():
    global current_table, insert_fields_cache, update_fields_cache
    table_name = table_combo.get()

    if not table_name:
        clear_crud_fields()
        return

    current_table = table_name
    colunas = get_table_structure(table_name)

    if not colunas:
        messagebox.showwarning("Aviso", f"Não foi possível obter a estrutura da tabela '{table_name}'")
        return

    clear_crud_fields()
    primary_key = get_primary_key(table_name)

    # Criar campos dinamicamente para INSERT
    insert_fields_cache = []
    for coluna in colunas:
        if coluna.get('identity'):
            continue

        field_frame = customtkinter.CTkFrame(insert_fields_frame)
        field_frame.pack(pady=5, fill="x", padx=10)

        label_text = f"{coluna['name']}"
        if coluna['type']:
            label_text += f" ({coluna['type']})"

        label = customtkinter.CTkLabel(
            field_frame,
            text=label_text,
            width=200,
            anchor="w"
        )
        label.pack(side="left", padx=5)

        entry = customtkinter.CTkEntry(
            field_frame,
            placeholder_text=f"Valor para {coluna['name']}",
            width=250
        )
        entry.pack(side="left", padx=5)

        if not coluna['nullable']:
            obrigatorio_label = customtkinter.CTkLabel(
                field_frame,
                text="*",
                text_color="red",
                font=("Arial", 14, "bold")
            )
            obrigatorio_label.pack(side="left", padx=5)

        insert_fields_cache.append({
            'name': coluna['name'],
            'entry': entry,
            'type': coluna['type'],
            'nullable': coluna['nullable']
        })

    def insert_wrapper():
        insert_record(insert_fields_cache, primary_key)

    insert_btn.configure(command=insert_wrapper)

    # Criar campos dinamicamente para UPDATE
    update_fields_cache = []
    for coluna in colunas:
        if primary_key and coluna['name'] == primary_key:
            continue

        field_frame = customtkinter.CTkFrame(update_fields_frame)
        field_frame.pack(pady=5, fill="x", padx=10)

        label = customtkinter.CTkLabel(
            field_frame,
            text=coluna['name'],
            width=200,
            anchor="w"
        )
        label.pack(side="left", padx=5)

        entry = customtkinter.CTkEntry(
            field_frame,
            placeholder_text="Novo valor (deixe vazio para não alterar)",
            width=250
        )
        entry.pack(side="left", padx=5)

        update_fields_cache.append({
            'name': coluna['name'],
            'entry': entry
        })

    def load_update_wrapper():
        load_record_for_update(update_fields_cache, primary_key or "id", colunas)

    def update_wrapper():
        update_record(update_fields_cache, primary_key or "id")

    load_for_update_btn.configure(
        command=load_update_wrapper,
        state="normal" if primary_key else "disabled"
    )
    update_btn.configure(
        command=update_wrapper,
        state="normal" if primary_key else "disabled"
    )

    def delete_wrapper():
        delete_record(primary_key or "id")

    delete_btn.configure(
        command=delete_wrapper,
        state="normal" if primary_key else "disabled"
    )

    if not primary_key:
        messagebox.showwarning("Aviso",
                               f"A tabela '{table_name}' não tem uma chave primária definida.\n"
                               f"As operações de UPDATE e DELETE podem não funcionar corretamente.")

def clear_crud_fields():
    for widget in insert_fields_frame.winfo_children():
        widget.destroy()

    for widget in update_fields_frame.winfo_children():
        widget.destroy()

    update_id_entry.delete(0, 'end')
    delete_id_entry.delete(0, 'end')

def insert_record(fields, primary_key):
    global current_table

    if not current_table:
        messagebox.showerror("Erro", "Selecione uma tabela primeiro")
        return

    record_data = {
        'fields': fields,
        'primary_key': primary_key
    }

    success, message = insert_record_into_table(current_table, record_data)

    if success:
        messagebox.showinfo("Sucesso", message)
        for field in fields:
            field['entry'].delete(0, 'end')
    else:
        messagebox.showerror("Erro", message)

def load_record_for_update(fields, primary_key, colunas_info):
    global current_table

    if not current_table:
        messagebox.showerror("Erro", "Selecione uma tabela primeiro")
        return

    record_id = update_id_entry.get().strip()
    if not record_id:
        messagebox.showerror("Erro", "Insira o ID do registo")
        return

    success, result = load_record_for_update_from_db(current_table, primary_key, record_id)

    if success:
        record_dict = result
        for field in fields:
            if field['name'] in record_dict:
                value = record_dict[field['name']]
                field['entry'].delete(0, 'end')
                if value is not None:
                    field['entry'].insert(0, str(value))

        messagebox.showinfo("Sucesso", "Registo carregado para edição!")
    else:
        messagebox.showwarning("Aviso", result)

def update_record(fields, primary_key):
    global current_table

    if not current_table:
        messagebox.showerror("Erro", "Selecione uma tabela primeiro")
        return

    record_id = update_id_entry.get().strip()
    if not record_id:
        messagebox.showerror("Erro", "Insira o ID do registo")
        return

    update_data = {
        'fields': fields,
        'primary_key': primary_key,
        'record_id': record_id
    }

    success, message = update_record_in_table(current_table, update_data)

    if success:
        messagebox.showinfo("Sucesso", message)
        update_id_entry.delete(0, 'end')
        for field in fields:
            field['entry'].delete(0, 'end')
    else:
        messagebox.showerror("Erro", message)

def delete_record(primary_key):
    global current_table

    if not current_table:
        messagebox.showerror("Erro", "Selecione uma tabela primeiro")
        return

    record_id = delete_id_entry.get().strip()
    if not record_id:
        messagebox.showerror("Erro", "Insira o ID do registo")
        return

    if not messagebox.askyesno("Confirmar", f"Tem certeza que deseja excluir o registo {primary_key}={record_id}?"):
        return

    success, message = delete_record_from_table(current_table, primary_key, record_id)

    if success:
        messagebox.showinfo("Sucesso", message)
        delete_id_entry.delete(0, 'end')
    else:
        messagebox.showerror("Erro", message)

def query_table():
    table_name = query_table_combo.get()
    if not table_name:
        messagebox.showwarning("Aviso", "Selecione uma tabela")
        return

    filters = None
    filter_column = filter_column_combo.get()
    filter_operator = filter_operator_combo.get()
    filter_value = filter_value_entry.get().strip()

    if filter_column and filter_value:
        filters = {
            'column': filter_column,
            'operator': filter_operator,
            'value': filter_value
        }

    success, result = query_table_with_filters(table_name, filters)

    if success:
        columns, records = result

        # Limpar resultados anteriores
        results_tree.delete(*results_tree.get_children())
        results_tree["columns"] = columns

        # Configurar colunas
        for col in columns:
            results_tree.heading(col, text=col)
            results_tree.column(col, width=120, minwidth=50, stretch=True)

        # Adicionar registos
        for record in records:
            str_record = [str(val) if val is not None else "" for val in record]
            results_tree.insert('', 'end', values=str_record)

        # Atualizar estatísticas
        stats_label.configure(text=f"Total de registos: {len(records)}")

        # Atualizar combo de filtros
        filter_column_combo.configure(values=columns)
        if columns and filter_column_combo.get() == "":
            filter_column_combo.set(columns[0])
    else:
        messagebox.showerror("Erro", result)

def clear_filters():
    filter_column_combo.set("")
    filter_value_entry.delete(0, 'end')
    query_table()

# funções para notificações

def carregar_configuracoes_notificacao():
    """
    Carrega as configurações de notificação para o email atual
    """
    email = email_entry.get().strip()

    if not email:
        return

    success, result = load_notification_settings_for_email(email)

    if success:
        high_priority_var.set(result['high_priority'])
        medium_priority_var.set(result['medium_priority'])
        low_priority_var.set(result['low_priority'])
        print(f"Configurações carregadas para {email}")
    else:
        print(f"{result}")

def save_notification_settings():
    """
    Salva as configurações de notificação
    """
    email = email_entry.get().strip()

    if not email:
        messagebox.showerror("Erro", "Por favor, insira um email")
        return

    if '@' not in email or '.' not in email:
        messagebox.showwarning("Aviso", "Por favor, insira um email válido")
        return

    success, message = update_notification_settings(
        email=email,
        high_priority=high_priority_var.get(),
        medium_priority=medium_priority_var.get(),
        low_priority=low_priority_var.get()
    )

    if success:
        messagebox.showinfo("Sucesso", message)
    else:
        messagebox.showerror("Erro", message)

def load_active_alerts():
    """
    Carrega alertas ativos com filtros
    """
    filters = {}

    priority = priority_combo.get()
    if priority != "Todas":
        filters['priority'] = priority

    success, result = get_active_alerts(filters)

    if success:
        # Limpar treeview
        for item in alerts_tree.get_children():
            alerts_tree.delete(item)

        columns, records = result

        # Adicionar registos
        for record in records:
            # Mapear priority_level de int para texto
            priority_level = record[3]  # priority_level é int
            priority_text = {
                1: 'Alta',
                2: 'Média',
                3: 'Baixa'
            }.get(priority_level, f'Nível {priority_level}')

            # Formatar data
            alert_date = record[2]
            if alert_date:
                try:
                    date_str = alert_date.strftime("%Y-%m-%d %H:%M")
                except:
                    date_str = str(alert_date)
            else:
                date_str = ""

            # Formatar ativo
            is_active = "Sim" if record[5] else "Não"

            # Formatar nome do asteroide (pode ser None)
            asteroid_name = record[1] if record[1] else "Desconhecido"

            alerts_tree.insert('', 'end', values=(
                record[0],  # alert_id
                asteroid_name,
                date_str,
                priority_text,
                record[4] or "",  # description
                is_active  # is_active
            ))

        # Atualizar estatísticas
        alerts_stats_label.configure(text=f"Total de alertas ativos: {len(records)}")

        # Verificar se há novos alertas de alta prioridade
        if priority == "Todas" or priority == "Alta":
            check_high_priority_notifications()

    else:
        messagebox.showerror("Erro", result)
        print(f"Erro ao carregar alertas: {result}")

def clear_alert_filters():
    """
    Limpa os filtros de alertas
    """
    priority_combo.set("Todas")
    load_active_alerts()

def check_high_priority_notifications():
    """
    Verifica se há novos alertas de alta prioridade (priority_level = 1)
    """
    email = email_entry.get().strip()

    if not email:
        return

    # Verificar se o usuário quer notificações de alta prioridade
    success_settings, settings = load_notification_settings_for_email(email)

    if success_settings and settings['high_priority']:
        # Verificar novos alertas de alta prioridade (priority_level = 1)
        success_alerts, new_alerts = check_new_high_priority_alerts()

        if success_alerts and new_alerts > 0:
            # Mostrar notificação
            messagebox.showwarning(
                "NOVO ALERTA DE ALTA PRIORIDADE",
                f"Existem {new_alerts} novo(s) alerta(s) de alta prioridade (nível 1)!\n\n"
                f"Verifique a aba 'Alertas e Monitorização' para mais detalhes."
            )

def check_new_alerts_periodic():
    """
    Verificação periódica de novos alertas (a cada 60 segundos)
    """
    # Só verificar se estiver na aba de alertas
    current_tab = tabview.get()
    if current_tab == "Alertas e Monitorização":
        # Verificar notificações para o email atual
        check_high_priority_notifications()

    # Agendar próxima verificação em 60 segundos
    app.after(60000, check_new_alerts_periodic)

def load_statistics():
    """
    Carrega e exibe estatísticas completas - VERSÃO ROBUSTA
    """
    try:
        success, stats = get_statistics()

        if success:
            # Limpar frame de estatísticas
            for widget in stats_numeric_frame.winfo_children():
                widget.destroy()

            # Container principal
            main_container = customtkinter.CTkScrollableFrame(stats_numeric_frame)
            main_container.pack(pady=10, padx=10, fill="both", expand=True)

            # Título
            title_label = customtkinter.CTkLabel(
                main_container,
                text="ESTATÍSTICAS DO SISTEMA",
                font=("Arial", 18, "bold")
            )
            title_label.pack(pady=(0, 20))

            # Estatísticas gerais
            if 'total_asteroides' in stats or 'alertas_ativos' in stats:
                geral_frame = customtkinter.CTkFrame(main_container)
                geral_frame.pack(pady=5, padx=10, fill="x")

                geral_title = customtkinter.CTkLabel(
                    geral_frame,
                    text="VISÃO GERAL",
                    font=("Arial", 14, "bold")
                )
                geral_title.pack(pady=5, padx=10, anchor="w")

                geral_text = f"""
                - Total de Asteroides: {stats.get('total_asteroides', 0)}
                - Alertas Ativos: {stats.get('alertas_ativos', 0)}
                - Parâmetros Orbitais: {stats.get('total_orbitas', 0)}
                """
                geral_label = customtkinter.CTkLabel(
                    geral_frame,
                    text=geral_text,
                    font=("Arial", 12),
                    justify="left"
                )
                geral_label.pack(pady=5, padx=20, anchor="w")

            # Estatísticas de alerta
            alert_frame = customtkinter.CTkFrame(main_container)
            alert_frame.pack(pady=10, padx=10, fill="x")

            alert_title = customtkinter.CTkLabel(
                alert_frame,
                text=" ESTATÍSTICAS DE ALERTA",
                font=("Arial", 14, "bold")
            )
            alert_title.pack(pady=5, padx=10, anchor="w")

            alert_text = f"""
            - Alertas Vermelhos (Nível 4): {stats.get('alertas_vermelhos', 0)}
            - Alertas Laranja (Nível 3): {stats.get('alertas_laranja', 0)}
            - Total de PHAs (diâmetro > 100m): {stats.get('total_phas_100m', 0)}
            """

            alert_label = customtkinter.CTkLabel(
                alert_frame,
                text=alert_text,
                font=("Arial", 12),
                justify="left"
            )
            alert_label.pack(pady=5, padx=20, anchor="w")

            # Próximo evento crítico
            event_frame = customtkinter.CTkFrame(main_container)
            event_frame.pack(pady=10, padx=10, fill="x")

            event_title = customtkinter.CTkLabel(
                event_frame,
                text=" PRÓXIMO EVENTO CRÍTICO",
                font=("Arial", 14, "bold")
            )
            event_title.pack(pady=5, padx=10, anchor="w")

            evento = stats.get('proximo_evento_critico')
            if evento:
                event_text = f"""
                - Asteroide: {evento.get('asteroide', 'Desconhecido')}
                - Distância: {evento.get('distancia_ld', 0):.2f} LD
                - Data Prevista: {evento.get('data', 'Desconhecida')}
                """
            else:
                event_text = "- Nenhum evento crítico previsto para os próximos dias."

            event_label = customtkinter.CTkLabel(
                event_frame,
                text=event_text,
                font=("Arial", 12),
                justify="left"
            )
            event_label.pack(pady=5, padx=20, anchor="w")

            # Estatística de descoberta
            discovery_frame = customtkinter.CTkFrame(main_container)
            discovery_frame.pack(pady=10, padx=10, fill="x")

            discovery_title = customtkinter.CTkLabel(
                discovery_frame,
                text="ESTATÍSTICAS DE DESCOBERTA",
                font=("Arial", 14, "bold")
            )
            discovery_title.pack(pady=5, padx=10, anchor="w")

            discovery_text = f"""
            - Novos NEOs (último mês): {stats.get('novos_neos_ultimo_mes', 0)}
            - Total de NEOs: {stats.get('total_neos', 0)}
            """

            discovery_label = customtkinter.CTkLabel(
                discovery_frame,
                text=discovery_text,
                font=("Arial", 12),
                justify="left"
            )
            discovery_label.pack(pady=5, padx=20, anchor="w")

            # Evolução da precisão
            precision_frame = customtkinter.CTkFrame(main_container)
            precision_frame.pack(pady=10, padx=10, fill="x")

            precision_title = customtkinter.CTkLabel(
                precision_frame,
                text="EVOLUÇÃO DA PRECISÃO ORBITAL",
                font=("Arial", 14, "bold")
            )
            precision_title.pack(pady=5, padx=10, anchor="w")

            evolucao = stats.get('evolucao_precisao', [])
            if evolucao:
                precision_text = "RMS Médio:\n"
                for item in evolucao:
                    precision_text += f"  - Ano {item.get('ano', 'N/A')}: {item.get('rms_medio', 0):.3f} (baseado em {item.get('qtd_calculos', 0)} cálculos)\n"
            else:
                precision_text = "- Dados insuficientes para análise de precisão."

            precision_label = customtkinter.CTkLabel(
                precision_frame,
                text=precision_text,
                font=("Arial", 12),
                justify="left"
            )
            precision_label.pack(pady=5, padx=20, anchor="w")

            # Distribuição por classificação
            classificacoes = stats.get('classificacoes', {})
            if classificacoes:
                class_frame = customtkinter.CTkFrame(main_container)
                class_frame.pack(pady=10, padx=10, fill="x")

                class_title = customtkinter.CTkLabel(
                    class_frame,
                    text="DISTRIBUIÇÃO POR CLASSIFICAÇÃO",
                    font=("Arial", 14, "bold")
                )
                class_title.pack(pady=5, padx=10, anchor="w")

                class_text = ""
                for classe, quantidade in list(classificacoes.items())[:5]:
                    class_text += f"  • {classe}: {quantidade}\n"

                class_label = customtkinter.CTkLabel(
                    class_frame,
                    text=class_text,
                    font=("Arial", 12),
                    justify="left"
                )
                class_label.pack(pady=5, padx=20, anchor="w")

            # Distribuição por tamanho
            distribuicao_tamanhos = stats.get('distribuicao_tamanhos', {})
            if distribuicao_tamanhos:
                size_frame = customtkinter.CTkFrame(main_container)
                size_frame.pack(pady=10, padx=10, fill="x")

                size_title = customtkinter.CTkLabel(
                    size_frame,
                    text="DISTRIBUIÇÃO POR TAMANHO",
                    font=("Arial", 14, "bold")
                )
                size_title.pack(pady=5, padx=10, anchor="w")

                size_text = ""
                for categoria, quantidade in distribuicao_tamanhos.items():
                    size_text += f"  • {categoria}: {quantidade}\n"

                size_label = customtkinter.CTkLabel(
                    size_frame,
                    text=size_text,
                    font=("Arial", 12),
                    justify="left"
                )
                size_label.pack(pady=5, padx=20, anchor="w")

        else:
            # Mostrar erro de forma mais amigável
            error_frame = customtkinter.CTkFrame(stats_numeric_frame)
            error_frame.pack(pady=20, padx=20, fill="both", expand=True)

            error_label = customtkinter.CTkLabel(
                error_frame,
                text=f"Erro ao carregar estatísticas:\n{stats}",
                font=("Arial", 12),
                text_color="red",
                justify="left"
            )
            error_label.pack(pady=10)

            # Botão para tentar novamente
            retry_btn = customtkinter.CTkButton(
                error_frame,
                text="Tentar Novamente",
                command=load_statistics,
                width=150
            )
            retry_btn.pack(pady=10)

    except Exception as e:
        messagebox.showerror("Erro", f"Falha crítica ao carregar estatísticas: {str(e)}")

def criar_grafico_selecionado():
    """
    Cria o gráfico selecionado pelo usuário
    """
    tipo_grafico = grafico_combo.get()

    # Limpar frame do gráfico
    for widget in grafico_frame.winfo_children():
        widget.destroy()

    # Obter estatísticas
    success, stats = get_statistics()

    if not success:
        error_label = customtkinter.CTkLabel(
            grafico_frame,
            text=f"Erro ao carregar dados: {stats}",
            text_color="red",
            font=("Arial", 12)
        )
        error_label.pack(pady=50)
        return

    try:
        # Importar matplotlib
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

        # Criar figura
        fig, ax = plt.subplots(figsize=(8, 6))

        if tipo_grafico == "Alertas por Prioridade":
            # Dados para o gráfico de alertas
            niveis = ["Vermelho (4)", "Laranja (3)", "Amarelo (2)", "Verde (1)"]
            quantidades = [
                stats.get('alertas_vermelhos', 0),
                stats.get('alertas_laranja', 0),
                stats.get('alertas_amarelos', 0) if 'alertas_amarelos' in stats else 0,
                stats.get('alertas_verdes', 0) if 'alertas_verdes' in stats else 0
            ]

            # Filtrar apenas níveis com quantidade > 0
            dados = [(n, q) for n, q in zip(niveis, quantidades) if q > 0]
            if dados:
                niveis_filtrado, quantidades_filtrado = zip(*dados)
                cores = ['#ff4444', '#ffa500', '#ffff44', '#44ff44'][:len(niveis_filtrado)]

                bars = ax.bar(niveis_filtrado, quantidades_filtrado, color=cores)
                ax.set_title('Alertas por Nível de Prioridade', fontsize=14, fontweight='bold')
                ax.set_xlabel('Nível de Prioridade')
                ax.set_ylabel('Quantidade')

                # Adicionar valores nas barras
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width() / 2., height,
                            f'{int(height)}', ha='center', va='bottom')
            else:
                ax.text(0.5, 0.5, 'Não há alertas ativos',
                        ha='center', va='center', fontsize=12)
                ax.set_title('Alertas por Nível de Prioridade', fontsize=14, fontweight='bold')

        elif tipo_grafico == "Distribuição por Tamanho":
            distribuicao = stats.get('distribuicao_tamanhos', {})
            if distribuicao:
                categorias = list(distribuicao.keys())
                quantidades = list(distribuicao.values())

                # Criar gráfico de pizza
                wedges, texts, autotexts = ax.pie(
                    quantidades,
                    labels=categorias,
                    autopct='%1.1f%%',
                    startangle=90
                )
                ax.set_title('Distribuição de Asteroides por Tamanho', fontsize=14, fontweight='bold')

                # Melhorar legibilidade
                for autotext in autotexts:
                    autotext.set_color('white')
                    autotext.set_fontsize(10)
            else:
                ax.text(0.5, 0.5, 'Sem dados de distribuição',
                        ha='center', va='center', fontsize=12)
                ax.set_title('Distribuição por Tamanho', fontsize=14, fontweight='bold')

        elif tipo_grafico == "Classificação de Asteroides":
            classificacoes = stats.get('classificacoes', {})
            if classificacoes:
                # Pegar top 10 classificações
                items = sorted(classificacoes.items(), key=lambda x: x[1], reverse=True)[:10]
                categorias = [item[0] for item in items]
                quantidades = [item[1] for item in items]

                # Criar gráfico de barras horizontais
                bars = ax.barh(range(len(categorias)), quantidades)
                ax.set_yticks(range(len(categorias)))
                ax.set_yticklabels(categorias)
                ax.set_title('Top 10 Classificações de Asteroides', fontsize=14, fontweight='bold')
                ax.set_xlabel('Quantidade')

                # Adicionar valores nas barras
                for i, (bar, qtd) in enumerate(zip(bars, quantidades)):
                    width = bar.get_width()
                    ax.text(width, bar.get_y() + bar.get_height() / 2.,
                            f' {qtd}', va='center')
            else:
                ax.text(0.5, 0.5, 'Sem dados de classificação',
                        ha='center', va='center', fontsize=12)
                ax.set_title('Classificação de Asteroides', fontsize=14, fontweight='bold')

        elif tipo_grafico == "Evolução da Precisão":
            evolucao = stats.get('evolucao_precisao', [])
            if evolucao and len(evolucao) > 1:
                # Ordenar por ano
                evolucao_ordenada = sorted(evolucao, key=lambda x: x.get('ano', 0))
                anos = [item.get('ano', 0) for item in evolucao_ordenada]
                rms_medio = [item.get('rms_medio', 0) for item in evolucao_ordenada]

                ax.plot(anos, rms_medio, marker='o', linewidth=2, markersize=8)
                ax.set_title('Evolução da Precisão Orbital (RMS)', fontsize=14, fontweight='bold')
                ax.set_xlabel('Ano')
                ax.set_ylabel('RMS Médio')
                ax.grid(True, alpha=0.3)

                # Adicionar valores nos pontos
                for i, (ano, rms) in enumerate(zip(anos, rms_medio)):
                    ax.text(ano, rms, f' {rms:.3f}', va='bottom' if i % 2 == 0 else 'top')
            else:
                ax.text(0.5, 0.5, 'Dados insuficientes para análise temporal',
                        ha='center', va='center', fontsize=12)
                ax.set_title('Evolução da Precisão', fontsize=14, fontweight='bold')

        # Ajustar layout
        plt.tight_layout()

        # Embeddar o gráfico no tkinter
        canvas = FigureCanvasTkAgg(fig, master=grafico_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

        # Botão para salvar o gráfico
        def salvar_grafico():
            from tkinter import filedialog
            file_path = filedialog.asksaveasfilename(
                defaultextension=".png",
                filetypes=[("PNG files", "*.png"), ("PDF files", "*.pdf"), ("All files", "*.*")]
            )
            if file_path:
                fig.savefig(file_path, dpi=300, bbox_inches='tight')
                messagebox.showinfo("Sucesso", f"Gráfico salvo em:\n{file_path}")

        salvar_btn = customtkinter.CTkButton(
            grafico_frame,
            text="Salvar Gráfico",
            command=salvar_grafico,
            width=150
        )
        salvar_btn.pack(pady=10)

    except ImportError:
        # Se matplotlib não estiver instalado
        error_label = customtkinter.CTkLabel(
            grafico_frame,
            text="Biblioteca matplotlib não instalada!\n\nInstale com: pip install matplotlib",
            text_color="red",
            font=("Arial", 12),
            justify="center"
        )
        error_label.pack(pady=50)

        install_btn = customtkinter.CTkButton(
            grafico_frame,
            text="Instalar Matplotlib",
            command=lambda: instalar_matplotlib(),
            width=200
        )
        install_btn.pack(pady=10)

    except Exception as e:
        error_label = customtkinter.CTkLabel(
            grafico_frame,
            text=f"Erro ao criar gráfico: {str(e)}",
            text_color="red",
            font=("Arial", 12)
        )
        error_label.pack(pady=50)

def instalar_matplotlib():
    """
    Tenta instalar o matplotlib (requer permissões)
    """
    import subprocess
    import sys

    try:
        # Mostrar mensagem de progresso
        progress_label = customtkinter.CTkLabel(
            grafico_frame,
            text="Instalando matplotlib...\nIsso pode levar alguns segundos.",
            text_color="blue",
            font=("Arial", 11)
        )
        progress_label.pack(pady=10)

        # Instalar via pip
        subprocess.check_call([sys.executable, "-m", "pip", "install", "matplotlib"])

        progress_label.configure(
            text="Matplotlib instalado com sucesso!\nReinicie a aplicação.",
            text_color="green"
        )

    except Exception as e:
        progress_label.configure(
            text=f"Erro na instalação: {str(e)}\n\nInstale manualmente:\npip install matplotlib",
            text_color="red"
        )

# Funções para triggers
def configurar_triggers():
    """
    Configura os triggers a partir do ficheiro triggers.txt
    """
    success, message = setup_triggers()

    if success:
        triggers_status.configure(text=message, text_color="green")
        messagebox.showinfo("Sucesso", message)
        atualizar_lista_triggers()
    else:
        triggers_status.configure(text=message, text_color="red")
        messagebox.showerror("Erro", message)

def configurar_views():
    """
    Configura as views a partir do ficheiro queries.txt
    """
    success, message = setup_views()

    if success:
        triggers_status.configure(text=message, text_color="green")
        messagebox.showinfo("Sucesso", message)
    else:
        triggers_status.configure(text=message, text_color="red")
        messagebox.showerror("Erro", message)

def create_notification_table_wrapper():
    """
    Cria a tabela de notificações
    """
    success, message = create_notification_table()

    if success:
        triggers_status.configure(text=message, text_color="green")
        messagebox.showinfo("Sucesso", message)
    else:
        triggers_status.configure(text=message, text_color="red")
        messagebox.showerror("Erro", message)

def verificar_triggers():
    """
    Verifica se todos os triggers necessários estão configurados
    """
    success, message = check_triggers_exist()

    if success:
        triggers_status.configure(text=message, text_color="green")
        messagebox.showinfo("Verificação", message)
    else:
        triggers_status.configure(text=message, text_color="orange")
        messagebox.showwarning("Aviso", message)

def atualizar_lista_triggers():
    """
    Atualiza a lista de triggers na interface
    """
    triggers = get_all_triggers()

    # Limpar treeview
    for item in triggers_tree.get_children():
        triggers_tree.delete(item)

    # Adicionar triggers à lista
    for trigger in triggers:
        estado = "Ativo" if not trigger['disabled'] else "Desativado"
        created_date = trigger['created'].strftime("%Y-%m-%d %H:%M") if trigger['created'] else ""

        triggers_tree.insert(
            "", "end",
            values=(
                trigger['name'],
                trigger['table'],
                estado,
                created_date
            )
        )

    triggers_status.configure(text=f"Total de triggers: {len(triggers)}", text_color="blue")

def ativar_trigger_selecionado():
    """
    Ativa o trigger selecionado
    """
    selecionado = triggers_tree.selection()

    if not selecionado:
        messagebox.showwarning("Aviso", "Selecione um trigger primeiro")
        return

    trigger_name = triggers_tree.item(selecionado[0])['values'][0]

    success, message = enable_disable_trigger(trigger_name, enable=True)

    if success:
        messagebox.showinfo("Sucesso", message)
        atualizar_lista_triggers()
    else:
        messagebox.showerror("Erro", message)

def desativar_trigger_selecionado():
    """
    Desativa o trigger selecionado
    """
    selecionado = triggers_tree.selection()

    if not selecionado:
        messagebox.showwarning("Aviso", "Selecione um trigger primeiro")
        return

    trigger_name = triggers_tree.item(selecionado[0])['values'][0]

    success, message = enable_disable_trigger(trigger_name, enable=False)

    if success:
        messagebox.showinfo("Sucesso", message)
        atualizar_lista_triggers()
    else:
        messagebox.showerror("Erro", message)

def eliminar_trigger_selecionado():
    """
    Elimina o trigger selecionado
    """
    selecionado = triggers_tree.selection()

    if not selecionado:
        messagebox.showwarning("Aviso", "Selecione um trigger primeiro")
        return

    trigger_name = triggers_tree.item(selecionado[0])['values'][0]

    # Confirmar eliminação
    if not messagebox.askyesno("Confirmar", f"Tem certeza que deseja eliminar o trigger '{trigger_name}'?"):
        return

    success, message = drop_trigger(trigger_name)

    if success:
        messagebox.showinfo("Sucesso", message)
        atualizar_lista_triggers()
    else:
        messagebox.showerror("Erro", message)

# Funções para views
def executar_view_selecionada(view_name):
    if not view_name:
        messagebox.showwarning("Aviso", "Selecione uma view")
        return

    success, result = execute_view(view_name)

    if success:
        columns, records = result

        # Limpar resultados anteriores
        view_tree.delete(*view_tree.get_children())
        view_tree["columns"] = columns

        # Configurar colunas
        for col in columns:
            view_tree.heading(col, text=col)
            view_tree.column(col, width=120, minwidth=50, stretch=True)

        # Adicionar registos
        for record in records:
            str_record = [str(val) if val is not None else "" for val in record]
            view_tree.insert('', 'end', values=str_record)

        # Atualizar estatísticas
        view_stats_label.configure(text=f"Total de registos: {len(records)}")
    else:
        messagebox.showerror("Erro", result)

# Configurar comandos dos botões
btn_ligar.configure(command=ligar_bd)
create_table_btn.configure(command=create_table)
refresh_tables_btn.configure(command=atualizar_lista_tabelas)

# Configurar eventos
table_combo.configure(command=lambda value: load_table_for_crud())
query_refresh_btn.configure(command=atualizar_lista_tabelas)
query_all_btn.configure(command=query_table)
apply_filter_btn.configure(command=query_table)
clear_filter_btn.configure(command=clear_filters)
query_table_combo.configure(command=lambda value: query_table())

# Configurar botões de triggers
setup_triggers_btn.configure(command=configurar_triggers)
setup_views_btn.configure(command=configurar_views)
check_triggers_btn.configure(command=verificar_triggers)
refresh_triggers_btn.configure(command=atualizar_lista_triggers)
enable_trigger_btn.configure(command=ativar_trigger_selecionado)
disable_trigger_btn.configure(command=desativar_trigger_selecionado)
delete_trigger_btn.configure(command=eliminar_trigger_selecionado)

# Inicializar aplicações
add_column_field()
app.mainloop()