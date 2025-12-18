import pyodbc
import customtkinter
from tkinter import messagebox

customtkinter.set_appearance_mode("system")
customtkinter.set_default_color_theme("dark-blue")

app = customtkinter.CTk()
app.geometry("500x500")
app.title("Aplicação Cliente para SQL Server")

# Função conecta base de dados
def ligar_bd():
    servidor = entry_server.get().strip()
    database = entry_database.get().strip()

    if not servidor:
        messagebox.showerror("Erro", "Por favor, insira o nome do servidor")
        return

    if not database:
        messagebox.showerror("Erro", "Por favor, insira o nome da base de dados")
        return

    try:
        # string de conexão
        porta = entry_port.get().strip()
        usuario = entry_user.get().strip()
        password = entry_password.get()

        if porta:
            servidor_completo = f"{servidor},{porta}"
        else:
            servidor_completo = servidor

        if usuario and password:
            # Autenticação SQL Server
            conn_str = (
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={servidor_completo};'
                f'DATABASE={database};'
                f'UID={usuario};'
                f'PWD={password};'
            )
        else:
            # Autenticação Windows
            conn_str = (
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={servidor_completo};'
                f'DATABASE={database};'
                f'Trusted_Connection=yes;'
            )

        # Tentar conexão
        conn = pyodbc.connect(conn_str, timeout=10)

        # Obter informações
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION, DB_NAME()")
        resultado = cursor.fetchone()

        versao_sql = resultado[0].split('\n')[0]
        nome_bd = resultado[1]

        messagebox.showinfo(
            "Conexão Estabelecida",
            f"Conectado com sucesso!\n\n"
            f"Base de Dados: {nome_bd}\n"
            f"SQL Server: {versao_sql}"
        )

        cursor.close()
        conn.close()

    except pyodbc.Error as e:
        error_msg = str(e)
        if "Login failed" in error_msg:
            messagebox.showerror(
                "Erro de Autenticação"
            )
        elif "Cannot open database" in error_msg:
            messagebox.showerror(
                "Erro de Database",
                f"A base de dados '{database}' não existe.\n"
                "Verifique o nome."
            )
        else:
            messagebox.showerror(
                "Erro de Conexão",
                f"Não foi possível conectar:\n{error_msg}"
            )


# Interface gráfica

frame_principal = customtkinter.CTkFrame(app)
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
    placeholder_text="Ex: _\\SQLEXPRESS",
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
    command=ligar_bd,
    fg_color="blue",
    hover_color="dark blue",
    width=150,
    height=35
)
btn_ligar.pack(pady=20)

# Configuração padrão inicial
entry_server.insert(0, "INESTRIPECA\\SQLEXPRESS")

app.mainloop()