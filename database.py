import pyodbc
import customtkinter

customtkinter.set_appearance_mode("system")
customtkinter.set_default_color_theme("dark-blue")

app = customtkinter.CTk()
app.geometry("200x300")
app.title("Ateroide")

entry_database = customtkinter.CTkEntry(
    app,
    placeholder_text="Database Name",
    width=140
)
entry_database.place(relx=0.1, rely=0.1)

def create_db():
    try:
        connection = pyodbc.connect(r'DRIVER={ODBC Driver 17 for SQL Server};'
                                    r'SERVER=INESTRIPECA\SQLEXPRESS;'
                                    f'DATABASE={entry_database.get()};'
                                    'Trusted_Connection=yes;')
        info_label.configure(text="Conexão bem sucedida")
    except pyodbc.Error as ex:
        print('Conexão falhada', ex)
        info_label.configure(text='Conexão falhou')

create_button = customtkinter.CTkButton(app, text="Criar",
                                        command=create_db,
                                        fg_color="blue")
create_button.place(relx=0.1, rely=0.2)

def connect_db():
    pass

connect_button = customtkinter.CTkButton(app, text="Conectar",
                                        command=create_db,
                                        fg_color="red")
connect_button.place(relx=0.1, rely=0.3)

info_label = customtkinter.CTkLabel(app, text="")
info_label.place(relx=0.1, rely=0.4)

app.mainloop()




