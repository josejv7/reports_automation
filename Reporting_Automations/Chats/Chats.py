import pandas as pd
import numpy as np
from unidecode import unidecode

#Ruta de acceso
Ruta_Chats = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Chats\Ruta Entrada Chats\report1754015173801.csv"

#Ruta de salida
Ruta_Salida_Chats = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Chats\Ruta Salida Chats\Chats_7_31_25.csv"

#Ruta Roster
roster = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Roster\Primo W Roster.xlsx"

#Creación DataFrame Chats

df_chats = pd.read_csv(Ruta_Chats, encoding="ISO-8859-1")

## CREACIÓN DE COLUMNAS ##

### Agent ID ###
df_roster = pd.read_excel(roster, sheet_name="Roster")

def norm(name):
   if pd.isna(name):
      return ""
   name = str(name)
   name = unidecode(name) #Elimina acentos y caracteres especiales
   name = name.strip() #Elimina espacios en blanco, al principio y final
   name = " ".join(name.split()) #Elimina espacios dobles o multiples entre letras
   return name.lower() #Devuelve todo en minusculas

df_chats["Last Modified By: Full Name"] = df_chats["Last Modified By: Full Name"].apply(norm)
df_roster["Name Five9"] = df_roster["Name Five9"].apply(norm)
df_roster["Name2"] = df_roster["Name2"].apply(norm)
df_chats["User: Full Name"] = df_chats["User: Full Name"].apply(norm)

df_chats["Last Modified By: Full Name"] = df_chats["Last Modified By: Full Name"].replace(
    "juan casta?o", "juan castano"
)

# Opcional: también en User: Full Name si fuera necesario
df_chats["User: Full Name"] = df_chats["User: Full Name"].replace(
    "juan casta?o", "juan castano")


dic1 = df_roster.set_index("Name Five9")["User2"].to_dict()
dic2 = df_roster.set_index("Name2")["User2"].to_dict()

df_chats["Agent ID"] = df_chats["Last Modified By: Full Name"].map(dic1).combine_first(df_chats["Last Modified By: Full Name"].map(dic2)).fillna("Other")

### DATE ###

df_chats["Date"] = pd.to_datetime(df_chats["Close Date"], errors = "coerce")
df_chats["Date"] = df_chats["Date"].dt.date

### SUPERVISOR ###

dic3 = df_roster.set_index("User2")["Direct Report"].to_dict()
df_chats["Supervisor"] = df_chats["Agent ID"].map(dic3).fillna("Other")

### Name ###

dic4 = df_roster.set_index("User2")["Name Five9"].to_dict()
df_chats["Name"] = df_chats["Agent ID"].map(dic4).fillna("Other")
df_chats["Name"] = df_chats["Name"].str.title()

### CHATS HANDLED ###

df_chats["Chats Handled"] = np.where(df_chats["User: Full Name"] == "[None]", 0,1)

### BPO Chats ###

df_chats["BPO Chats"] = np.where((df_chats["Chats Handled"] == 1) & (df_chats["Supervisor"] != "Other") & (df_chats["Supervisor"] != "Abandoned"), 1, 0)

### Chat Abandoned ###

df_chats["Chats Abandoned"] = np.where(df_chats["Chats Handled"] == 1, 0, 1)

### SL > 30seg ###

df_chats["Speed To Answer"] = pd.to_numeric(df_chats["Speed To Answer"])

df_chats["SL > 30seg"] = np.where((df_chats["Speed To Answer"] < 30) & (df_chats["Chats Handled"] == 1), 1, 0)

### Chat Offered ###

df_chats["Chats Offered"] = df_chats["Chats Abandoned"] + df_chats["Chats Handled"]

### Lakeland Chats ###

df_chats["Chats Lakeland"] = np.where((df_chats["Chats Handled"] == 1) & (df_chats["Supervisor"] == "Other"), 1, 0)

### Day ###

df_chats["Day"] = pd.to_datetime(df_chats["Close Date"]).dt.day_name()

### BPO AHT ### (Modificar en PowerBI)

df_chats["BPO AHT"] = np.where(df_chats["BPO Chats"] == 1, df_chats["Handle Time"], "")

### Interval ###

df_chats["Close Date"] = pd.to_datetime(df_chats["Close Date"], errors="coerce")
df_chats["Close Date"] = df_chats["Close Date"].dt.floor("30min")

df_chats["Interval"] = df_chats["Close Date"] + pd.Timedelta(hours=2)
df_chats["Interval"] = df_chats["Interval"].dt.strftime("%H:%M:%S")

 ### Ruta Salida ###
df_chats.to_csv(Ruta_Salida_Chats, index=False, encoding="ISO-8859-1")