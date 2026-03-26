import pandas as pd
import numpy as np
from unidecode import unidecode

#Rutas de Acceso
ruta_entrada_LPW = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Emails\RAW_LPW\report1753999594982.csv"
ruta_entrada_LBT = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Emails\RAW_LBT\report1753999656003.csv"

#Ruta de Salida
ruta_salida_combine = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Emails\Ruta_Final\LPW_LBT_Emails_7_31.csv"

#Ruta del Roster
roster = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Roster\Primo W Roster.xlsx"

#Creación DataFrame .csv Emails
df_emails = pd.read_csv(ruta_entrada_LPW, encoding="ISO-8859-1")
#df_emails.head()

#Eliminación de las comas en las columnas F, G y H.

delete_commas = ["Created Date", "Accept Date", "Close Date"]

for col in delete_commas:
  df_emails[col] = df_emails[col].str.replace(",", "")
#df_emails.sample()

#Ordenar la columna "Case Number(menor a mayor)" y "User: Full Name(A-Z)"

df_emails = df_emails.sort_values(by=["User: Full Name","Case Number"], ascending=[True, True])

###########################################################################################################################################################################

##COLUMNAS FORMULADAS

#### DATE ####

df_emails["Close Date"] = pd.to_datetime(df_emails["Close Date"], errors="coerce")
df_emails["Date"] = df_emails["Close Date"].dt.date

#### AGENT ID ####

df_roster = pd.read_excel(roster, sheet_name='Roster')

def norm(name):
   if pd.isna(name):
      return ""
   name = str(name)
   name = unidecode(name) #Elimina acentos y caracteres especiales
   name = name.strip() #Elimina espacios en blanco, al principio y final
   name = " ".join(name.split()) #Elimina espacios dobles o multiples entre letras
   return name.lower() #Devuelve todo en minusculas

#Normalizando los datos

df_emails["Last Modified By: Full Name"] = df_emails["Last Modified By: Full Name"].apply(norm)
df_roster["Name Five9"] = df_roster["Name Five9"].apply(norm)
df_roster["Name2"] = df_roster["Name2"].apply(norm)
df_emails["User: Full Name"] = df_emails["User: Full Name"].apply(norm)

#print(df_emails.columns.tolist())
df_emails["Last Modified By: Full Name"] = df_emails["Last Modified By: Full Name"].replace(
    "juan casta?o", "juan castano"
)

# Opcional: también en User: Full Name si fuera necesario
df_emails["User: Full Name"] = df_emails["User: Full Name"].replace(
    "juan casta?o", "juan castano")



#Construcción diccionarios de busqueda
dic1 = df_roster.set_index("Name Five9")["User2"].to_dict()
dic2 = df_roster.set_index("Name2")["User2"].to_dict()

df_emails["Agent ID"] = (df_emails["Last Modified By: Full Name"].map(dic1).combine_first(df_emails["Last Modified By: Full Name"].map(dic2)).fillna("Other"))


#### AHT ####
def sec_to_hms(sec):
   if pd.isna(sec):
      return ""
   sec = int(sec)
   h, m = divmod(sec, 3600)
   m, s = divmod(m, 60)
   return f"{h:02}:{m:02}:{s:02}"


df_emails["AHT"] = df_emails["Handle Time"].apply(sec_to_hms)
df_emails["AHT"] = df_emails["AHT"].replace(r'^\s*$', "0:00:00", regex=True)

#### Supervisor ####

dic3 = df_roster.set_index("User2")["Direct Report"].to_dict()

df_emails["Supervisor"] = df_emails["Agent ID"].map(dic3).fillna("Other")

#### Name ####

dic4 = df_roster.set_index("User2")["Name Five9"].to_dict()

df_emails["Name"] = df_emails["Agent ID"].map(dic4).fillna("Other")
df_emails["Name"] = df_emails["Name"].str.title()


#### Validation ####

df_emails["Validation"] = (df_emails["User: Full Name"].str.lower() == df_emails["Last Modified By: Full Name"].str.lower()).astype(int) #

#### ASA ####

#Conversión de columnas a datetime
for x in ["Accept Date", "Created Date", "Close Date"]:
   df_emails[x] = pd.to_datetime(df_emails[x], errors="coerce")

#Diferencia en segundos
delta_accept = (df_emails["Accept Date"] - df_emails["Created Date"]).dt.total_seconds()
delta_close = (df_emails["Close Date"] - df_emails["Created Date"]).dt.total_seconds()

df_emails["ASA(Seg)"] = np.where(delta_accept < 0, delta_close, delta_accept)

df_emails["ASA"] = df_emails["ASA(Seg)"].apply(sec_to_hms)


#9. Email Handled 

#Condiciones vectorizadas
df_emails["User: Full Name"] = df_emails["User: Full Name"].str.lower()

df_emails["Prev_Case"] = df_emails["Case Number"].shift(1)

df_emails["Email Handled"] = np.where(df_emails["Validation"] == 1, np.where((df_emails["Case Number"] == df_emails["Prev_Case"]) & (df_emails["AHT"] == "0:00:00"),0,1),0)

#10. LOB

#Forma vectorizada

df_emails["LOB"] = np.where(df_emails["Case Origin"].isin(["Customer Service Inbox", "Email"]), "LPW", "LBT")

#11. Interval

df_emails["Close Date"] = pd.to_datetime(df_emails["Close Date"], errors="coerce")
floor_30 = df_emails["Close Date"].dt.floor("30min")

df_emails["Interval"] = np.where(df_emails["LOB"] == "LPW", floor_30 - pd.Timedelta(hours=1), floor_30 + pd.Timedelta(hours=2))
df_emails["Interval"] = df_emails["Interval"].dt.strftime("%H:%M:%S")

#12. BPO Emails

df_emails["BPO Emails"] = np.where((df_emails["Email Handled"] == 1) & (df_emails["Supervisor"] != "Other"), 1,0)


##BPO AHT(revisar si eliminar los datos vacios o dejarlos como 0)
df_emails["BPO AHT"] = np.where(df_emails["BPO Emails"]==1, df_emails["AHT"],0)

df_emails["BPO AHT(sec)"] = np.where(df_emails["BPO Emails"]==1, df_emails["Handle Time"],0)



#####################################################################   LBT  #############################################################################


#Leyendo el archivo .CSV de emails.
df_emails_LBT = pd.read_csv(ruta_entrada_LBT, encoding="ISO-8859-1")
df_emails_LBT.head()

#Ordenar la columna "Case Number(menor a mayor)" y "User: Full Name(A-Z)"

df_emails_LBT = df_emails_LBT.sort_values(by=["User: Full Name","Case Number: Case Number"], ascending=[True, True])

#Reemplazaar 'ñ' por 'n'
replace_ñ = ["User: Full Name"]

for c in replace_ñ:
    df_emails_LBT[c] = df_emails_LBT[c].str.replace("ñ", "n")
#print(df_emails_LBT.sample())


#### DATE ####

df_emails_LBT["Close Date"] = pd.to_datetime(df_emails_LBT["Close Date"], errors="coerce")
df_emails_LBT["Date"] = df_emails_LBT["Close Date"].dt.date

#### AGENT ID ####

#Normalizando los datos

df_emails_LBT["Last Modified By: Full Name"] = df_emails_LBT["Last Modified By: Full Name"].apply(norm)
df_roster["Name Five9"] = df_roster["Name Five9"].apply(norm)
df_roster["Name2"] = df_roster["Name2"].apply(norm)
df_emails_LBT["User: Full Name"] = df_emails_LBT["User: Full Name"].apply(norm)

df_emails_LBT["Last Modified By: Full Name"] = df_emails_LBT["Last Modified By: Full Name"].str.replace("?", "n")
df_emails_LBT["User: Full Name"] = df_emails_LBT["User: Full Name"].str.replace("?", "n")

#Construcción diccionarios de busqueda
dic5 = df_roster.set_index("Name Five9")["User2"].to_dict()
dic6 = df_roster.set_index("Name2")["User2"].to_dict()

df_emails_LBT["Agent ID"] = (df_emails_LBT["Last Modified By: Full Name"].map(dic5).combine_first(df_emails_LBT["Last Modified By: Full Name"].map(dic6)).fillna("Other"))


#### AHT ####

df_emails_LBT["AHT"] = df_emails_LBT["Handle Time"].apply(sec_to_hms)
df_emails_LBT["AHT"] = df_emails_LBT["AHT"].replace(r'^\s*$', "0:00:00", regex=True)


#### Supervisor ####

dic7 = df_roster.set_index("User2")["Direct Report"].to_dict()

df_emails_LBT["Supervisor"] = df_emails_LBT["Agent ID"].map(dic7).fillna("Other")

#### Name ####

dic8 = df_roster.set_index("User2")["Name Five9"].to_dict()

df_emails_LBT["Name"] = df_emails_LBT["Agent ID"].map(dic8).fillna("Other")
df_emails_LBT["Name"] = df_emails_LBT["Name"].str.title()

#### Validation ####

df_emails_LBT["Validation"] = (df_emails_LBT["User: Full Name"].str.lower() == df_emails_LBT["Last Modified By: Full Name"].str.lower()).astype(int) #

#### ASA ####

#Conversión de columnas a datetime
for y in ["Accept Date", "Request Date", "Close Date"]:
   df_emails_LBT[y] = pd.to_datetime(df_emails_LBT[y], errors="coerce")

#Diferencia en segundos
delta_accept_LBT = (df_emails_LBT["Accept Date"] - df_emails_LBT["Request Date"]).dt.total_seconds()
delta_close_LBT = (df_emails_LBT["Close Date"] - df_emails_LBT["Request Date"]).dt.total_seconds()

df_emails_LBT["ASA(Seg)"] = np.where(delta_accept_LBT < 0, delta_close_LBT, delta_accept_LBT)

df_emails_LBT["ASA"] = df_emails_LBT["ASA(Seg)"].apply(sec_to_hms)

#### EMAIL HANDLED ####

df_emails_LBT["User: Full Name"] = df_emails_LBT["User: Full Name"].str.lower()

df_emails_LBT["Case Number: Case Number"] = df_emails_LBT["Case Number: Case Number"].fillna(0)

df_emails_LBT["Prev_Case"] = df_emails_LBT["Case Number: Case Number"].shift(1)

df_emails_LBT["Email Handled"] = np.where(df_emails_LBT["Validation"] == 1, np.where((df_emails_LBT["Case Number: Case Number"] == df_emails_LBT["Prev_Case"]) & (df_emails_LBT["AHT"] == "0:00:00"),0,1),0)

#### LOB ####

df_emails_LBT["LOB"] = np.where(df_emails_LBT["Queue: Name"].isin(["Customer Service Inbox", "Email"]), "LPW", "LBT")

#### Interval ####

df_emails_LBT["Close Date"] = pd.to_datetime(df_emails_LBT["Close Date"], errors="coerce")
floor_30 = df_emails_LBT["Close Date"].dt.floor("30T")

df_emails_LBT["Interval"] = np.where(df_emails_LBT["LOB"] == "LPW", floor_30 - pd.Timedelta(hours=1), floor_30 + pd.Timedelta(hours=2))
df_emails_LBT["Interval"] = df_emails_LBT["Interval"].dt.strftime("%H:%M:%S")


#### BPO Emails ####

df_emails_LBT["BPO Emails"] = np.where((df_emails_LBT["Email Handled"] == 1) & (df_emails_LBT["Supervisor"] != "Other"), 1,0)


##BPO AHT

df_emails_LBT["BPO AHT"] = np.where(df_emails_LBT["BPO Emails"]==1, df_emails_LBT["AHT"],"")

df_emails_LBT["BPO AHT(sec)"] = np.where(df_emails_LBT["BPO Emails"]==1, df_emails_LBT["Handle Time"],"")


## Renombrar columnas de LBT como LPW para no tener errores.
df_emails_LBT.columns = df_emails.columns


#Unión de los dos dataframes.
df_combine = pd.concat([df_emails, df_emails_LBT], ignore_index=True)


#### LPW - LBT EMAILS - AHT

df_combine.loc[df_combine["Handle Time"] > 2999, "Handle Time"] = 600

df_combine["BPO AHT"] = np.where(df_combine["BPO Emails"]==1, df_combine["AHT"],"")
df_combine["BPO AHT(sec)"] = np.where(df_combine["BPO Emails"]==1, df_combine["Handle Time"],"")

df_combine["LPW_emails"] = np.where((df_combine["BPO Emails"] == 1) & (df_combine["LOB"] == "LPW"),1,0)
df_combine["LBT_emails"] = np.where((df_combine["BPO Emails"] == 1) & (df_combine["LOB"] == "LBT"),1,0)

df_combine["BPO_LPW_AHT_Sec"] = np.where(df_combine["LPW_emails"]==1, df_combine["Handle Time"],"")
df_combine["BPO_LBT_AHT_Sec"] = np.where(df_combine["LBT_emails"]==1, df_combine["Handle Time"],"")

df_combine["Email_Handled_LPW"] = np.where((df_combine["Email Handled"] == 1) & (df_combine["LOB"] == "LPW"),1,0)
df_combine["Email_Handled_LBT"] = np.where((df_combine["Email Handled"] == 1) & (df_combine["LOB"] == "LBT"),1,0)

####### RUTA FINAL
df_combine.to_csv(ruta_salida_combine, index=False, encoding="ISO-8859-1")