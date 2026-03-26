import pandas as pd
import numpy as np

#RUTAS DE ENTRADA
ruta_entrada_Daily_OB_All_Skills = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Winback\Entrada\Daily OB all skills 250901_125450.csv"
ruta_entrada_Worksheet = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Winback\Entrada\Worksheet Details Winback 250901_135534.csv"

#RUTA DE SALIDA
ruta_salida_Daily_OB_All_Skills = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Winback\Salida\Daily OB\Daily_OB_Report.csv"
ruta_salida_Worksheet = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Winback\Salida\Worksheet\Worksheet_Report.csv"

#RUTA DEL ROSTER
roster = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Roster\Primo W Roster.xlsx"

#CREACIÓN DE DATAFRAMES
df_roster = pd.read_excel(roster, sheet_name="Roster")
df_Daily_OB = pd.read_csv(ruta_entrada_Daily_OB_All_Skills)
df_Worksheet = pd.read_csv(ruta_entrada_Worksheet)

### DAILY OB ### COLUMNAS CALCULADAS ###

# OB - No Contact
df_Daily_OB["OB - No Contact"] = np.where(df_Daily_OB["DISPOSITION"] == "OB - No Contact",1,0)

# OB - Not reinstated Live Contact
df_Daily_OB["OB - Not reinstated Live Contact"] = np.where(df_Daily_OB["DISPOSITION"] == "OB - Not reinstated Live Contact",1,0)

# OB - Reinstated Delivery
df_Daily_OB["OB - Reinstated Delivery"] = np.where(df_Daily_OB["DISPOSITION"] == "OB - Reinstated Delivery",1,0)

# OB - Voicemail Left
df_Daily_OB["OB - Voicemail Left"] = np.where(df_Daily_OB["DISPOSITION"] == "OB - Voicemail Left",1,0)

# Delivery
df_Daily_OB["Delivery"] = np.where(df_Daily_OB["DISPOSITION"] == "Delivery",1,0)

# Incorrect Disposition
df_Daily_OB["Incorrect Disposition"] = 1 - df_Daily_OB.loc[:, ["OB - No Contact", "OB - Not reinstated Live Contact", "OB - Reinstated Delivery", "OB - Voicemail Left","Delivery"]].sum(axis=1)

# All reactivation (WS)
#print(df_Worksheet.columns)
df_Worksheet["All reactivation"] = np.where(df_Worksheet["Did customer reactivate?.1"].isna(),(np.where(df_Worksheet["Did customer reactivate?.2"].isna(),df_Worksheet["Did customer reactivate?"],df_Worksheet["Did customer reactivate?.2"])),df_Worksheet["Did customer reactivate?.1"])

# All save offer (WS)
df_Worksheet["All save offer"] = np.where(df_Worksheet["Save Offer Used.2"].isna(), df_Worksheet["Save Offer Used"], df_Worksheet["Save Offer Used.2"])

#All save offer (DO)
dic1 = df_Worksheet.set_index("CALL ID")["All save offer"].to_dict()

df_Daily_OB["All save offer"] = df_Daily_OB["CALL ID"].map(dic1).fillna("")

# Reinstated in Worksheet
dic2 = df_Worksheet.set_index("CALL ID")["All reactivation"].to_dict()

df_Daily_OB["Reinstated In Worksheet"] = df_Daily_OB["CALL ID"].map(dic2).fillna("Not found")

# In Worksheet
df_Daily_OB["In Worksheet"] = np.where(df_Daily_OB["Reinstated In Worksheet"] == "Yes", 1,0)

# LOB
df_Daily_OB["AGENT NAME"] = df_Daily_OB["AGENT NAME"].str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
df_roster["Name Five9"] = df_roster["Name Five9"].str.lower().str.replace(r"\s+", " ", regex=True).str.strip()

dic3 = df_roster.set_index("Name Five9")["Division"].to_dict()
df_Daily_OB["LOB"] = df_Daily_OB["AGENT NAME"].map(dic3).fillna("Check")

df_Daily_OB["AGENT NAME"] = df_Daily_OB["AGENT NAME"].str.title()

# CONVERSIÓN HT, TT, ACW, MT, PT (Modificar en Power BI)

## HANDLE TIME
df_Daily_OB["HANDLE TIME"] = pd.to_timedelta(df_Daily_OB["HANDLE TIME"]).dt.total_seconds()

## TALK TIME
df_Daily_OB["TALK TIME"] = pd.to_timedelta(df_Daily_OB["TALK TIME"]).dt.total_seconds()

## AFTER CALL WORK
df_Daily_OB["AFTER CALL WORK TIME"] = pd.to_timedelta(df_Daily_OB["AFTER CALL WORK TIME"]).dt.total_seconds()

## MANUAL TIME
df_Daily_OB["MANUAL TIME"] = pd.to_timedelta(df_Daily_OB["MANUAL TIME"]).dt.total_seconds()

## PREVIEW TIME
df_Daily_OB["PREVIEW TIME"] = pd.to_timedelta(df_Daily_OB["PREVIEW TIME"]).dt.total_seconds()



### RUTA SALIDA ###
df_Daily_OB.to_csv(ruta_salida_Daily_OB_All_Skills, index=False)
df_Worksheet.to_csv(ruta_salida_Worksheet, index=False)
