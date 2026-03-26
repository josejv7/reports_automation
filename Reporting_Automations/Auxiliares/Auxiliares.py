import pandas as pd
import numpy as np

#Rutas de acceso

RutaE_LPW = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Auxiliares\RAW_LPW\Agent Login-Logout2 250819_143538.csv"
RutaE_LBT = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Auxiliares\RAW_LBT\LoginLogout 250819_143610.csv"
roster = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Roster\Primo W Roster.xlsx"

#Ruta de salida
RutaS = r"C:\Users\josej\OneDrive\Escritorio\Reportes BPO\Auxiliares\RUTA_FINAL\Auxiliares.csv"

#Creación de DF's
df_LPW = pd.read_csv(RutaE_LPW)
df_LBT = pd.read_csv(RutaE_LBT)
df_roster = pd.read_excel(roster, sheet_name="Roster")

#Union de DF's
df_aux = pd.concat([df_LPW, df_LBT], ignore_index= True)

#CREACIÓN DE COLUMNAS CALCULADAS

#DATE2
df_aux["DATE2"] = pd.to_datetime(df_aux["DATE"])

#CC
df_roster["User2"] = df_roster["User2"].str.lower()
df_roster["Email BpoLabs"] = df_roster["Email BpoLabs"].str.lower()

dic1 = df_roster.set_index("User2")["CC"].to_dict()
dic2 = df_roster.set_index("Email BpoLabs")["CC"].to_dict()

df_aux["CC"] = (df_aux["AGENT"].map(dic1).combine_first(df_aux["AGENT"].map(dic2)).fillna(""))

#END TIME
df_aux["End Time"] = pd.to_datetime(df_aux["END TIME MILLISECOND"])
df_aux["End Time"] = df_aux["End Time"].dt.strftime("%H:%M:%S")

# LUNCH LPW
df_aux["AGENT STATE TIME"] = pd.to_timedelta(df_aux["AGENT STATE TIME"])

mask = df_aux["REASON CODE"].isin(["Lunch Out", "Lunch In"])

suma_lunch = (df_aux.loc[mask].groupby(["DATE", "AGENT"], as_index= False)["AGENT STATE TIME"].sum().rename(columns = {"AGENT STATE TIME" : "Lunch LPW"}))

df_aux = df_aux.merge(suma_lunch, on=["DATE", "AGENT"], how="left")

df_aux["Lunch LPW"] = df_aux["Lunch LPW"].fillna(pd.Timedelta(0))

df_aux['Lunch LPW'] = df_aux['Lunch LPW'].astype(str).str.split().str[-1]

df_aux['AGENT STATE TIME'] = df_aux['AGENT STATE TIME'].astype(str).str.split().str[-1]

# LUNCH LBT
df_aux["Lunch LBT"] = np.where(df_aux["REASON CODE"] == "Lunch", df_aux["AGENT STATE TIME"], "")

# BPO PT
df_aux["BPO PT"] = np.where(df_aux["REASON CODE"] == "BPO PT", df_aux["AGENT STATE TIME"], "0:00:00")

# READY
df_aux["Ready"] = np.where(df_aux["STATE"] == "Ready", df_aux["AGENT STATE TIME"], "0:00:00")

# AFTERCALL WORK
df_aux["Aftercall"] = np.where(df_aux["STATE"] == "After Call Work", df_aux["AGENT STATE TIME"], "0:00:00")

# ON CALL
df_aux["On Call"] = np.where(df_aux["STATE"] == "On Call", df_aux["AGENT STATE TIME"], "0:00:00")

# COACHING
df_aux["Coaching"] = np.where(df_aux["REASON CODE"] == "Coaching", df_aux["AGENT STATE TIME"], "0:00:00")

# BREAK
df_aux["Break"] = np.where(df_aux["REASON CODE"] == "Break", df_aux["AGENT STATE TIME"], "0:00:00")

# COMPUTER ISSUES
df_aux["Computer Issues"] = np.where(df_aux["REASON CODE"] == "Computer Issues", df_aux["AGENT STATE TIME"], "0:00:00")

#TRAINING
df_aux["Training"] = np.where(df_aux["REASON CODE"] == "Training", df_aux["AGENT STATE TIME"], "0:00:00")

# MEETING
df_aux["Meeting"] = np.where(df_aux["REASON CODE"] == "Meeting", df_aux["AGENT STATE TIME"], "0:00:00")

# PERSONAL
df_aux["Personal"] = np.where(df_aux["REASON CODE"] == "Personal", df_aux["AGENT STATE TIME"], "0:00:00")

# FLOOR SUPPORT
df_aux["Floor Support"] = np.where(df_aux["REASON CODE"] == "Floor Support", df_aux["AGENT STATE TIME"], "0:00:00")

# NAME
df_roster["User2"] = df_roster["User2"].str.lower()
df_roster["Email BpoLabs"] = df_roster["Email BpoLabs"].str.lower()

dic3 = df_roster.set_index("User2")["Name Five9"].to_dict()
dic4 = df_roster.set_index("Email BpoLabs")["Name Five9"].to_dict()

df_aux["Name"] = (df_aux["AGENT"].map(dic3).combine_first(df_aux["AGENT"].map(dic4)).fillna(""))

# SUPERVISOR
dic5 = df_roster.set_index("CC")["Direct Report"].to_dict()

df_aux["Supervisor"] = (df_aux["CC"].map(dic5).fillna(""))

# DIVISION
dic6 = df_roster.set_index("CC")["Division"].to_dict()

df_aux["Division"] = (df_aux["CC"].map(dic6).fillna(""))

# STATUS
dic7 = df_roster.set_index("CC")["Status"].to_dict()

df_aux["Status"] = (df_aux["CC"].map(dic7).fillna(""))

# HOOPS
df_aux["TIME"] = pd.to_datetime(df_aux["TIME"], errors="coerce")
#df_aux["HOOPs2"] = np.where(df_aux["TIME"].between("6:50:00", "20:00:00", inclusive="both"), "Y", "N")
df_aux["HOOPs"] = np.where((df_aux["TIME"] < "6:50:00"), "N",(np.where(df_aux["TIME"] > "20:00:00", "N", "Y")))





#RUTA FINAL
df_aux.to_csv(RutaS, index = False)
