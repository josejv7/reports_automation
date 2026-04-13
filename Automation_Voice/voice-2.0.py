"""
ETL Script - BPO Voice Report Automation

Procesa archivos crudos .csv descargados desde la plataforma Five9. Realiza limpieza, transformación y 
generación de un nuevo archivo .csv el cual alimenta un dashboard en Power BI.

Author: Jose Joaquin Velasco
Versión: 2.0

"""
import os
import logging
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# -------------------------------------------------------
# LOGGING
#--------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------
# CONFIGURACIÓN DE RUTAS
#--------------------------------------------------------

load_dotenv()

ruta_LPW = os.getenv("ruta_LPW")
ruta_LPW_OB = os.getenv("ruta_LPW_OB")
ruta_LBT = os.getenv("ruta_LBT")
ruta_LBT_OB = os.getenv("ruta_LBT_OB")
ruta_roster = os.getenv("ruta_roster")
ruta_salida = os.getenv("ruta_salida")

# -------------------------------------------------------
# CLASIFICACIÓN DE SKILLS
# -------------------------------------------------------

SKILL_MAP = {
    "Tier 1":["Tier 1","AQ_ATC","Mountain Valley VIP"],
    "Tier 2":["Tier 2","SCB_EN","Acquisitions","Frontline","PureFlo","CLR_CLN_SANI"],
    "BPO Leadership":["BPO Leadership"],
    "Costco":["Costco Tier 1","Costco Tier 1 Spanish","Costco Service_Billing","Costco Service","Costco Partially Migrated","Costco Retention","Costco Service_Billing_ES","Costco Service_ES"],
    "Costco_OB":["R5 Outreach Costco OB"],
    "LBT EN":["Delivery","Billing","General","CS_Primo Brands","Sams Service","BJs Customer Service","Sams Service_Billing","BJs Customer Service_Billing","ZZ_Repeat Caller","Do not transfer Here Sams Club Feedback"],
    "LBT OB":["R5 Outreach OB"],
    "LBT SP":["Delivery_ES","Billing_ES","General_ES","Equipment_ES","Support_ES","Sams Service_Billing_ES","Sams Service_ES"],
    "Manual OB":["[None]"],
    "Order Confirmation":["CTS - OC Outbound"],
    "Retention":["Retention"],
    "Retention SP":["Retention_ES","Retention Spanish"],
    "Telesales":["Roadshow Support","Telesales","Telesales Spanish","Sales_LPW","Telesales Mountain Valley","Sales","Sales_ES","Sales_LPW_ES","Sales_Overflow","Canada_LPW_TS"],
    "Tier 1 FR":["AQ_ATC_EX_FR","AQ_ATC_FR","AQ_ATC_PRIMO_FR","AQ_ATC_TS_FR"],
    "Tier 2 Spanish":["Tier 1 Spanish","Tier 2 Spanish"],
    "Winback OB":["Winback OB Service","Winback OB Price","Winback OB","Winback Callback","Winback_OB","Winback OB CFLI"],
    "Winback SP":["Spanish OB Outbound Do not Transfer","OB_ES Outbound Do not Transfer"]
}

SKILL_LOOKUP = {skill: label for label, skills in SKILL_MAP.items() for skill in skills}    

# ------------------------------------------------------
# CARGA DEL ROSTER & NORMALIZACIÓN DE STRINGS
# ------------------------------------------------------

logger.info("Cargando roster...")

df_roster = pd.read_excel(ruta_roster, sheet_name="Roster")
df_roster["User2"] = df_roster["User2"].str.lower()
df_roster["Email Bpo"] = df_roster["Email Bpo"].str.lower()
df_roster["Recycled Email LBT"] = df_roster["Recycled Email LBT"].str.lower()
df_roster["Recycled Email LPW"] = df_roster["Recycled Email LPW"].str.lower()

logger.info("Roster cargado correctamente.")

# -------------------------------------------------------
# DICCIONARIOS DEL ROSTER
# ------------------------------------------------------

dic_supervisor_1 = df_roster.set_index("User2")["Direct Report"].to_dict()
dic_supervisor_2 = df_roster.set_index("Email Bpo")["Direct Report"].to_dict()
dic_supervisor_3 = df_roster.set_index("Recycled Email LBT")["Direct Report"].to_dict()
dic_supervisor_4 = df_roster.set_index("Recycled Email LPW")["Direct Report"].to_dict()

dic_username_1 = df_roster.set_index("User2")["Name Five9"].to_dict()
dic_username_2 = df_roster.set_index("Email Bpo")["Name Five9"].to_dict()
dic_username_3 = df_roster.set_index("User2")["Name Five9"].to_dict()
dic_username_4 = df_roster.set_index("User2")["Name Five9"].to_dict()

dic_status_1 = df_roster.set_index("User2")["Status"].to_dict()
dic_status_2 = df_roster.set_index("Email Bpo")["Status"].to_dict()
dic_status_3 = df_roster.set_index("Recycled Email LBT")["Status"].to_dict()
dic_status_4 = df_roster.set_index("Recycled Email LPW")["Status"].to_dict()

dic_lob_1 = df_roster.set_index("User2")["Division"].to_dict()
dic_lob_2 = df_roster.set_index("Email Bpo")["Division"].to_dict()
dic_lob_3 = df_roster.set_index("Recycled Email LBT")["Division"].to_dict()
dic_lob_4 = df_roster.set_index("Recycled Email LPW")["Division"].to_dict()

dic_wave_1 = df_roster.set_index("User2")["Wave"].to_dict()
dic_wave_2 = df_roster.set_index("Email Bpo")["Wave"].to_dict()
dic_wave_3 = df_roster.set_index("Recycled Email LBT")["Wave"].to_dict()
dic_wave_4 = df_roster.set_index("Recycled Email LPW")["Wave"].to_dict()

# ------------------------------------------------------------
# CARGA DEL LPW_CSV
# ------------------------------------------------------------

logger.info("Cargando LPW_CSV...")
df_LPW = pd.read_csv(ruta_LPW)
logger.info(f"LPW_CSV cargado: {len(df_LPW)} filas.")

# ------------------------------------------------------------
# PROCESAMIENTO DE LPW
# ------------------------------------------------------------

#Handle Time
df_LPW["Handle Time2"] = pd.to_timedelta(df_LPW["HANDLE TIME"]).dt.total_seconds().fillna(0)

# After Call Work (ACW)
df_LPW["AFTER CALL WORK TIME"] = pd.to_timedelta(df_LPW["AFTER CALL WORK TIME"]).dt.total_seconds()
df_LPW["ACW"] = np.where(df_LPW["AFTER CALL WORK TIME"] > 0, df_LPW["AFTER CALL WORK TIME"], 0)

# Hold Time en segundos
df_LPW["BPO Hold"] = pd.to_timedelta(df_LPW["HOLD TIME"]).dt.total_seconds().fillna(0)

# Día de la semana
df_LPW["Day"] = pd.to_datetime(df_LPW["DATE"]).dt.day_name()

# Supervisor
df_LPW["Supervisor"] = np.where(
    df_LPW["AGENT"] == "[None]",
    "Abandoned",
    df_LPW["AGENT"].map(dic_supervisor_1)
    .combine_first(df_LPW["AGENT"].map(dic_supervisor_2))
    .combine_first(df_LPW["AGENT"].map(dic_supervisor_3))
    .combine_first(df_LPW["AGENT"].map(dic_supervisor_4))
    .fillna("Other")
)

#Name
df_LPW["Name"] = (
    df_LPW["AGENT"].map(dic_username_1)
    .combine_first(df_LPW["AGENT"].map(dic_username_2))
    .combine_first(df_LPW["AGENT"].map(dic_username_3))
    .combine_first(df_LPW["AGENT"].map(dic_username_4))
    .fillna("Other")
)

# Status
df_LPW["Status"] = (
    df_LPW["AGENT"].map(dic_status_1)
    .combine_first(df_LPW["AGENT"].map(dic_status_2))
    .combine_first(df_LPW["AGENT"].map(dic_status_3))
    .combine_first(df_LPW["AGENT"].map(dic_status_4))
    .fillna("")
)

# LOB
df_LPW["Lob"] = (
    df_LPW["AGENT"].map(dic_lob_1)
    .combine_first(df_LPW["AGENT"].map(dic_lob_2))
    .combine_first(df_LPW["AGENT"].map(dic_lob_3))
    .combine_first(df_LPW["AGENT"].map(dic_lob_4))
    .fillna("")
)

# Wave
df_LPW["Wave"] = (
    df_LPW["AGENT"].map(dic_wave_1)
    .combine_first(df_LPW["AGENT"].map(dic_wave_2))
    .combine_first(df_LPW["AGENT"].map(dic_wave_3))
    .combine_first(df_LPW["AGENT"].map(dic_wave_4))
    .fillna("")
)

# Real Abandoned
df_LPW["Real Abandoned"] = np.where(df_LPW["AGENT"] == "[None]", 1, 0)

# BPO Calls
df_LPW["BPO Calls"] = np.where(
    (df_LPW["Supervisor"] == "Other") | (df_LPW["Supervisor"] == "Abandoned"), 0, 1
)

# Lakeland Calls
df_LPW["Lakeland Calls"] = np.where(df_LPW["Supervisor"] == "Other", 1, 0)

# BPO AHT
df_LPW["BPO AHT"] = np.where(
    (df_LPW["Supervisor"] == "Other") | (df_LPW["Supervisor"] == "Abandoned"), 0, df_LPW["Handle Time2"]
).astype(float)

# BPO Hold
df_LPW["BPO Hold"] = np.where(
    (df_LPW["Supervisor"] == "Other") | (df_LPW["Supervisor"] == "Abandoned"), 0, df_LPW["BPO Hold"]
).astype(float)

# BPO ACW
df_LPW["BPO ACW"] = np.where(
    (df_LPW["Supervisor"] == "Other") | (df_LPW["Supervisor"] == "Abandoned"), 0, df_LPW["AFTER CALL WORK TIME"]
).astype(float)

# ASA en segundos
df_LPW["SPEED OF ANSWER2"]  = pd.to_timedelta(df_LPW["SPEED OF ANSWER"]).dt.total_seconds()
df_LPW["TOTAL QUEUE TIME2"] = pd.to_timedelta(df_LPW["TOTAL QUEUE TIME"]).dt.total_seconds()

# In ASA
df_LPW["In Asa"] = np.where(
    df_LPW["SKILL"] != "SCB_EN",
    np.where(df_LPW["SPEED OF ANSWER2"] <= 30, 1, 0),
    np.where(df_LPW["TOTAL QUEUE TIME2"] <= 30, 1, 0)
)

# ASA BPO
df_LPW["ASA BPO"] = np.where(
    (df_LPW["Supervisor"] == "Other") | (df_LPW["Supervisor"] == "Abandoned"),
    np.nan,
    df_LPW["SPEED OF ANSWER2"]
)

#Skills
df_LPW["Skill h"] = df_LPW["SKILL"].map(SKILL_LOOKUP).fillna("")

# ---------------------------------------------------
# EXPORTAR LPW_CSV 
# -----------------------------------------------------

df_LPW.to_csv(ruta_salida, index=False)
logger.info(f"Archivo exportado: {ruta_salida}")

# ------------------------------------------------------------
# CARGA DEL LBT_CSV
# ------------------------------------------------------------

logger.info("Cargando LBT_CSV...")
df_LBT = pd.read_csv(ruta_LBT)
logger.info(f"LBT_CSV cargado: {len(df_LBT)} filas.")

# ------------------------------------------------------------
# PROCESAMIENTO DE LBT
# ------------------------------------------------------------

#Handle Time
df_LBT["Handle Time2"] = pd.to_timedelta(df_LBT["HANDLE TIME"]).dt.total_seconds().fillna(0)

# After Call Work (ACW)
df_LBT["AFTER CALL WORK TIME"] = pd.to_timedelta(df_LBT["AFTER CALL WORK TIME"]).dt.total_seconds()
df_LBT["ACW"] = np.where(df_LBT["AFTER CALL WORK TIME"] > 0, df_LBT["AFTER CALL WORK TIME"], 0)

# Hold Time en segundos
df_LBT["BPO Hold"] = pd.to_timedelta(df_LBT["HOLD TIME"]).dt.total_seconds().fillna(0)

# Día de la semana
df_LBT["Day"] = pd.to_datetime(df_LBT["DATE"]).dt.day_name()

# Supervisor
df_LBT["Supervisor"] = np.where(
    df_LBT["AGENT"] == "[None]",
    "Abandoned",
    df_LBT["AGENT"].map(dic_supervisor_1)
    .combine_first(df_LBT["AGENT"].map(dic_supervisor_2))
    .combine_first(df_LBT["AGENT"].map(dic_supervisor_3))
    .combine_first(df_LBT["AGENT"].map(dic_supervisor_4))
    .fillna("Other")
)

#Name
df_LBT["Name"] = (
    df_LBT["AGENT"].map(dic_username_1)
    .combine_first(df_LBT["AGENT"].map(dic_username_2))
    .combine_first(df_LBT["AGENT"].map(dic_username_3))
    .combine_first(df_LBT["AGENT"].map(dic_username_4))
    .fillna("Other")
)

# Status
df_LBT["Status"] = (
    df_LBT["AGENT"].map(dic_status_1)
    .combine_first(df_LBT["AGENT"].map(dic_status_2))
    .combine_first(df_LBT["AGENT"].map(dic_status_3))
    .combine_first(df_LBT["AGENT"].map(dic_status_4))
    .fillna("")
)

# LOB
df_LBT["Lob"] = (
    df_LBT["AGENT"].map(dic_lob_1)
    .combine_first(df_LBT["AGENT"].map(dic_lob_2))
    .combine_first(df_LBT["AGENT"].map(dic_lob_3))
    .combine_first(df_LBT["AGENT"].map(dic_lob_4))
    .fillna("")
)

# Wave
df_LBT["Wave"] = (
    df_LBT["AGENT"].map(dic_wave_1)
    .combine_first(df_LBT["AGENT"].map(dic_wave_2))
    .combine_first(df_LBT["AGENT"].map(dic_wave_3))
    .combine_first(df_LBT["AGENT"].map(dic_wave_4))
    .fillna("")
)

# Real Abandoned
df_LBT["Real Abandoned"] = np.where(df_LBT["AGENT"] == "[None]", 1, 0)

# BPO Calls
df_LBT["BPO Calls"] = np.where(
    (df_LBT["Supervisor"] == "Other") | (df_LBT["Supervisor"] == "Abandoned"), 0, 1
)

# Lakeland Calls
df_LBT["Lakeland Calls"] = np.where(df_LBT["Supervisor"] == "Other", 1, 0)

# BPO AHT
df_LBT["BPO AHT"] = np.where(
    (df_LBT["Supervisor"] == "Other") | (df_LBT["Supervisor"] == "Abandoned"), 0, df_LBT["Handle Time2"]
).astype(float)

# BPO Hold
df_LBT["BPO Hold"] = np.where(
    (df_LBT["Supervisor"] == "Other") | (df_LBT["Supervisor"] == "Abandoned"), 0, df_LBT["BPO Hold"]
).astype(float)

# BPO ACW
df_LBT["BPO ACW"] = np.where(
    (df_LBT["Supervisor"] == "Other") | (df_LBT["Supervisor"] == "Abandoned"), 0, df_LBT["AFTER CALL WORK TIME"]
).astype(float)

# ASA en segundos
df_LBT["SPEED OF ANSWER2"]  = pd.to_timedelta(df_LBT["SPEED OF ANSWER"]).dt.total_seconds()
df_LBT["TOTAL QUEUE TIME2"] = pd.to_timedelta(df_LBT["TOTAL QUEUE TIME"]).dt.total_seconds()

# In ASA
df_LBT["In Asa"] = np.where(
    df_LBT["SKILL"] != "SCB_EN",
    np.where(df_LBT["SPEED OF ANSWER2"] <= 30, 1, 0),
    np.where(df_LBT["TOTAL QUEUE TIME2"] <= 30, 1, 0)
)

# ASA BPO
df_LBT["ASA BPO"] = np.where(
    (df_LBT["Supervisor"] == "Other") | (df_LBT["Supervisor"] == "Abandoned"),
    np.nan,
    df_LBT["SPEED OF ANSWER2"]
)

#Skills
df_LBT["Skill h"] = df_LBT["SKILL"].map(SKILL_LOOKUP).fillna("")

# ---------------------------------------------------
# EXPORTAR LPW_CSV 
# -----------------------------------------------------

df_LBT.to_csv(ruta_salida, index=False)
logger.info(f"Archivo exportado: {ruta_salida}")

# ------------------------------------------------------------
# CARGA DEL LPW_OB_CSV
# ------------------------------------------------------------

logger.info("Cargando LPW_OB_CSV...")
df_LPW_OB = pd.read_csv(ruta_LPW_OB)
logger.info(f"LPW_OB_CSV cargado: {len(df_LPW_OB)} filas.")

# ------------------------------------------------------------
# PROCESAMIENTO DE LPW
# ------------------------------------------------------------

#Handle Time
df_LPW_OB["Handle Time2"] = pd.to_timedelta(df_LPW_OB["HANDLE TIME"]).dt.total_seconds().fillna(0)

# After Call Work (ACW)
df_LPW_OB["AFTER CALL WORK TIME"] = pd.to_timedelta(df_LPW_OB["AFTER CALL WORK TIME"]).dt.total_seconds()
df_LPW_OB["ACW"] = np.where(df_LPW_OB["AFTER CALL WORK TIME"] > 0, df_LPW_OB["AFTER CALL WORK TIME"], 0)

# Hold Time en segundos
df_LPW_OB["BPO Hold"] = pd.to_timedelta(df_LPW_OB["HOLD TIME"]).dt.total_seconds().fillna(0)

# Día de la semana
df_LPW_OB["Day"] = pd.to_datetime(df_LPW_OB["DATE"]).dt.day_name()

# Supervisor
df_LPW_OB["Supervisor"] = np.where(
    df_LPW_OB["AGENT"] == "[None]",
    "Abandoned",
    df_LPW_OB["AGENT"].map(dic_supervisor_1)
    .combine_first(df_LPW_OB["AGENT"].map(dic_supervisor_2))
    .combine_first(df_LPW_OB["AGENT"].map(dic_supervisor_3))
    .combine_first(df_LPW_OB["AGENT"].map(dic_supervisor_4))
    .fillna("Other")
)

#Name
df_LPW_OB["Name"] = (
    df_LPW_OB["AGENT"].map(dic_username_1)
    .combine_first(df_LPW_OB["AGENT"].map(dic_username_2))
    .combine_first(df_LPW_OB["AGENT"].map(dic_username_3))
    .combine_first(df_LPW_OB["AGENT"].map(dic_username_4))
    .fillna("Other")
)

# Status
df_LPW_OB["Status"] = (
    df_LPW_OB["AGENT"].map(dic_status_1)
    .combine_first(df_LPW_OB["AGENT"].map(dic_status_2))
    .combine_first(df_LPW_OB["AGENT"].map(dic_status_3))
    .combine_first(df_LPW_OB["AGENT"].map(dic_status_4))
    .fillna("")
)

# LOB
df_LPW_OB["Lob"] = (
    df_LPW_OB["AGENT"].map(dic_lob_1)
    .combine_first(df_LPW_OB["AGENT"].map(dic_lob_2))
    .combine_first(df_LPW_OB["AGENT"].map(dic_lob_3))
    .combine_first(df_LPW_OB["AGENT"].map(dic_lob_4))
    .fillna("")
)

# Wave
df_LPW_OB["Wave"] = (
    df_LPW_OB["AGENT"].map(dic_wave_1)
    .combine_first(df_LPW_OB["AGENT"].map(dic_wave_2))
    .combine_first(df_LPW_OB["AGENT"].map(dic_wave_3))
    .combine_first(df_LPW_OB["AGENT"].map(dic_wave_4))
    .fillna("")
)

# Real Abandoned
df_LPW_OB["Real Abandoned"] = np.where(df_LPW_OB["AGENT"] == "[None]", 1, 0)

# BPO Calls
df_LPW_OB["BPO Calls"] = np.where(
    (df_LPW_OB["Supervisor"] == "Other") | (df_LPW_OB["Supervisor"] == "Abandoned"), 0, 1
)

# Lakeland Calls
df_LPW_OB["Lakeland Calls"] = np.where(df_LPW_OB["Supervisor"] == "Other", 1, 0)

# BPO AHT
df_LPW_OB["BPO AHT"] = np.where(
    (df_LPW_OB["Supervisor"] == "Other") | (df_LPW_OB["Supervisor"] == "Abandoned"), 0, df_LPW_OB["Handle Time2"]
).astype(float)

# BPO Hold
df_LPW_OB["BPO Hold"] = np.where(
    (df_LPW_OB["Supervisor"] == "Other") | (df_LPW_OB["Supervisor"] == "Abandoned"), 0, df_LPW_OB["BPO Hold"]
).astype(float)

# BPO ACW
df_LPW_OB["BPO ACW"] = np.where(
    (df_LPW_OB["Supervisor"] == "Other") | (df_LPW_OB["Supervisor"] == "Abandoned"), 0, df_LPW_OB["AFTER CALL WORK TIME"]
).astype(float)

# ASA en segundos
df_LPW_OB["SPEED OF ANSWER2"]  = pd.to_timedelta(df_LPW_OB["SPEED OF ANSWER"]).dt.total_seconds()
df_LPW_OB["TOTAL QUEUE TIME2"] = pd.to_timedelta(df_LPW_OB["TOTAL QUEUE TIME"]).dt.total_seconds()

# In ASA
df_LPW_OB["In Asa"] = np.where(
    df_LPW_OB["SKILL"] != "SCB_EN",
    np.where(df_LPW_OB["SPEED OF ANSWER2"] <= 30, 1, 0),
    np.where(df_LPW_OB["TOTAL QUEUE TIME2"] <= 30, 1, 0)
)

# ASA BPO
df_LPW_OB["ASA BPO"] = np.where(
    (df_LPW_OB["Supervisor"] == "Other") | (df_LPW_OB["Supervisor"] == "Abandoned"),
    np.nan,
    df_LPW_OB["SPEED OF ANSWER2"]
)

#Skills
df_LPW_OB["Skill h"] = df_LPW_OB["SKILL"].map(SKILL_LOOKUP).fillna("")

# ---------------------------------------------------
# EXPORTAR LPW_OB_CSV 
# -----------------------------------------------------

df_LPW_OB.to_csv(ruta_salida, index=False)
logger.info(f"Archivo exportado: {ruta_salida}")

# ------------------------------------------------------------
# CARGA DEL LBT_OB_CSV
# ------------------------------------------------------------

logger.info("Cargando LBT_OB_CSV...")
df_LBT_OB = pd.read_csv(ruta_LBT_OB)
logger.info(f"LBT_OB_CSV cargado: {len(df_LBT_OB)} filas.")

# ------------------------------------------------------------
# PROCESAMIENTO DE LBT_OB_CSV
# ------------------------------------------------------------

#Handle Time
df_LBT_OB["Handle Time2"] = pd.to_timedelta(df_LBT_OB["HANDLE TIME"]).dt.total_seconds().fillna(0)

# After Call Work (ACW)
df_LBT_OB["AFTER CALL WORK TIME"] = pd.to_timedelta(df_LBT_OB["AFTER CALL WORK TIME"]).dt.total_seconds()
df_LBT_OB["ACW"] = np.where(df_LBT_OB["AFTER CALL WORK TIME"] > 0, df_LBT_OB["AFTER CALL WORK TIME"], 0)

# Hold Time en segundos
df_LBT_OB["BPO Hold"] = pd.to_timedelta(df_LBT_OB["HOLD TIME"]).dt.total_seconds().fillna(0)

# Día de la semana
df_LBT_OB["Day"] = pd.to_datetime(df_LBT_OB["DATE"]).dt.day_name()

# Supervisor
df_LBT_OB["Supervisor"] = np.where(
    df_LBT_OB["AGENT"] == "[None]",
    "Abandoned",
    df_LBT_OB["AGENT"].map(dic_supervisor_1)
    .combine_first(df_LBT_OB["AGENT"].map(dic_supervisor_2))
    .combine_first(df_LBT_OB["AGENT"].map(dic_supervisor_3))
    .combine_first(df_LBT_OB["AGENT"].map(dic_supervisor_4))
    .fillna("Other")
)

#Name
df_LBT_OB["Name"] = (
    df_LBT_OB["AGENT"].map(dic_username_1)
    .combine_first(df_LBT_OB["AGENT"].map(dic_username_2))
    .combine_first(df_LBT_OB["AGENT"].map(dic_username_3))
    .combine_first(df_LBT_OB["AGENT"].map(dic_username_4))
    .fillna("Other")
)

# Status
df_LBT_OB["Status"] = (
    df_LBT_OB["AGENT"].map(dic_status_1)
    .combine_first(df_LBT_OB["AGENT"].map(dic_status_2))
    .combine_first(df_LBT_OB["AGENT"].map(dic_status_3))
    .combine_first(df_LBT_OB["AGENT"].map(dic_status_4))
    .fillna("")
)

# LOB
df_LBT_OB["Lob"] = (
    df_LBT_OB["AGENT"].map(dic_lob_1)
    .combine_first(df_LBT_OB["AGENT"].map(dic_lob_2))
    .combine_first(df_LBT_OB["AGENT"].map(dic_lob_3))
    .combine_first(df_LBT_OB["AGENT"].map(dic_lob_4))
    .fillna("")
)

# Wave
df_LBT_OB["Wave"] = (
    df_LBT_OB["AGENT"].map(dic_wave_1)
    .combine_first(df_LBT_OB["AGENT"].map(dic_wave_2))
    .combine_first(df_LBT_OB["AGENT"].map(dic_wave_3))
    .combine_first(df_LBT_OB["AGENT"].map(dic_wave_4))
    .fillna("")
)

# Real Abandoned
df_LBT_OB["Real Abandoned"] = np.where(df_LBT_OB["AGENT"] == "[None]", 1, 0)

# BPO Calls
df_LBT_OB["BPO Calls"] = np.where(
    (df_LBT_OB["Supervisor"] == "Other") | (df_LBT_OB["Supervisor"] == "Abandoned"), 0, 1
)

# Lakeland Calls
df_LBT_OB["Lakeland Calls"] = np.where(df_LBT_OB["Supervisor"] == "Other", 1, 0)

# BPO AHT
df_LBT_OB["BPO AHT"] = np.where(
    (df_LBT_OB["Supervisor"] == "Other") | (df_LBT_OB["Supervisor"] == "Abandoned"), 0, df_LBT_OB["Handle Time2"]
).astype(float)

# BPO Hold
df_LBT_OB["BPO Hold"] = np.where(
    (df_LBT_OB["Supervisor"] == "Other") | (df_LBT_OB["Supervisor"] == "Abandoned"), 0, df_LBT_OB["BPO Hold"]
).astype(float)

# BPO ACW
df_LBT_OB["BPO ACW"] = np.where(
    (df_LBT_OB["Supervisor"] == "Other") | (df_LBT_OB["Supervisor"] == "Abandoned"), 0, df_LBT_OB["AFTER CALL WORK TIME"]
).astype(float)

# ASA en segundos
df_LBT_OB["SPEED OF ANSWER2"]  = pd.to_timedelta(df_LBT_OB["MANUAL TIME"]).dt.total_seconds()
df_LBT_OB["TOTAL QUEUE TIME2"] = pd.to_timedelta(df_LBT_OB["RINGING TIME"]).dt.total_seconds()

# In ASA
df_LBT_OB["In Asa"] = np.where(
    df_LBT_OB["SKILL"] != "SCB_EN",
    np.where(df_LBT_OB["SPEED OF ANSWER2"] <= 30, 1, 0),
    np.where(df_LBT_OB["TOTAL QUEUE TIME2"] <= 30, 1, 0)
)

# ASA BPO
df_LBT_OB["ASA BPO"] = np.where(
    (df_LBT_OB["Supervisor"] == "Other") | (df_LBT_OB["Supervisor"] == "Abandoned"),
    np.nan,
    df_LBT_OB["SPEED OF ANSWER2"]
)

#Skills
df_LBT_OB["Skill h"] = df_LBT_OB["SKILL"].map(SKILL_LOOKUP).fillna("")

# ---------------------------------------------------
# EXPORTAR LBT_OB_CSV 
# -----------------------------------------------------

df_LBT_OB.to_csv(ruta_salida, index=False)
logger.info(f"Archivo exportado: {ruta_salida}")

# ----------------------------------------------------
# UNIÓN FINAL
# ----------------------------------------------------

logger.info("Uniendo los archivos...")

#Alineando las columnas
df_LBT.columns = df_LPW.columns
df_LPW_OB.columns = df_LPW.columns
df_LBT_OB.columns = df_LPW.columns

#Unión de DataFrames
df_final = pd.concat([df_LPW,df_LBT,df_LPW_OB,df_LBT_OB])

logger.info(f"Total filas combinadas: {len(df_final)}")

# -----------------------------------------------------------
# EXPORTACION FINAL
# ----------------------------------------------------------
df_final.to_csv(ruta_salida, index=False)
logger.info(f"Archivo exportado exitosamente: {ruta_salida}")
