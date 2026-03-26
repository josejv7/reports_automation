#M. Load Data
import pandas as pd

def load_csv(path, encoding="ISO-8859-1"):
    return pd.read_csv(path, encoding=encoding)

def load_excel(path, sheet = "Roster"):
    return pd.read_excel(path, sheet_name = sheet)

#M. Cleaning Data
from unidecode import unidecode
import pandas as pd

def normalize_text(text):
    if pd.isna(text):
        return ""
    text = unidecode(str(text)).strip()
    return " ".join(text.split()).lower()

def delete_commas(df, cols):
    for col in cols:
        df[col] = df[col].str.replace(",", "")
    return df

def replace_ñ(df, col, old = "?", new = "n"):
    df[col] = df[col].str.replace(old, new)
    return df

#M. Transformations

import pandas as pd
import numpy as np

def 

