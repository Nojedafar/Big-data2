import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# ----- 0. Configuración MongoDB Atlas -----
load_dotenv()  # Lee variables desde .env si existe
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://asdarot333:<db_password>@cluster0.qzkfxb8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DB_NAME     = "weather_demo"
COLLECTION  = "springfield_2022"

# ----- 1. Extract (datos sintéticos) -----
np.random.seed(42)
dates = pd.date_range(start="2022-01-01", end="2022-12-31", freq="D")

data = {
    "city": ["Springfield"] * len(dates),
    "datetime": dates,
    "temp_f": np.random.normal(loc=65, scale=15, size=len(dates)).round(1),
    "humidity": np.random.uniform(low=30, high=90, size=len(dates)).round(1),
    "precip_in": np.random.choice([0.0, 0.1, 0.3, 0.5, 1.0], size=len(dates), p=[0.6, 0.2, 0.1, 0.05, 0.05])
}
df = pd.DataFrame(data)
df.loc[np.random.choice(df.index, size=20, replace=False), "temp_f"] = np.nan

# ----- 2. Transform -----
df["datetime"] = pd.to_datetime(df["datetime"])
df["date"]     = df["datetime"].dt.date
df["year"]     = df["datetime"].dt.year
df["month"]    = df["datetime"].dt.month
df["day"]      = df["datetime"].dt.day
df["temp_c"]   = ((df["temp_f"] - 32) * 5 / 9).round(1)
df_clean = df.dropna(subset=["temp_f"]).copy()

# Reemplazar caracteres no válidos para Mongo
df_clean.columns = [col.replace('.', '_') for col in df_clean.columns]

# ----- 3. Load a MongoDB Atlas -----
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

# Borrar colección si ya existe (opcional)
collection.delete_many({})

# Insertar registros
records = df_clean.to_dict("records")
result = collection.insert_many(records)
print(f"Se insertaron {len(result.inserted_ids)} documentos en MongoDB Atlas.")

client.close()

