import pandas as pd
import numpy as np

# ----- 1. Extract (generación de datos sintéticos) -----
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

# Introducir valores faltantes en temperatura
missing_indices = np.random.choice(df.index, size=20, replace=False)
df.loc[missing_indices, "temp_f"] = np.nan

# Guardar CSV original (opcional)
df.to_csv("synthetic_weather_springfield_2022.csv", index=False)

# ----- 2. Transform -----
df["datetime"] = pd.to_datetime(df["datetime"])
df["date"]     = df["datetime"].dt.date
df["year"]     = df["datetime"].dt.year
df["month"]    = df["datetime"].dt.month
df["day"]      = df["datetime"].dt.day

# Convertir temperatura de Fahrenheit a Celsius
df["temp_c"] = ((df["temp_f"] - 32) * 5 / 9).round(1)

# Eliminar registros con temperatura faltante
df_clean = df.dropna(subset=["temp_f"]).copy()

# ----- 3. Load -----
# Guardar el resultado transformado
df_clean.to_csv("synthetic_weather_transformed.csv", index=False)
print("ETL completado. Archivo guardado: synthetic_weather_transformed.csv")
