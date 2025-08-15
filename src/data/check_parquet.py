import os
import pandas as pd
from pathlib import Path  
from datetime import datetime

os.chdir(Path(__file__).resolve().parent.parent.parent.parent)

df = pd.read_parquet("pollution-prediction/data/raw/sensors_measurements_2025_01_08.parquet")
print(f"Data shape: {df.shape}")
df.to_csv('pollution-prediction/data/raw/sensors_measurements_2025_01_08.csv',index=False)