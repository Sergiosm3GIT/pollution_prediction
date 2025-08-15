import os
import json
import requests
from pathlib import Path
from openaq import OpenAQ
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent.parent
dotenv_path = project_root / '.env'
print("Loading environment variables from:", dotenv_path)
load_dotenv(dotenv_path=dotenv_path, override=True)

API_KEY = os.getenv("OPENAQ_API_KEY")
print("API Key loaded:", API_KEY ) 
client = OpenAQ(api_key=API_KEY)
response = client.locations.list(coordinates=[-33.447487,-70.673676], radius=25000, limit=1000)
print(response.results)
client.close()