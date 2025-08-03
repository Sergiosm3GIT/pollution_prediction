import os
import json
import requests
from openaq import OpenAQ
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY") 
client = OpenAQ(api_key=API_KEY)
response = client.locations.list(coordinates=[-33.447487,-70.673676], radius=25000, limit=1000)
print(response.results)
client.close()