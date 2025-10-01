from google.ai import generativelanguage_v1beta as genai
from google.oauth2 import service_account

# Path to your service account JSON
key_path = r"C:\Users\Praha\OneDrive\Documents\Python\formula1_project\service_account.json"
credentials = service_account.Credentials.from_service_account_file(key_path)

# Create a client with explicit credentials
client = genai.ModelServiceClient(credentials=credentials)

# List models
models = client.list_models()
for m in models:
    print(m.name)
