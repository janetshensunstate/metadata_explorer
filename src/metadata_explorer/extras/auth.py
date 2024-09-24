import tableauserverclient as TSC
from dotenv import load_dotenv
import os

# Loads values from .env file.  See .env.example for example
load_dotenv()
token_name = os.getenv("PERSONAL_ACCESS_TOKEN_NAME")
token_value = os.getenv("PERSONAL_ACCESS_TOKEN_SECRET")
site_id = os.getenv("SITE_ID")

# Information needed to create Tableau Server session
tableau_auth = TSC.PersonalAccessTokenAuth( token_name, token_value, site_id)
server = TSC.Server('https://10az.online.tableau.com/', use_server_version=True)