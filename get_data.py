import requests
import pandas as pd
import config
from datetime import datetime
import json
import os
import os.path
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def get_activities():
    print("########### GET PORTEFEUILLE START ###########")

    # List of Pipedrive API tokens
    api_tokens = [
        config.api_pipedrive,
        config.api_pipedrive_thibault,
        config.api_pipedrive_jerem,
        config.api_pipedrive_jordan,
        config.api_pipedrive_julien
    ]

    all_data = []  # To store activities from all API tokens

    for api_token in api_tokens:
        print(f"Fetching data for API token: {api_token}")

        # Define the base API URL for activities with the filter_id
        base_url = f"https://api.pipedrive.com/v1/activities?filter_id=1573&api_token={api_token}"

        start = 0
        limit = 400  # Set a reasonable limit for pagination

        while True:
            # Adjust URL to handle pagination by using 'start' and 'limit' parameters
            url = f"{base_url}&start={start}&limit={limit}"

            payload = {}
            headers = {
                'Accept': 'application/json',
                'Cookie': '__cf_bm=iHJQT34iNDqL_9aEoTv6DUIb4h5R5tRU7NUUFYPEnrY-1729235478-1.0.1.1-.uAb42hCTClrFiw_CZQuiqzvMmtMn8b1Vl0LvUWlv4fwQR1tAsUuxnKadKOAGaGGALlCOyL6SrUQpegQqJgLgw'
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            # Parse the response text as JSON
            response_json = json.loads(response.text)

            # Check if 'success' is True and 'data' is present
            if response_json.get("success") and "data" in response_json:
                data = response_json["data"]
                # Add this page's data to the all_data list
                all_data.extend(data)

                # Check if more data is available
                if len(data) < limit:
                    # If fewer items are returned than the limit, we've reached the end
                    break
                else:
                    # Otherwise, move to the next page
                    start += limit
            else:
                print(f"Failed to retrieve data for API token: {api_token}")
                break

    # Define the list of columns to keep
    columns_to_keep = [
        'done', 'type_name', 'subject', 'due_date', 'add_time',
        'marked_as_done_time', 'org_id', 'org_name', 'person_id',
        'person_name', 'deal_title', 'owner_name', 'assigned_to_user_id',
        'created_by_user_id'
    ]

    # Convert data to a DataFrame
    if all_data:
        df = pd.DataFrame(all_data)

        # Keep only the specified columns
        df_filtered = df[columns_to_keep]

        # Save the filtered data to a CSV file
        df_filtered.to_csv("csv/pipedrive/activities_1573.csv", index=False)
        print("Filtered data saved to 'pipe_activities_filtered_1573.csv'")
    else:
        print("No data to save.")

def get_deals():
    print("########### GET PORTEFEUILLE START ###########")

    # List of Pipedrive API tokens
    api_tokens = [
        config.api_pipedrive,
        config.api_pipedrive_thibault,
        config.api_pipedrive_jerem,
        config.api_pipedrive_jordan,
        config.api_pipedrive_julien
    ]

    all_data = []  # To store activities from all API tokens

    for api_token in api_tokens:
        print(f"Fetching data for API token: {api_token}")

        # Define the base API URL for activities with the filter_id
        base_url = f"https://api.pipedrive.com/v1/deals?filter_id=1574&api_token={api_token}"

        start = 0
        limit = 400  # Set a reasonable limit for pagination

        while True:
            # Adjust URL to handle pagination by using 'start' and 'limit' parameters
            url = f"{base_url}&start={start}&limit={limit}"

            payload = {}
            headers = {
                'Accept': 'application/json',
                'Cookie': '__cf_bm=iHJQT34iNDqL_9aEoTv6DUIb4h5R5tRU7NUUFYPEnrY-1729235478-1.0.1.1-.uAb42hCTClrFiw_CZQuiqzvMmtMn8b1Vl0LvUWlv4fwQR1tAsUuxnKadKOAGaGGALlCOyL6SrUQpegQqJgLgw'
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            # Parse the response text as JSON
            response_json = json.loads(response.text)

            # Check if 'success' is True and 'data' is present
            if response_json.get("success") and "data" in response_json:
                data = response_json["data"]
                # Add this page's data to the all_data list
                all_data.extend(data)

                # Check if more data is available
                if len(data) < limit:
                    # If fewer items are returned than the limit, we've reached the end
                    break
                else:
                    # Otherwise, move to the next page
                    start += limit
            else:
                print(f"Failed to retrieve data for API token: {api_token}")
                break

    # Define the list of columns to keep
    columns_to_keep = [
        "id",
        "creator_user_id","org_name","add_time","value","pipeline_id","lost_time",
        "won_time","lost_reason","status","org_id","owner_name"
    ]

    # Convert data to a DataFrame
    if all_data:
        df_filtered = pd.DataFrame(all_data)

        # Keep only the specified columns
        df_filtered = df_filtered[columns_to_keep]

        def extract_name_from_dict(column_value):
            if isinstance(column_value, dict) and 'name' in column_value:
                return column_value['name']
            return None

        # Apply the function to the 'created_by_user_id' column
        df_filtered.loc[:, 'creator_user_id'] = df_filtered['creator_user_id'].apply(extract_name_from_dict)

        def extract_value_from_dict(column_value):
            if isinstance(column_value, dict) and 'value' in column_value:
                return column_value['value']
            return None

        # Use .loc[] to update the 'Organisation - ID' column (formerly org_id) safely
        df_filtered.loc[:, 'org_id'] = df_filtered['org_id'].apply(extract_value_from_dict)


        # Save the filtered data to a CSV file
        df_filtered.to_csv("csv/pipedrive/deals_1574.csv", index=False)
        print("Filtered data saved to 'pipe_deals_filtered_1574.csv'")
    else:
        print("No data to save.")

def clean_deals():
    df = pd.read_csv('csv/pipedrive/deals_1574.csv')
    # Define the renaming dictionary
    column_renaming = {
        'id': 'Affaire - ID',
        'creator_user_id': 'Affaire - Propriétaire',
        'org_name': 'Affaire - Organisation',
        'add_time': 'Affaire - Affaire créée',
        'value': 'Affaire - Valeur',
        'pipeline_id': 'Affaire - Pipeline',
        'lost_time': 'Affaire - Date de perte',
        'won_time': 'Affaire - Date de gain',
        'lost_reason': 'Affaire - Raison de la perte',
        'status': 'Affaire - Statut',
        'org_id': 'Organisation - ID',
        'owner_name': 'Organisation - Propriétaire'
    }

    # Apply the renaming
    df.rename(columns=column_renaming, inplace=True)

    df_act = pd.read_csv('csv/pipedrive/final_activities.csv')

    # Rename 'org_id' in df to match 'Organisation - ID' in df_act for the merge operation
    df.rename(columns={'org_id': 'Organisation - ID'}, inplace=True)

    # Check for matches between 'Organisation - ID' in deals and activities and merge accordingly
    df_deals = df.merge(df_act[['Organisation - ID', 'Organisation - 🙋‍♀️Source du lead']],
                        on='Organisation - ID', how='left')

    # If needed, rename 'Organisation - ID' back to 'org_id' after the merge
    df_deals.rename(columns={'Organisation - ID': 'org_id'}, inplace=True)

    # Remove the existing 'Organisation - 🙋‍♀️Source du lead' column (if it exists)
    if 'Organisation - 🙋‍♀️Source du lead' in df_deals.columns:
        df_deals.drop(columns=['Organisation - 🙋‍♀️Source du lead'], inplace=True)

    # Find the position of "Affaire - Pipeline" to insert the new column after it
    column_order = df_deals.columns.tolist()
    pipeline_index = column_order.index('Affaire - Pipeline')

    # Reorder columns by moving "Organisation - 🙋‍♀️Source du lead" after "Affaire - Pipeline"
    df_deals.insert(pipeline_index + 1, 'Organisation - 🙋‍♀️Source du lead',
                    df_act['Organisation - 🙋‍♀️Source du lead'])

    # Replace values in "Affaire - Pipeline" column
    df_deals['Affaire - Pipeline'] = df_deals['Affaire - Pipeline'].replace({1: 'Sales', 3: 'Appel d\'offres'})

    df_deals = df_deals.sort_values(by='Affaire - Affaire créée', ascending=False).drop_duplicates(
        subset='Affaire - Organisation', keep='first')

    # Save the final DataFrame to a CSV file
    df_deals.to_csv("csv/pipedrive/final_deal.csv", index=False)

def get_org():
    df = pd.read_csv("csv/pipedrive/activities_1573.csv")
    # Function to get organization details from Pipedrive API
    # Organization columns we need to extract and rename
    org_columns = {
        '922bac37cb73616026fec8bd253168c4926d5dc8': 'Organisation - ❌ Base Repoussoir',
        '2f79932ae88bd7bdb90cc2f1b467812661746c15': 'Organisation - Secteur Activité',
        '904e23a587184d6c5de5735f5755ffd5beabb5b7': 'Organisation - Last move',
        '928ab60ffc358cfe777e028d7a74617eb6a5362b': '💸 Volume MENSUEL estimé',
        '7a39b9620b49fb8d4ec4a053c02f1feebf6f28a7': 'Organisation - 🙋‍♀️Source du lead'
    }

    # Function to get and filter organization details from Pipedrive API
    def get_filtered_organization_details(org_id, api_token, org_columns):
        url = f"https://api.pipedrive.com/v1/organizations/{org_id}?api_token={api_token}"
        headers = {
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            org_data = response.json().get('data', {})
            # Filter the organization data based on required columns
            filtered_data = {org_columns[col]: org_data.get(col, None) for col in org_columns if col in org_data}
            filtered_data['org_id'] = org_id  # Keep org_id for merging later
            return filtered_data
        else:
            print(f"Failed to retrieve data for org_id {org_id}")
            return {}

    # List to store filtered organization details
    org_details_list = []

    # Pipedrive API token (replace with your active token)
    api_token = "ee422492cf8524fccc4914a021bca955e0153745"

    # Get unique org_ids from the DataFrame
    unique_org_ids = df['org_id'].unique()

    # Loop through each unique org_id and get the organization details
    for org_id in unique_org_ids:
        org_details = get_filtered_organization_details(org_id, api_token, org_columns)
        org_details_list.append(org_details)

    # Convert the list of filtered organization details into a DataFrame
    org_details_df = pd.DataFrame(org_details_list)

    # Merging the organization details back into the original DataFrame using 'org_id' as the key
    merged_df = pd.merge(df, org_details_df, on='org_id', how='left')

    merged_df.to_csv("csv/pipedrive/activities_1573_with_details.csv")

def merge_and_remove_duplicates():
    # Load the two dataframes
    df1 = pd.read_csv('csv/pipedrive/activities_1573.csv')
    df2 = pd.read_csv('csv/pipedrive/activities_1573_with_details.csv')

    # Merge with suffixes to differentiate between overlapping column names
    df = pd.merge(df1, df2, on='org_id', how='left', suffixes=('', '_duplicate'))

    # Drop columns with the '_duplicate' suffix (these are the duplicate columns from df2)
    duplicate_columns = [col for col in df.columns if '_duplicate' in col]
    df.drop(columns=duplicate_columns, inplace=True)

    # Mapping for "Organisation - 🙋‍♀️Source du lead"
    source_lead_mapping = {
        837: "Chasse Sales",
        838: "Réseau Perso",
        839: "Prospection externe",
        840: "AO public",
        841: "Market - Sales",
        842: "Market (Publi)",
        843: "Market (Formulaire)",
        869: "Market (Magnet)",
        844: "Market (Linkedin)",
        845: "Market (Call SDR)",
        846: "Market (Email)",
        847: "Event",
        848: "Account"
    }

    # Mapping for "assigned_to_user_id"
    assigned_to_user_mapping = {
        6969457: "Patrick Phimvilayphone",
        15574962: "Thibault",
        11447064: "Jordan Camelo",
        18425502: "Jeremy BORDAS",
        14905920: "Julien Ribeiro"
    }

    # Mapping for "Organisation - ❌ Base Repoussoir"
    base_repoussoir_mapping = {
        "805": "Ancien client",
        "811": "AO Public Only",
        "856": "Connectivité/Fonctionnalité manquante",
        "800": "Filiale",
        "817": "Filiale d'un groupe étranger",
        "801": "Ne voyage pas",
        "804": "Pas interessé",
        "812": "Pas interessant (ex. trop petit...)",
        "803": "Suspect",
        "802": "Trop gros",
        "806": "Zone non couverte (ex. Portugal only)"
    }

    # Convert the "💸 Volume MENSUEL estimé" column to float
    df['💸 Volume MENSUEL estimé'] = df['💸 Volume MENSUEL estimé'].astype(float)

    # Replace the IDs in the "Organisation - 🙋‍♀️Source du lead" column with their labels
    df['Organisation - 🙋‍♀️Source du lead'] = df['Organisation - 🙋‍♀️Source du lead'].map(source_lead_mapping)

    # Replace the IDs in the "assigned_to_user_id" column with their labels
    df['assigned_to_user_id'] = df['assigned_to_user_id'].map(assigned_to_user_mapping)

    # Replace the IDs in the "Organisation - ❌ Base Repoussoir" column with their labels
    df['Organisation - ❌ Base Repoussoir'] = df['Organisation - ❌ Base Repoussoir'].map(base_repoussoir_mapping)

    df.rename(columns={
        'org_name': 'Organisation - Nom',
        'add_time': 'Activité - Date de création',
        'person_name': 'Personne - Nom',
        'subject': 'Activité - Sujet',
        'org_id': 'Organisation - ID',
        'assigned_to_user_id': 'Activité - Attribuée à',
        '💸 Volume MENSUEL estimé': 'Organisation - 💸Volume MENSUEL estimé',
    }, inplace=True)

    # Sort by Organisation - Nom and Activité - Date de création in descending order
    df_sorted = df.sort_values(by=['Organisation - Nom', 'Activité - Date de création'], ascending=[True, False])

    # Drop duplicates, keeping the row with the most recent "Activité - Date de création"
    df_deduplicated = df_sorted.drop_duplicates(subset=['Organisation - Nom'], keep='first')

    # Define columns to keep
    columns_to_keep = [
        'Organisation - Nom', 'Activité - Date de création', 'Personne - Nom',
        'Activité - Sujet', 'Organisation - 🙋‍♀️Source du lead', 'Activité - Attribuée à',
        'Organisation - 💸Volume MENSUEL estimé', 'Organisation - ID', 'Organisation - Last move',
        'Organisation - Secteur Activité', 'Organisation - ❌ Base Repoussoir'
    ]

    # Filter the DataFrame
    df_filtered = df_deduplicated[columns_to_keep]

    # Save the final merged dataframe to a CSV file
    df_filtered.to_csv("csv/pipedrive/final_activities.csv", index=False)

def updated():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import pandas as pd

    # Chemin vers vos credentials JSON
    CREDENTIALS_FILE = 'creds/n8n-api-311609-115ae3a49fd9.json'

    # URL de votre Google Sheet
    GOOGLE_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1OxhBYP_WN99qPinTAFyjr9ZcD9AGLkrFB7C56Fu0O8M/edit?gid=1575663403'

    # Autorisations
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    # Authentification
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(credentials)

    # ------------------ Upload final_activities.csv to the "Demos" sheet ------------------ #
    # CSV que vous souhaitez uploader
    CSV_FILE_ACTIVITIES = 'csv/pipedrive/final_activities.csv'

    # Nom de l'onglet dans lequel vous voulez uploader le CSV (Demos)
    WORKSHEET_NAME_ACTIVITIES = 'Demos'

    # Ouverture du Google Sheet
    spreadsheet = client.open_by_url(GOOGLE_SHEET_URL)
    worksheet_activities = spreadsheet.worksheet(WORKSHEET_NAME_ACTIVITIES)

    # Lecture du CSV
    df_activities = pd.read_csv(CSV_FILE_ACTIVITIES)

    # Remplacer les NaN par des chaînes vides ou une autre valeur appropriée
    df_activities = df_activities.astype({col: 'object' for col in df_activities.select_dtypes(include='float64').columns})
    df_activities.fillna('', inplace=True)

    # Conversion en liste de listes (format accepté par gspread)
    data_activities = [df_activities.columns.values.tolist()] + df_activities.values.tolist()

    # Mise à jour du Google Sheet (onglet Demos)
    worksheet_activities.clear()  # Si vous voulez écraser les anciennes données
    worksheet_activities.update('A1', data_activities)  # Écrire les données à partir de la cellule A1

    # ------------------ Upload final_deal.csv to the "Affaires" sheet ------------------ #
    # CSV que vous souhaitez uploader
    CSV_FILE_DEAL = 'csv/pipedrive/final_deal.csv'

    # Nom de l'onglet dans lequel vous voulez uploader le CSV (Affaires)
    WORKSHEET_NAME_DEAL = 'Affaires'

    # Ouverture du Google Sheet (onglet Affaires)
    worksheet_deal = spreadsheet.worksheet(WORKSHEET_NAME_DEAL)

    # Lecture du CSV
    df_deal = pd.read_csv(CSV_FILE_DEAL)

    # Remplacer les NaN par des chaînes vides ou une autre valeur appropriée
    df_deal = df_deal.astype({col: 'object' for col in df_deal.select_dtypes(include='float64').columns})
    df_deal.fillna('', inplace=True)

    # Conversion en liste de listes (format accepté par gspread)
    data_deal = [df_deal.columns.values.tolist()] + df_deal.values.tolist()

    # Mise à jour du Google Sheet (onglet Affaires)
    worksheet_deal.clear()  # Si vous voulez écraser les anciennes données
    worksheet_deal.update('A1', data_deal)  # Écrire les données à partir de la cellule A1

def envoi_email(status,error):
    SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.readonly']

    sender_email = 'ope@supertripper.com'
    sender_name = 'Supertripper Reports'
    recipient_email = "ope@supertripper.com"
    subject = f'STATS ACQUISITION : CRON {status}'

    # Construction du corps de l'e-mail
    body = (
        f'{error}'
    )
    creds_file = 'creds/cred_gmail.json'
    token_file = 'token.json'
    def authenticate_gmail():
        """Authentifie l'utilisateur via OAuth 2.0 et retourne les credentials"""
        creds = None
        # Le token est stocké localement après la première authentification
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        # Si le token n'existe pas ou est expiré, on initie un nouveau flux OAuth
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
                creds = flow.run_local_server(port=0)
            # Enregistrer le token pour des sessions futures
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        return creds

    def create_message_with_attachment(sender, sender_name, to, subject, message_text):
        """Crée un e-mail avec une pièce jointe et un champ Cc"""
        message = MIMEMultipart()
        message['to'] = to
        message['from'] = f'{sender_name} <{sender}>'
        message['subject'] = subject

        # Attacher le corps du texte
        message.attach(MIMEText(message_text, 'plain'))

        # Encoder le message en base64 pour l'envoi via l'API Gmail
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        return {'raw': raw_message}

    def send_email(service, user_id, message):
        """Envoie un e-mail via l'API Gmail"""
        try:
            message = service.users().messages().send(userId=user_id, body=message).execute()
            print(f"Message Id: {message['id']}")
            return message
        except HttpError as error:
            print(f'An error occurred: {error}')
            return None

    # Authentifier l'utilisateur et créer un service Gmail
    creds = authenticate_gmail()
    service = build('gmail', 'v1', credentials=creds)

    # Créer le message avec pièce jointe et copie
    message = create_message_with_attachment(sender_email, sender_name, recipient_email, subject, body)

    # Envoyer l'e-mail
    send_email(service, 'me', message)
    print("Mail envoyé pour vérif ")