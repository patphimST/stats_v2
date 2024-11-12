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
import pandas as pd
from datetime import datetime, timedelta
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
        df_filtered.to_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/activities_1573.csv", index=False)
        print("Filtered data saved to 'pipe_activities_filtered_1573.csv'")
    else:
        print("No data to save.")


import requests
import json
import pandas as pd
import config  # Assuming config holds your API tokens


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

            response = requests.get(url, headers=headers, data=payload)
            response_json = response.json()

            if response_json.get("success") and "data" in response_json:
                data = response_json["data"]
                # Add this page's data to the all_data list
                all_data.extend(data)

                # Check if more data is available
                if len(data) < limit:
                    break  # No more data to fetch
                else:
                    start += limit  # Move to the next page
            else:
                print(f"Failed to retrieve data for API token: {api_token}")
                break

    # Define the list of columns to keep
    columns_to_keep = [
        "id", "creator_user_id", "org_name", "add_time", "value",
        "pipeline_id", "lost_time", "won_time", "lost_reason",
        "status", "org_id", "owner_name"
    ]

    if all_data:
        df_filtered = pd.DataFrame(all_data)

        # Keep only the specified columns
        df_filtered = df_filtered[columns_to_keep]

        # Extract necessary values from dictionaries in specific columns
        df_filtered['creator_user_id'] = df_filtered['creator_user_id'].apply(
            lambda x: x['name'] if isinstance(x, dict) and 'name' in x else None
        )
        df_filtered['org_id'] = df_filtered['org_id'].apply(
            lambda x: x['value'] if isinstance(x, dict) and 'value' in x else None
        )

        # Remove duplicates based on 'id' column
        df_filtered = df_filtered.drop_duplicates(subset="id")

        # Display row count to verify
        print(f"Final row count after removing duplicates: {len(df_filtered)}")

        # Save the filtered data to a CSV file
        df_filtered.to_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/deals_1574.csv", index=False)
    else:
        print("No data to save.")


def clean_deals():
    df = pd.read_csv('/Users/patrick/PycharmProjects/stats/csv/pipedrive/deals_1574.csv')
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

    df_act = pd.read_csv('/Users/patrick/PycharmProjects/stats/csv/pipedrive/final_activities.csv')

    # Rename 'org_id' in df to match 'Organisation - ID' in df_act for the merge operation
    df.rename(columns={'org_id': 'Organisation - ID'}, inplace=True)

    # Check for matches between 'Organisation - ID' in deals and activities and merge accordingly
    df_deals = df.merge(df_act[['Organisation - ID', 'Organisation - 🙋‍♀️Source du lead']],
                        on='Organisation - ID', how='left')

    # If needed, rename 'Organisation - ID' back to 'org_id' after the merge
    df_deals.rename(columns={'Organisation - ID': 'org_id'}, inplace=True)

    # # Remove the existing 'Organisation - 🙋‍♀️Source du lead' column (if it exists)
    # if 'Organisation - 🙋‍♀️Source du lead' in df_deals.columns:
    #     df_deals.drop(columns=['Organisation - 🙋‍♀️Source du lead'], inplace=True)

    # Find the position of "Affaire - Pipeline" to insert the new column after it
    column_order = df_deals.columns.tolist()
    pipeline_index = column_order.index('Affaire - Pipeline')

    # # Reorder columns by moving "Organisation - 🙋‍♀️Source du lead" after "Affaire - Pipeline"
    # df_deals.insert(pipeline_index + 1, 'Organisation - 🙋‍♀️Source du lead',
    #                 df_act['Organisation - 🙋‍♀️Source du lead'])

    # Replace values in "Affaire - Pipeline" column
    df_deals['Affaire - Pipeline'] = df_deals['Affaire - Pipeline'].replace({1: 'Sales', 3: 'Appel d\'offres'})

    df_deals['Affaire - Affaire créée'] = pd.to_datetime(df_deals['Affaire - Affaire créée'], errors='coerce')
    df_deals['yearmonth_deal_created'] = df_deals['Affaire - Affaire créée'].dt.strftime('%Y-%m')

    # df_deals['Affaire - Date de perte'] = pd.to_datetime(df_deals['Affaire - Date de perte'], errors='coerce')
    # df_deals['yearmonth_deal_lost'] = df_deals['Affaire - Date de perte'].dt.strftime('%Y-%m')

    df_deals['Affaire - Date de gain'] = pd.to_datetime(df_deals['Affaire - Date de gain'], errors='coerce')
    df_deals['yearmonth_deal_won'] = df_deals['Affaire - Date de gain'].dt.strftime('%Y-%m')

    df_deals = df_deals.sort_values(by='Affaire - Affaire créée', ascending=False).drop_duplicates(
        subset='Affaire - Organisation', keep='first')

    # Save the final DataFrame to a CSV file
    df_deals.to_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/final_deal.csv", index=False)

def get_org():
    df = pd.read_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/activities_1573.csv")
    df["org_id"] = df["org_id"].fillna(0).astype(int)
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
    api_token = config.api_pipedrive

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

    merged_df.to_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/activities_1573_with_details.csv")

def merge_and_remove_duplicates():
    # Load the two dataframes
    df1 = pd.read_csv('/Users/patrick/PycharmProjects/stats/csv/pipedrive/activities_1573.csv')
    df2 = pd.read_csv('/Users/patrick/PycharmProjects/stats/csv/pipedrive/activities_1573_with_details.csv')

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

    df_filtered = df_filtered.copy()

    df_filtered['Activité - Date de création'] = pd.to_datetime(df_filtered['Activité - Date de création'], errors='coerce')

    # Create a new column 'yearmonth' by extracting year and month in 'YYYY-MM' format
    df_filtered['yearmonth'] = df_filtered['Activité - Date de création'].dt.strftime('%Y-%m')

    # Save the final merged dataframe to a CSV file
    df_filtered.to_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/final_activities.csv", index=False)

def updated():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import pandas as pd

    # Chemin vers vos credentials JSON
    CREDENTIALS_FILE = '/Users/patrick/PycharmProjects/stats/creds/n8n-api-311609-115ae3a49fd9.json'

    # URL de votre Google Sheet
    GOOGLE_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1OxhBYP_WN99qPinTAFyjr9ZcD9AGLkrFB7C56Fu0O8M/edit?gid=1575663403'

    # Autorisations
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    # Authentification
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(credentials)

    # ------------------ Upload final_activities.csv to the "Demos" sheet ------------------ #
    # CSV que vous souhaitez uploader
    CSV_FILE_ACTIVITIES = '/Users/patrick/PycharmProjects/stats/csv/pipedrive/final_activities.csv'

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
    CSV_FILE_DEAL = '/Users/patrick/PycharmProjects/stats/csv/pipedrive/final_deal.csv'

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
    subject = f'CRON "STATS ACQUISITION" {status}'

    # Construction du corps de l'e-mail
    body = (
        f'{error}'
    )
    creds_file = '/Users/patrick/PycharmProjects/stats/creds/cred_gmail.json'
    token_file = '/Users/patrick/PycharmProjects/stats/token.json'
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

def summary_rdv():
    # Load the CSV files provided by the user
    file_1_path = '/Users/patrick/PycharmProjects/stats/csv/pipedrive/final_activities.csv'

    # Reading the CSV files into pandas DataFrames
    activities_df = pd.read_csv(file_1_path)

    # Convert the "Activité - Date de création" column to datetime
    activities_df['Activité - Date de création'] = pd.to_datetime(activities_df['Activité - Date de création'], errors='coerce')

    # Get today's date and calculate the date range for the previous week
    today = datetime.today()
    start_of_last_week = today - timedelta(days=today.weekday() + 7)  # Start of last week (Monday)
    end_of_last_week = start_of_last_week + timedelta(days=6)  # End of last week (Sunday)

    # Filter activities that fall within the last week, creating a copy to avoid SettingWithCopyWarning
    last_week_activities = activities_df[
        (activities_df['Activité - Date de création'] >= start_of_last_week) &
        (activities_df['Activité - Date de création'] <= end_of_last_week)
    ].copy()  # Use .copy() to explicitly create a new DataFrame

    # Renaming columns for ease of use
    last_week_activities.rename(columns={
        "Organisation - 🙋‍♀️Source du lead": "lead_source",
        "Organisation - 💸Volume MENSUEL estimé": "monthly_volume",
        "Organisation - Nom": "organisation_name"  # Rename Organisation name column for easy access
    }, inplace=True)

    # Creating a function to categorize lead sources and monthly volumes as specified
    def categorize_lead_and_volume(row):
        lead_source = row['lead_source']
        monthly_volume = row['monthly_volume']

        # Initialize variables
        category = None

        # Conditions based on lead sources and volume estimates
        if lead_source in ["Market (Email)", "Market (Linkedin)"]:
            if monthly_volume < 25000:
                category = "RDV Market (Email & LK) -25k"
            else:
                category = "RDV Market (Email & LK) +25k"
        elif lead_source == "Market - Sales":
            if monthly_volume < 25000:
                category = "RDV Market Sales -25k"
            else:
                category = "RDV Market Sales +25k"
        elif lead_source == "Market (Formulaire)":
            if monthly_volume < 25000:
                category = "Demandes de Démo -25k"
            else:
                category = "Demandes de Démo +25k"
        elif lead_source == "Market (Publi)":
            category = "Publipostage"
        elif lead_source == "Event":
            category = "Event"

        return category

    # Apply this function to create a new column 'Category'
    last_week_activities['Category'] = last_week_activities.apply(categorize_lead_and_volume, axis=1)

    # Creating the 'Prospect' column by combining "Organisation - Nom" and "monthly_volume" with the € symbol
    last_week_activities['Prospect'] = last_week_activities.apply(
        lambda row: f'{row["organisation_name"]} : {(row["monthly_volume"])} €', axis=1  # Add € symbol to monthly_volume
    )

    # Now, let's compute the number of items, total values, and concatenate prospects for each category
    summary = last_week_activities.groupby('Category').agg(
        Nombre_d_items=('monthly_volume', 'count'),
        Total_des_valeurs=('monthly_volume', 'sum'),
        Prospect=('Prospect', lambda x: ', '.join(x))  # Concatenate prospects separated by commas
    ).reset_index()

    # Debugging step: Print the columns of the summary DataFrame to verify
    print("Summary DataFrame Columns:", summary.columns)

    # Ensure all requested categories are in the result and order them
    requested_categories = [
        "RDV Market (Email & LK) -25k",
        "RDV Market (Email & LK) +25k",
        "RDV Market Sales -25k",
        "RDV Market Sales +25k",
        "Demandes de Démo -25k",
        "Demandes de Démo +25k",
        "Publipostage",
        "Event"
    ]

    # Create an empty DataFrame with the same structure if categories are missing
    empty_df = pd.DataFrame({
        'Category': requested_categories,
        'Nombre_d_items': [0] * len(requested_categories),
        'Total_des_valeurs': [0] * len(requested_categories),
        'Prospect': [''] * len(requested_categories)
    })

    # Merge the summary with the empty DataFrame to ensure all categories are present
    final_summary = pd.merge(empty_df, summary, on='Category', how='left')

    # Fill missing values with 0 or empty strings where appropriate
    final_summary['Nombre_d_items_y'] = final_summary['Nombre_d_items_y'].fillna(0).astype(int)
    final_summary['Total_des_valeurs_y'] = final_summary['Total_des_valeurs_y'].fillna(0).astype(int)
    final_summary['Prospect_y'] = final_summary['Prospect_y'].fillna('')

    # Renaming merged columns back to original names
    final_summary.rename(columns={
        'Nombre_d_items_y': 'Nombre_d_items',
        'Total_des_valeurs_y': 'Total_des_valeurs',
        'Prospect_y': 'Prospect'
    }, inplace=True)

    # Print the final summary in the requested order
    final_summary = final_summary[['Category', 'Nombre_d_items', 'Total_des_valeurs', 'Prospect']]

    final_summary.to_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/summary_rdv.csv", index=False)
    print(final_summary)

def summary_deals():
    # Load the CSV file provided by the user
    file_2_path = '/Users/patrick/PycharmProjects/stats/csv/pipedrive/final_deal.csv'

    # Reading the CSV file into a pandas DataFrame
    deals_df = pd.read_csv(file_2_path)

    # Convert the "Affaire - Affaire créée" column to datetime in deals_df
    deals_df['Affaire - Affaire créée'] = pd.to_datetime(deals_df['Affaire - Affaire créée'], errors='coerce')

    # Get today's date and calculate the date range for the previous week (without time)
    today = datetime.today().date()
    start_of_last_week = today - timedelta(days=today.weekday() + 7)  # Start of last week (Monday)
    end_of_last_week = start_of_last_week + timedelta(days=6)  # End of last week (Sunday)

    # Filter deals that were created within the last week (comparing only dates)
    last_week_deals = deals_df[
        (deals_df['Affaire - Affaire créée'].dt.date >= start_of_last_week) &
        (deals_df['Affaire - Affaire créée'].dt.date <= end_of_last_week)
    ].copy()  # Use .copy() to explicitly create a new DataFrame

    # Creating a function to categorize deals based on their value
    def categorize_deal(row):
        deal_value = row['Affaire - Valeur']
        if deal_value < 25000:
            return 'Deals <25k'
        else:
            return 'Deals >=25k'

    # Apply this function to create a new column 'Category'
    last_week_deals['Category'] = last_week_deals.apply(categorize_deal, axis=1)

    # Creating the 'Prospect' column by combining "Affaire - Organisation" and "Affaire - Valeur" with the € symbol
    last_week_deals['Prospect'] = last_week_deals.apply(
        lambda row: f'{row["Affaire - Organisation"]} : {int(row["Affaire - Valeur"])} €', axis=1
    )

    # Now, let's compute the number of items, total values, and concatenate prospects for each category
    summary_deals = last_week_deals.groupby('Category').agg(
        Nombre_d_items=('Affaire - Valeur', 'count'),
        Total_des_valeurs=('Affaire - Valeur', 'sum'),
        Prospect=('Prospect', lambda x: ', '.join(x))  # Concatenate prospects separated by commas
    ).reset_index()

    # Ensure all requested categories are in the result and order them
    requested_categories_deals = [
        "Deals <25k",
        "Deals >=25k"
    ]

    # Create an empty DataFrame with the same structure if categories are missing
    empty_deals_df = pd.DataFrame({
        'Category': requested_categories_deals,
        'Nombre_d_items': [0] * len(requested_categories_deals),
        'Total_des_valeurs': [0] * len(requested_categories_deals),
        'Prospect': [''] * len(requested_categories_deals)
    })

    # Merge the summary with the empty DataFrame to ensure all categories are present
    final_summary_deals = pd.merge(empty_deals_df, summary_deals, on='Category', how='left')

    # Fill missing values with 0 or empty strings where appropriate
    final_summary_deals['Nombre_d_items'] = final_summary_deals['Nombre_d_items_y'].fillna(0).astype(int)
    final_summary_deals['Total_des_valeurs'] = final_summary_deals['Total_des_valeurs_y'].fillna(0).astype(int)
    final_summary_deals['Prospect'] = final_summary_deals['Prospect_y'].fillna('')

    # Drop the unnecessary _y columns after merging
    final_summary_deals = final_summary_deals[['Category', 'Nombre_d_items', 'Total_des_valeurs', 'Prospect']]

    # Print the final summary for deals in the requested order
    final_summary_deals.to_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/summary_deals.csv", index=False)

def summary_merge():
    # Load the CSV files provided by the user
    file_rdv = '/Users/patrick/PycharmProjects/stats/csv/pipedrive/summary_rdv.csv'
    file_offers = '/Users/patrick/PycharmProjects/stats/csv/pipedrive/summary_deals.csv'

    # Read both CSV files
    df_rdv = pd.read_csv(file_rdv)
    df_offers = pd.read_csv(file_offers)

    # Remove 'Unnamed: 0' column if it exists
    if 'Unnamed: 0' in df_rdv.columns:
        df_rdv_clean = df_rdv.drop(columns=['Unnamed: 0'])
    else:
        df_rdv_clean = df_rdv

    if 'Unnamed: 0' in df_offers.columns:
        df_offers_clean = df_offers.drop(columns=['Unnamed: 0'])
    else:
        df_offers_clean = df_offers

    from datetime import datetime, timedelta

    # Calculate the start and end date of last week
    today = datetime.today()
    start_of_week_last = today - timedelta(days=today.weekday() + 7)  # start of last week (Monday)
    end_of_week_last = start_of_week_last + timedelta(days=4)  # end of last week (Friday)

    # Format the dates for the title
    formatted_date_range = f'{start_of_week_last.strftime("%d/%m/%Y")} - {end_of_week_last.strftime("%d/%m/%Y")}'

    # Add the € symbol to the 'Total_des_valeurs' and 'Prospect' columns for both RDV and deals
    df_rdv_clean['Total_des_valeurs'] = df_rdv_clean['Total_des_valeurs'].apply(lambda x: f'{x} €')
    df_offers_clean['Total_des_valeurs'] = df_offers_clean['Total_des_valeurs'].apply(lambda x: f'{x} €')

    # Apply the € symbol to the 'Prospect' column and sorting values by amount in descending order
    def sort_prospects(prospect_str):
        if isinstance(prospect_str, str):
            # Split each prospect and extract the numeric part for sorting
            prospects = prospect_str.split(', ')
            sorted_prospects = sorted(prospects, key=lambda x: (x.split(':')[1].strip(' €')), reverse=True)
            return ', '.join(sorted_prospects)
        else:
            return prospect_str  # Return the original value if not a string (likely NaN)

    # Apply sorting and € formatting to the 'Prospect' columns of both RDV and offers
    df_rdv_clean['Prospect'] = df_rdv_clean['Prospect'].apply(lambda x: sort_prospects(x) if pd.notnull(x) else '')
    df_offers_clean['Prospect'] = df_offers_clean['Prospect'].apply(lambda x: sort_prospects(x) if pd.notnull(x) else '')

    # Write the updated data into Excel with the correct date range and without extra spacing
    excel_file_path_updated = '/Users/patrick/PycharmProjects/stats/csv/pipedrive/resume_rdvs_offers.xlsx'
    with pd.ExcelWriter(excel_file_path_updated, engine='xlsxwriter') as writer:
        # Write the first table (Resume des RDV)
        df_rdv_clean.to_excel(writer, sheet_name='Sheet1', startrow=1, index=False)

        # Write the second table (Resume des Offres) with exactly 3 interline spacing, no extra header
        df_offers_clean.to_excel(writer, sheet_name='Sheet1', startrow=len(df_rdv_clean) + 5, header=True, index=False)

        # Access the workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add titles for each table with actual date range
        title_format = workbook.add_format({'bold': True, 'font_size': 14})
        worksheet.write('A1', f'Resume des RDV : {formatted_date_range}', title_format)
        worksheet.write(f'A{len(df_rdv_clean) + 4}', f'Resume des offres : {formatted_date_range}', title_format)

        # Apply header formatting
        header_format = workbook.add_format({'bold': True, 'bg_color': '#FFC000', 'border': 1})

        # Apply header formatting to the first table (RDV)
        for col_num, value in enumerate(df_rdv_clean.columns.values):
            worksheet.write(1, col_num, value, header_format)

        # Apply header formatting to the second table (Deals) manually
        for col_num, value in enumerate(df_offers_clean.columns.values):
            worksheet.write(len(df_rdv_clean) + 5, col_num, value, header_format)

