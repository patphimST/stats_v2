import numpy as np
import requests
import pandas as pd
import config
from datetime import datetime


def demos():
    df_rdv = pd.read_csv('csv/pipedrive/rdv.csv',delimiter=",")
    df_rdv['Organisation - Étiquettes'] = df_rdv['Organisation - Étiquettes'].fillna("-")
    df_rdv['Organisation - 💶 Volume mensuel estimé'] = df_rdv['Organisation - 💶 Volume mensuel estimé'].fillna("-")
    if "Activité - Date de création" in df_rdv.columns:
        # Renommer la colonne en "Date d'ajout"
        df_rdv.rename(columns={"Activité - Date de création": "Activité - Date d’ajout"}, inplace=True)
    df_rdv['year_week'] = pd.to_datetime(df_rdv['Activité - Date d’ajout']).dt.strftime("%yW%U")
    df_rdv = df_rdv.sort_values('Activité - Date d’ajout').drop_duplicates('Organisation - Nom', keep='last')
    df_rdv.to_csv(f"csv/pipedrive/rdv.csv", index=False)

    ahmed_regul = [130039,135482,139624,145550,138339,138353,138375,60063,139463,137700,140792,75961,138384,139562,139566,130601,136081,138608,147081,138575,137811]

    for a in ahmed_regul:
        masque = df_rdv['Organisation - ID'] == a
        df_rdv.loc[masque, 'Activité - Attribué à l’utilisateur'] = 'Ahmed NAFAI'

    df_deal = pd.read_csv("csv/pipedrive/deals.csv",delimiter=",")

    df_deal = df_deal.sort_values('Affaire - Affaire créée').drop_duplicates('Affaire - Organisation', keep='last')

    result = df_rdv['Organisation - Nom'].isin(df_deal['Affaire - Organisation'])
    result = df_rdv.loc[result]
    result.to_csv("result.csv")

    df1 = pd.merge(df_rdv, df_deal, left_on='Organisation - Nom', right_on='Affaire - Organisation', how='left')

    df1 = df1.drop('Affaire - Organisation', axis=1)

    df1["Activité - Date d’ajout"] = pd.to_datetime(df1["Activité - Date d’ajout"])
    df1["Affaire - Affaire créée"] = pd.to_datetime(df1["Affaire - Affaire créée"])

    df1.to_csv('csv/pipedrive/rdv2.csv')

def marquage_n():

    df = pd.read_csv('csv/pipedrive/rdv2.csv')

    def classify_date(date):
        if pd.Timestamp('2021-10-1') <= date <= pd.Timestamp('2022-09-30'):
            return 'N-2'
        elif pd.Timestamp('2022-10-1') <= date <= pd.Timestamp('2023-09-30'):
            return 'N-1'
        elif pd.Timestamp('2023-10-1') <= date <= pd.Timestamp('2024-09-30'):
            return 'N'
        else:
            return None  # ou vous pourriez vouloir gérer autrement les dates hors périodes

    # Convertir les colonnes de date en datetime
    df['Activité - Date d’ajout'] = pd.to_datetime(df['Activité - Date d’ajout'])
    df['Affaire - Affaire créée'] = pd.to_datetime(df['Affaire - Affaire créée'])
    df['Affaire - Date de gain'] = pd.to_datetime(df['Affaire - Date de gain'])

    df = df.rename(columns={'Affaire - Date de perte': "Affaire - Heure de l'échec"})
    df["Affaire - Heure de l'échec"] = pd.to_datetime(df["Affaire - Heure de l'échec"])

    # Appliquer la fonction pour créer les nouvelles colonnes
    df['RDV_N'] = df['Activité - Date d’ajout'].apply(classify_date)
    df['OFFRE_N'] = df['Affaire - Affaire créée'].apply(classify_date)
    df['WON_N'] = df['Affaire - Date de gain'].apply(classify_date)
    df['LOST_N'] = df["Affaire - Heure de l'échec"].apply(classify_date)

    unnamed_cols = [col for col in df.columns if col.startswith('Unnamed')]
    df.drop(columns=unnamed_cols, inplace=True)
    # df = df.drop(columns=['Affaire - Visible par',"Personne - Étiquette","Personne - Intitulé du poste","Activité - Date d'échéance","Affaire - Titre"])
    #
    # def categorize_source(value):
    #     if "Market - Sales" in value:
    #         return "Market - Sales"
    #     elif "Market" in value:
    #         return "Market"
    #     elif "Sales" in value:
    #         return "Sales"
    #     elif "Sales" in value:
    #         return "Sales"
    #     elif "Réseau perso" in value:
    #         return "Sales"
    #     else:
    #         return "Other"
    #
    # # Appliquer la fonction pour créer la nouvelle colonne
    # df['cat_source'] = df['Organisation - 🚨 Source du lead'].apply(categorize_source)

    # Afficher le DataFrame résultant
    print(df)
    df.to_csv('csv/pipedrive/rdv2.csv')

def update_sheet(oauth2client=None):
    from googleapiclient.http import MediaFileUpload
    import pandas as pd
    from google.oauth2.service_account import Credentials
    from gspread_pandas import Spread, Client
    from googleapiclient.discovery import build

    # Read and clean the CSV file
    df = pd.read_csv("csv/pipedrive/rdv2.csv", index_col=False)
    unnamed_cols = [col for col in df.columns if col.startswith('Unnamed')]
    df.drop(columns=unnamed_cols, inplace=True)
    df['Affaire - Valeur'] = df['Affaire - Valeur'].astype(str)
    df['Affaire - Valeur'] = df['Affaire - Valeur'].str.replace('.', ',', regex=False)
    df['Affaire - Valeur'] = df['Affaire - Valeur'].str.replace('nan', '', regex=False)

    df.to_csv('csv/pipedrive/rdv2.csv')

    print("########### GET UPDATE SHEET START ###########")

    # Path to your credentials JSON file
    cred_file = 'creds/n8n-api-311609-115ae3a49fd9.json'

    # Define required scopes
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    # Authenticate using service account file
    credentials = Credentials.from_service_account_file(cred_file, scopes=SCOPES)

    # Build Google Drive service
    drive_service = build('drive', 'v3', credentials=credentials)

    # Search for the existing file by name in the specified folder
    query = f"name = 'stats Acquisition 22-23' and '13iaRsAhvIj-PIXYi0GwaFBNHroUXKRyY' in parents"
    response = drive_service.files().list(q=query, fields='files(id, name)').execute()
    files = response.get('files', [])

    if not files:
        raise FileNotFoundError("The file 'stats Acquisition 22-23' was not found in the specified folder.")

    # Get the file ID of the existing file
    file_id = files[0]['id']

    # Initialize gspread_pandas client
    client = Client(scope=SCOPES, creds=credentials)

    # Open the Google Sheet by ID
    spread = Spread(spread=file_id, client=client)

    # Write DataFrame to the specific sheet 'Demos'
    spread.df_to_sheet(df, sheet='Demos', start='A1', replace=True)

    print('Data updated in the "Demos" sheet successfully!')
