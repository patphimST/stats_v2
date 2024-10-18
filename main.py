import traceback
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import base64
from get_data import *
# Other imports (requests, pandas, config, etc.)
import requests
import pandas as pd
import config


# Include your functions here (get_activities, get_deals, clean_deals, etc.)

def main():
    try:
        # Step 1: Retrieve activities and deals
        get_activities()
        get_deals()

        # Step 2: Clean the deals data
        clean_deals()

        # Step 3: Perform organization data operations
        get_org()

        # Step 4: Merge and remove duplicates
        merge_and_remove_duplicates()

        # Step 5: Upload the final CSV files to Google Sheets
        updated()

        # If everything is successful, send a success email
        status = "✅ Success"
        error = "OK"

    except Exception as e:
        # Capture the traceback in case of failure
        error_message = traceback.format_exc()
        status = "❌ Failed"
        error = error_message

    # Finally, send the status email
    envoi_email(status, error)


def envoi_email(status, error):
    # The envoi_email function as provided, without changes
    SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.readonly']

    sender_email = 'ope@supertripper.com'
    sender_name = 'Supertripper Reports'
    recipient_email = "ope@supertripper.com"
    subject = f'STATS ACQUISITION : CRON {status}'

    # Construction du corps de l'e-mail
    body = f'{error}'
    creds_file = 'creds/cred_gmail.json'
    token_file = 'token.json'

    def authenticate_gmail():
        """Authentifie l'utilisateur via OAuth 2.0 et retourne les credentials"""
        creds = None
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        return creds

    def create_message_with_attachment(sender, sender_name, to, subject, message_text):
        """Crée un e-mail avec une pièce jointe et un champ Cc"""
        message = MIMEMultipart()
        message['to'] = to
        message['from'] = f'{sender_name} <{sender}>'
        message['subject'] = subject

        message.attach(MIMEText(message_text, 'plain'))
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

    creds = authenticate_gmail()
    service = build('gmail', 'v1', credentials=creds)

    message = create_message_with_attachment(sender_email, sender_name, recipient_email, subject, body)
    send_email(service, 'me', message)
    print("Mail envoyé pour vérification")


# Run the main function
if __name__ == "__main__":
    main()
