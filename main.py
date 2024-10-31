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
import requests
import pandas as pd
import config


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


if __name__ == "__main__":
    main()

summary_rdv()
summary_deals()
summary_merge()
