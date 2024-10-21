import pandas as pd
from datetime import datetime, timedelta

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

    # Creating the 'Prospect' column by combining "Organisation - Nom" and "monthly_volume" without decimals
    last_week_activities['Prospect'] = last_week_activities.apply(
        lambda row: f'{row["organisation_name"]} : {int(row["monthly_volume"])}', axis=1  # Format monthly_volume as an integer
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

    final_summary.to_csv("/Users/patrick/PycharmProjects/stats/csv/pipedrive/summary_rdv.csv")
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

    # Creating the 'Prospect' column by combining "Affaire - Organisation" and "Affaire - Valeur" without decimals
    last_week_deals['Prospect'] = last_week_deals.apply(
        lambda row: f'{row["Affaire - Organisation"]} : {int(row["Affaire - Valeur"])}', axis=1  # Format value as integer
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

    # Add the € symbol to the 'Total_des_valeurs' and 'Prospect' columns
    df_rdv_clean['Total_des_valeurs'] = df_rdv_clean['Total_des_valeurs'].apply(lambda x: f'{x} €')
    df_offers_clean['Total_des_valeurs'] = df_offers_clean['Total_des_valeurs'].apply(lambda x: f'{x} €')

    # Apply the same for 'Prospect' column, sorting values by amount in descending order
    def sort_prospects(prospect_str):
        if isinstance(prospect_str, str):
            # Split each prospect and extract the numeric part for sorting
            prospects = prospect_str.split(', ')
            sorted_prospects = sorted(prospects, key=lambda x: int(x.split(':')[1].strip()), reverse=True)
            return ', '.join(sorted_prospects)
        else:
            return prospect_str  # Return the original value if not a string (likely NaN)

    df_rdv_clean['Prospect'] = df_rdv_clean['Prospect'].apply(sort_prospects)
    df_offers_clean['Prospect'] = df_offers_clean['Prospect'].apply(sort_prospects)

    # Write the updated data into Excel with the correct date range and without extra spacing
    excel_file_path_updated = '/Users/patrick/PycharmProjects/stats/csv/pipedrive/resume_rdvs_offers.xlsx'
    with pd.ExcelWriter(excel_file_path_updated, engine='xlsxwriter') as writer:
        # Write the first table (Resume des RDV)
        df_rdv_clean.to_excel(writer, sheet_name='Sheet1', startrow=1, index=False)

        # Write the second table (Resume des Offres) with exactly 3 interline spacing
        df_offers_clean.to_excel(writer, sheet_name='Sheet1', startrow=len(df_rdv_clean) + 4, index=False)

        # Access the workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add titles for each table with actual date range
        title_format = workbook.add_format({'bold': True, 'font_size': 14})
        worksheet.write('A1', f'Resume des RDV : {formatted_date_range}', title_format)
        worksheet.write(f'A{len(df_rdv_clean) + 4}', f'Resume des offres : {formatted_date_range}', title_format)

        # Apply header formatting
        header_format = workbook.add_format({'bold': True, 'bg_color': '#FFC000', 'border': 1})

        for col_num, value in enumerate(df_rdv_clean.columns.values):
            worksheet.write(1, col_num, value, header_format)

        for col_num, value in enumerate(df_offers_clean.columns.values):
            worksheet.write(len(df_rdv_clean) + 5, col_num, value, header_format)


summary_rdv()
summary_deals()
summary_merge()