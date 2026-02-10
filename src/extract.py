"""
    Esse módulo é responsável por extrair os dados do
    Google Sheets ou dos CSVs locais e convertê-los em
    Pandas Dataframes
"""

import pandas as pd
import gspread


def fetch_sheets() -> list[pd.DataFrame] | None:
    """ Busca os dados dos CSVs locais e os converte em Dataframes"""

    try:
        df_main = pd.read_csv('./data/raw/main.csv')
        df_genotypes = pd.read_csv('./data/raw/genotypes_williams.csv')
        df_juliana = pd.read_csv('./data/raw/genotypes_juliana.csv')

        return [df_main, df_genotypes, df_juliana]

    except FileNotFoundError as e:
        print(f'Error caught while fetching sheets: {e}')
        return None


def fetch_google_sheets(ids: dict[str, str]) -> list[pd.DataFrame] | None:
    """ Busca os dados do Google Sheets e os converte em Dataframes"""

    try:
        google_client = gspread.service_account(filename='../.auth.json')
        spreadsheet = google_client.open_by_key(ids['spreadsheet_id'])

        data_genotypes = (spreadsheet
                            .get_worksheet_by_id(ids['sheet_id_genotypes'])
                            .get_all_records())
        data_main = (spreadsheet
                        .get_worksheet_by_id(ids['sheet_id_main'])
                        .get_all_records())

        data_jul = (spreadsheet
                        .get_worksheet_by_id(ids['sheet_id_juliana'])
                        .get_all_records())

        return [pd.DataFrame(data_main),  pd.DataFrame(data_genotypes),  pd.DataFrame(data_jul)]

    except (gspread.exceptions.GSpreadException, FileNotFoundError) as e:
        print(f'Error caught while fetching sheets: {e}')
        return None


if __name__ == "__main__":
    print(fetch_sheets())
