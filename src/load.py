""" Módulo responsável pela conexão e inserção dos dados transformados """

from supabase import create_client


def connect(project_url: str, api_key: str):
    """Estabelece conexão com o cliente Supabase."""
    try:
        client = create_client(project_url, api_key)
        print("Connection successful")
        return client

    except (TypeError, ValueError, AttributeError) as e:
        print(f"Error caught while trying to connect: {e}")
        return None


def insert_records(client, df):
    """Insere os registros do DataFrame na tabela do banco de dados."""
    records = df.to_dict('records')

    try:
        if records:
            (
            client.table('individuos')
            .insert(records)
            .execute()
            )
            print(f"{len(records)} records inserted successfully")
        else:
            print('Records list is empty')
    except (ValueError, KeyError) as e:
        print(f"Error caught while trying to insert records: {e}")


def save_to_csv(df, filename: str = "transformed_data.csv"):
    """Salva o DataFrame como um arquivo CSV na pasta data/transformed."""
    try:
        path = f"./data/transformed/{filename}"
        df.to_csv(path, index=False)
        print(f"Arquivo salvo com sucesso em: {path}")
    except (IOError, OSError) as e:
        print(f"Erro ao salvar arquivo CSV: {e}")


if __name__ == "__main__":
    pass
