""" 
    O módulo main é o script de entrada do programa. 
    Ele será responsável por chamar as funções de extração e transformação dos dados
    e por carregá-los no destino final, que pode ser um banco de dados ou um arquivo CSV
"""

from extract import fetch_sheets
from transform import transform
from load import save_to_csv


if __name__ == '__main__':
    dfs = fetch_sheets()
    df = transform(dfs)
    save_to_csv(df)
