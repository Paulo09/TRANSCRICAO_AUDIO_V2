import pyarrow.parquet as pq

def find_information_in_parquet(file_path, column_name, search_value):
    # Carregar o arquivo parquet
    table = pq.read_table(file_path)
    
    # Criar um dataframe a partir da tabela
    df = table.to_pandas()
    
    # Procurar a informação na coluna especificada
    result = df[df[column_name] == search_value]
    
    return result

file_path = "C:\\Users\\paull\\OneDrive\Área de Trabalho\\ExtracaoTexto\\atendimento\\atendimento\\dados\\sigepe.parquet"
column_name = "cd_atn"
search_value = "2023010100001.0"

result = find_information_in_parquet(file_path, column_name, search_value)
print(result)
