def ler_arquivo_linha_por_linha(caminho_arquivo):
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
            for linha in arquivo:
                print(linha.strip())  # Use strip() para remover caracteres de nova linha
    except FileNotFoundError:
        print(f"O arquivo {caminho_arquivo} não foi encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")

# Use a função com o caminho do seu arquivo
ler_arquivo_linha_por_linha('C:\\Users\\paull\\OneDrive\\Área de Trabalho\\ExtracaoTexto\\Downloads\\Transcrições-Txts\\audios_01-20240713T123313Z-001\\audios_01\\1677693753.3612.txt')
