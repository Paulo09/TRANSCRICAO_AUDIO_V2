import re
import signal

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException

#FONTES_TRANSCRICOES = "C:\\Users\\paull\\OneDrive\\Área de Trabalho\\ExtracaoTexto\\Downloads\\Transcrições-Txts\\audios_01-20240713T123313Z-001\\audios_01"
FONTES_TRANSCRICOES = "C:\\Users\paull\\OneDrive\\Área de Trabalho\\ExtracaoTexto\\Downloads\\Transcrições-Txts\Testes"
def extrair_numeros_protocolo(diretorio = FONTES_TRANSCRICOES):
    resultados = []
    tempo_limite = 60
    # Expressão regular para encontrar os termos seguidos por um número de 13 dígitos
    padrao = re.compile(r'(protocolo|número do protocolo|atendimento|número do atendimento)\D*((?:\D*\d\D*){13})', re.IGNORECASE)
    # Percorre todos os arquivos no diretório

    signal.signal(signal.SIGALRM, timeout_handler)

    for nome_arquivo in tqdm(os.listdir(diretorio)):
        print(f"Arquivo {nome_arquivo} .....", end='')
        if nome_arquivo.endswith('.txt'):
            caminho_arquivo = os.path.join(diretorio, nome_arquivo)

            try:
                # Define o alarme para o tempo limite
                signal.alarm(tempo_limite)
                with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
                    conteudo = arquivo.read()              
                    # Busca por todos os padrões encontrados no conteúdo do arquivo
                    matches = padrao.findall(conteudo)
                    # Extrai e imprime os números de 13 dígitos encontrados
                    for match in matches:
                        numero_protocolo = re.sub(r'\D', '', match[1])  # Remove caracteres não numéricos
                        resultados.append((nome_arquivo,numero_protocolo))
            except TimeoutException:
                print(f'Tempo limite excedido para o arquivo: {nome_arquivo}')
            except Exception as e:
                print(f'Erro ao processar o arquivo {nome_arquivo}: {e}')
            finally:
                # Garante que o alarme seja desativado no final do bloco try
                signal.alarm(0)
        print("ok")
    return resultados

extrair_numeros_protocolo();