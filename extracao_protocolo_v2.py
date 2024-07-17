import re
import os
import threading
from tqdm import tqdm

class TimeoutException(Exception):
    pass

def extrair_numeros_protocolo(diretorio):
    resultados = []
    tempo_limite = 10
    padrao = re.compile(r'(protocolo|número do protocolo|atendimento|número do atendimento)\D*((?:\D*\d\D*){13})', re.IGNORECASE)
    
    def ler_arquivo(caminho_arquivo):
        with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
            return arquivo.read()

    for nome_arquivo in tqdm(os.listdir(diretorio)):
        print(f"Arquivo {nome_arquivo} .....", end='')
        if nome_arquivo.endswith('.txt'):
            caminho_arquivo = os.path.join(diretorio, nome_arquivo)
            conteudo = None

            def temporizador():
                raise TimeoutException

            timer = threading.Timer(tempo_limite, temporizador)
            timer.start()

            try:
                conteudo = ler_arquivo(caminho_arquivo)
                timer.cancel()
            except TimeoutException:
                print(f'Tempo limite excedido para o arquivo: {nome_arquivo}')
            except Exception as e:
                print(f'Erro ao processar o arquivo {nome_arquivo}: {e}')
            finally:
                timer.cancel()

            if conteudo:
                matches = padrao.findall(conteudo)
                for match in matches:
                    numero_protocolo = re.sub(r'\D', '', match[1])
                    resultados.append((nome_arquivo, numero_protocolo))
        print("ok")
    return resultados

# Exemplo de uso
diretorio = 'C:\\Users\\paull\\OneDrive\\Área de Trabalho\\ExtracaoTexto\Downloads\\Transcrições-Txts\\Testes'
resultados = extrair_numeros_protocolo(diretorio)
print(resultados)