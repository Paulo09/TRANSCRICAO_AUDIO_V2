import re
import os
import pandas as pd

import threading
from tqdm import tqdm


def ler_transcricoes(diretorio):
    transcricoes = []
    for filename in os.listdir(diretorio):
        if filename.endswith(".txt"):
            with open(os.path.join(diretorio, filename), 'r', encoding='utf-8') as file:
                transcricoes.append(file.read())
    return transcricoes

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


def pesquisar_modelos_de_atendimento(protocolos, parquet_path):
    df_parquet = pd.read_parquet(parquet_path)
    modelos_de_atendimento = {}
    
    for protocolo in protocolos:
        resultado = df_parquet[df_parquet['cd_atn'] == protocolo]

        if not resultado.empty:
            modelos_de_atendimento[protocolo] = resultado['modelo_atendimento'].values[0]   
    return modelos_de_atendimento


def automatizar_processo(diretorio_transcricoes, parquet_path):
    transcricoes = ler_transcricoes(diretorio_transcricoes)
    
    protocolos = extrair_numeros_protocolo(diretorio_transcricoes)
    modelos_de_atendimento = pesquisar_modelos_de_atendimento(list(map(lambda x: x[1], protocolos)), parquet_path)
    
    return modelos_de_atendimento

# Exemplo de uso
# diretorio_transcricoes = 'caminho/para/diretorio/de/transcricoes'
# parquet_path = 'caminho/para/arquivo/parquet.parquet'

diretorio_transcricoes = 'C:\\Users\\paull\\OneDrive\\Área de Trabalho\\ExtracaoTexto\\Downloads\\Transcrições-Txts\\Testes'
parquet_path = 'C:\\Users\\paull\\OneDrive\Área de Trabalho\\ExtracaoTexto\\atendimento\\atendimento\\dados\\sigepe.parquet'

resultados = automatizar_processo(diretorio_transcricoes, parquet_path)
print(resultados)