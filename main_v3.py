import re
import os
import polars as pl

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
    tempo_limite = 5
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

#
# função para encontrar o nome do modelo de atendimento relativo a um protocolo no dump do sisgep
#
def localizar_nm_mda(protocolo, sisgep_df):
    # Filtrar linhas onde a coluna especificada contém a substring fornecida
    linha = sisgep_df.filter(pl.col('nr_prt_spu_atn') == protocolo)
    if len(linha) == 0:
        linha = sisgep_df.filter(pl.col('cd_atn') == protocolo)
        if len(linha) == 0:
            return -1
    return linha["nm_mda"].item()

def pesquisar_modelos_de_atendimento(protocolos, parquet_path):
    sisgep_df = pl.read_parquet(parquet_path)
    sisgep_df = sisgep_df.with_columns(pl.col('cd_atn').cast(pl.Utf8))
    sisgep_df = sisgep_df.with_columns(pl.col('nr_prt_spu_atn').cast(pl.Utf8))
    modelos_de_atendimento = {}
    
    for protocolo in protocolos:
        print(protocolo)
        nm_mda = localizar_nm_mda(protocolo, sisgep_df)
        if nm_mda != -1:
            modelos_de_atendimento[protocolo] = nm_mda
    return modelos_de_atendimento
#%%
def automatizar_processo(diretorio_transcricoes, parquet_path):
    #transcricoes = ler_transcricoes(diretorio_transcricoes)
    arquivos_protocolos = extrair_numeros_protocolo(diretorio_transcricoes)
    modelos_de_atendimento = pesquisar_modelos_de_atendimento(list(map(lambda x: x[1], arquivos_protocolos)), parquet_path)
    massa_testes = []
    for protocolo in modelos_de_atendimento:
        transcricao = list(filter(lambda x: x[1] == protocolo, arquivos_protocolos))[0]
        texto_transcricao = ""
        with open(f"{diretorio_transcricoes}/{transcricao[0]}", 'r', encoding='utf-8') as arquivo:
            texto_transcricao = arquivo.read()
        massa_testes.append((texto_transcricao, modelos_de_atendimento[protocolo]))
    #massa_testes = pl.DataFrame(modelos_de_atendimento.values,
                  #schema=["protocolo", "mda"])
    return massa_testes
#%%

# Exemplo de uso
# diretorio_transcricoes = 'caminho/para/diretorio/de/transcricoes'
# parquet_path = 'caminho/para/arquivo/parquet.parquet'

diretorio_transcricoes = 'teste_audios_2'
parquet_path = 'sigepe.parquet'

resultados = automatizar_processo(diretorio_transcricoes, parquet_path)
print(resultados)