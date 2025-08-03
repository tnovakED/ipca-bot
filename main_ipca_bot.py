import pandas as pd
import requests
import numpy as np
import os
import time

# Contador de tempo para medir o desempenho da pipeline
start_process = time.time()

# caminho do arquivo diretamente da tabela 1737 do IBGE
URL_IPCA = "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1737.xlsx&terr=N&rank=-&query=t/1737/n1/all/v/all/p/all/d/v63%202,v69%202,v2263%202,v2264%202,v2265%202,v2266%2013/l/,v,t%2Bp"

#Caminho das pastas da aquitetura medalhao
caminho_bronze = r"D:\GitGithub\ipca-bot\bronze\ipca_tabela1737_download.xlsx"
caminho_silver = r'D:\GitGithub\ipca-bot\silver'
caminho_gold = r'D:\GitGithub\ipca-bot\gold'


# Funções Utilizadas
def baixar_arquivo(url: str, caminho_saida: str):

#   Faz o download de um arquivo a partir de uma URL e salva localmente no caminho especificado.
    response = requests.get(url)
    response.raise_for_status()  # Garante que houve sucesso no download
    with open(caminho_saida, "wb") as arquivo:
        arquivo.write(response.content)
    print(f"Arquivo baixado e salvo em: {caminho_saida}")

def carregar_dados_excel(caminho):
 '''
   Carrega os dados do arquivo Excel, descartando as 3 linhas iniciais que não contêm dados úteis.
   Ajusta colunas removendo a primeira coluna com dados nulos.
 '''
 df = pd.read_excel(caminho, skiprows = 3)
    # Remove a primeira coluna que é índice vazio
    df = df.iloc[:, 1:]
    # adiciono o cabeçalho das colunas com nomes padronizados
    df.columns = ['mes','ipca-indice', 'variacao_mensal', 'acumulada_3_meses', 
                  'acumulada_6_meses', 'acumulada_do_ano', 'acumulada_12_meses']
    return df

def formatar_colunas_numericas(df, colunas):
    '''Verifica as colunas com valores vazios, atribui NaN e converte os tipos de dados'''
    for col in colunas:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace('...', np.nan)
            .str.replace(',', '.', regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def salvar_parquet(df, caminho_saida) -> None:
    ''' Salva em formato Parquet sem o indice '''
    df.to_parquet(caminho_saida, index=False)
    print(f"Arquivo salvo em formato parquet: {caminho_saida}")



def separa_mes_ano(df):
    '''Separa as coluna mes e ano para analises futuras reordena o dataframe com a coluna 'ano' como a primeira'''
    df[['mes', 'ano']] = df['mes'].str.split(' ', expand=True)
    
    # Reordena as colunas
    colunas = ['ano'] + [col for col in df.columns if col != 'ano']
    df = df[colunas]
    return df

def criar_timestamp(df):
    '''Cria uma coluna timestamp para o momento do processamento'''
    df['data_process'] = pd.Timestamp.now()
    return df

def camada_gold(caminho_silver):
    df = pd.read_parquet(caminho_silver)
    gold_df = df.groupby('ano').agg({
        'ipca-indice': 'mean',
        'variacao_mensal': 'mean',
        'acumulada_12_meses': 'mean'
    }).reset_index()
    
    gold_df = gold_df.round(4)
    
    return gold_df

def drop_linha_nulas(df, coluna):
    '''remove linhas onde o valor seja nulo'''
    linha_nan = df[df[coluna].isna()]
    if not linha_nan.empty:
        print(f"Linhas com NaN encontradas:{linha_nan}")
    
    return df.dropna(subset=[coluna]).reset_index(drop=True)

#--------------------------
# Funções da Pipeline
#--------------------------

def main():
    
    #------BRONZE-----

    # Passo_1: Baixa o arquivo bruto da fonte
    baixar_arquivo(URL_IPCA, caminho_bronze)
    
    #----------------SILVER--------------
    
    # Passo_2: Carregar os dados no DataFrame
    df = carregar_dados_excel(caminho_bronze)
    
    #adiciona a coluna timestamp para o controle de processamento.
    df = criar_timestamp(df)
    
    
    # Passo_3: Formatar colunas numéricas para o tipo correto
    colunas_numericas = [
        'ipca-indice',
        'variacao_mensal',
        'acumulada_3_meses',
        'acumulada_6_meses',
        'acumulada_do_ano',
        'acumulada_12_meses'
    ]
    df = formatar_colunas_numericas(df, colunas_numericas)
    
    #Separa a coluna 'mes' e 'ano'
    df = separa_mes_ano(df)
    
    #Passo 4: Exibir amostra e tipos para conferência, usado em ambiente de testes
    #print(df.head(30))
    #print(df.dtypes)

    df = drop_linha_nulas(df, 'ano')
    
    # Passo 5: Salva os dados tratados em formato parquet na camada Silver
    salvar_parquet(df, os.path.join(caminho_silver, "ipca_tabela1737_silver.parquet"))

    

#---------------------GOLD----------------------
    #Agrega dados anuais e faz medias relevantes.
    caminhosilver = os.path.join(caminho_silver, 'ipca_tabela1737_silver.parquet')
    df_gold = camada_gold(caminhosilver)
    
    #Adiciona timestamp para controle de processamento na GOLD
    df_gold = criar_timestamp(df_gold)
    salvar_parquet(df_gold, os.path.join(caminho_gold, 'AVG_ipca_tabela1737_gold.parquet'))
    
    # log de finalizacao
    print("Quantidade de linhas processadas:", len(df))
    
    #tempo total de execução da pipeline
    end_process = time.time()
    print(f'O tempo de execução: {end_process - start_process:.2f} segundos')
    
if __name__ == "__main__":
    main()
