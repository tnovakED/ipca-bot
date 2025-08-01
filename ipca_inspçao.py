import pandas as pd
import requests
import numpy as np

# URL do arquivo Excel com dados do IPCA
URL_IPCA = "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1737.xlsx&terr=N&rank=-&query=t/1737/n1/all/v/all/p/all/d/v63%202,v69%202,v2263%202,v2264%202,v2265%202,v2266%2013/l/,v,t%2Bp"

# Caminho do arquivo local para salvar o Excel baixado
OUTPUT_PATH = "ipca_tabela1737_download.xlsx"

def baixar_arquivo(url: str, caminho_saida: str) -> None:
    """
    Baixa um arquivo a partir de uma URL e salva localmente.
    """
    response = requests.get(url)
    response.raise_for_status()  # Garante que houve sucesso no download
    with open(caminho_saida, "wb") as f:
        f.write(response.content)
    print(f"Arquivo baixado e salvo em: {caminho_saida}")

def carregar_dados_excel(caminho: str, linhas_descartar: int = 3) -> pd.DataFrame:
    """
    Carrega os dados do arquivo Excel, descartando linhas iniciais que não contêm dados úteis.
    Ajusta colunas removendo a primeira coluna com dados nulos.
    """
    df = pd.read_excel(caminho, skiprows=linhas_descartar)
    # Remove a primeira coluna que é índice vazio
    df = df.iloc[:, 1:]
    # Renomeia colunas para nomes padronizados
    df.columns = ['mes','ipca-indice', 'variacao_mensal', 'acumulada_3_meses', 
                  'acumulada_6_meses', 'acumulada_do_ano', 'acumulada_12_meses']
    return df

def formatar_colunas_numericas(df: pd.DataFrame, colunas: list) -> pd.DataFrame:
    """
    Formata colunas numéricas:
    - Substitui '...' por NaN
    - Troca vírgula por ponto para números decimais
    - Converte para tipo numérico, forçando NaN onde não for possível converter
    """
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

def salvar_parquet(df: pd.DataFrame, caminho_saida: str) -> None:
    """
    Salva o DataFrame em arquivo no formato Parquet.
    """
    df.to_parquet(caminho_saida, index=False)
    print(f"Arquivo salvo em formato parquet: {caminho_saida}")

def main():
    # Passo 1: Baixar o arquivo Excel
    baixar_arquivo(URL_IPCA, OUTPUT_PATH)
    
    # Passo 2: Carregar os dados no DataFrame
    df = carregar_dados_excel(OUTPUT_PATH, linhas_descartar=3)
    
    # Passo 3: Formatar colunas numéricas para o tipo correto
    colunas_numericas = [
        'ipca-indice',
        'variacao_mensal',
        'acumulada_3_meses',
        'acumulada_6_meses',
        'acumulada_do_ano',
        'acumulada_12_meses'
    ]
    df = formatar_colunas_numericas(df, colunas_numericas)
    
    # Passo 4: Exibir amostra e tipos para conferência
    print(df.head(10))
    print(df.dtypes)
    
    # Passo 5: Salvar os dados tratados em formato parquet
    salvar_parquet(df, "ipca_tabela1737.parquet")

if __name__ == "__main__":
    main()
