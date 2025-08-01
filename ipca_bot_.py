import requests
import pandas as pd
from io import BytesIO

def baixar_arquivo(url):
    """
    Faz o download do arquivo da URL e retorna o conteúdo em bytes.
    """
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.content
    except requests.RequestException as e:
        print(f"Erro ao baixar arquivo: {e}")
        return None

def inspecionar_planilha(bytes_content):
    """
    Lê as primeiras linhas do Excel para inspeção dos dados.
    """
    excel_io = BytesIO(bytes_content)
    # header=6: usa a linha 7 do arquivo como cabeçalho (índice 6 zero-based)
    df = pd.read_excel(excel_io, engine='openpyxl', header=6, nrows=10)
    print("Exemplo de linhas da planilha:")
    print(df)
    print("\nColunas originais:")
    print(df.columns.tolist())
    return df

def tratar_planilha(df):
    """
    Trata o DataFrame para extrair Ano, Mês e Valor.
    """
    # Renomear coluna de mês+ano para 'MesAno'
    df = df.rename(columns={df.columns[1]: 'MesAno'})

    # Separar Mes e Ano
    df[['Mes', 'Ano']] = df['MesAno'].str.split(' ', expand=True)

    # Pega a penúltima coluna (valor numérico do IPCA)
    valor_col = df.columns[-2]
    df['Valor'] = pd.to_numeric(df[valor_col], errors='coerce')

    # Selecionar só as colunas que interessam
    df = df[['Ano', 'Mes', 'Valor']]

    # Converter Ano para inteiro, ignorando erros
    df['Ano'] = pd.to_numeric(df['Ano'], errors='coerce').astype('Int64')

    # Remover linhas onde Ano ou Valor sejam nulos
    df = df.dropna(subset=['Ano', 'Valor']).reset_index(drop=True)

    return df

def salvar_em_parquet(df, caminho_arquivo):
    """
    Salva o DataFrame em arquivo Parquet.
    """
    try:
        df.to_parquet(caminho_arquivo, index=False)
        print(f"Arquivo salvo em: {caminho_arquivo}")
    except Exception as e:
        print(f"Erro ao salvar arquivo parquet: {e}")

def main():
    url = 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1737.xlsx&terr=NCS&rank=-&query=t/1737/n1/all/v/all/p/all/d/v63%202,v69%202,v2263%202,v2264%202,v2265%202,v2266%2013/l/,,v%2Bp%2Bt&measurecol=true'

    print("Baixando arquivo XLSX do IPCA...")
    bytes_xlsx = baixar_arquivo(url)
    if bytes_xlsx is None:
        print("Abortando por erro no download.")
        return

    # Inspeciona para garantir que está lendo certo
    df_inspecao = inspecionar_planilha(bytes_xlsx)

    # Lê o arquivo completo, com o cabeçalho correto
    excel_io = BytesIO(bytes_xlsx)
    df_raw = pd.read_excel(excel_io, engine='openpyxl', header=6)

    print("\nColunas após leitura completa:")
    print(df_raw.columns.tolist())

    # Tratar os dados para extrair ano, mês e valor
    df_tratado = tratar_planilha(df_raw)

    print("\nDados tratados (exemplo):")
    print(df_tratado.head())

    # Salvar em parquet
    salvar_em_parquet(df_tratado, 'ipca_tabela1737_tratado.parquet')

    print("Processo finalizado!")

if __name__ == "__main__":
    main()


'''
import requests
import pandas as pd
from io import BytesIO

def baixar_arquivo(url):
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.content
    except requests.RequestException as e:
        print(f"Erro ao baixar arquivo: {e}")
        return None

def inspecionar_planilha(bytes_content):
    excel_io = BytesIO(bytes_content)
    df = pd.read_excel(excel_io, engine='openpyxl', skiprows=6, nrows=10)
    print("Exemplo de linhas da planilha:")
    print(df)
    print("\nColunas originais:")
    print(df.columns.tolist())
    return df

def tratar_planilha(df):
    # Remover linhas onde a coluna do mês+ano (segunda coluna) está vazia ou nula
    df = df[df[df.columns[1]].notna()]

    # Renomear coluna do mês+ano
    df = df.rename(columns={df.columns[1]: 'MesAno'})

    # Extrair mês e ano
    df[['Mes', 'Ano']] = df['MesAno'].str.split(' ', expand=True)

    # Identificar coluna do valor IPCA: primeira coluna numérica plausível após a 3ª coluna
    valor_col = None
    for col in df.columns[2:]:
        if pd.api.types.is_numeric_dtype(df[col]):
            sample_vals = df[col].dropna().head(5)
            if all((sample_vals > 0.1) & (sample_vals < 1000)):
                valor_col = col
                break

    if valor_col is None:
        raise ValueError("Não foi possível identificar a coluna de valores IPCA.")

    df['Valor'] = pd.to_numeric(df[valor_col], errors='coerce')

    # Selecionar colunas importantes
    df = df[['Ano', 'Mes', 'Valor']]

    # Converter ano para int
    df['Ano'] = pd.to_numeric(df['Ano'], errors='coerce').astype('Int64')

    # Remover linhas inválidas
    df = df.dropna(subset=['Ano', 'Valor']).reset_index(drop=True)

    return df

def salvar_em_parquet(df, caminho_arquivo):
    try:
        df.to_parquet(caminho_arquivo, index=False)
        print(f"Arquivo salvo em: {caminho_arquivo}")
    except Exception as e:
        print(f"Erro ao salvar arquivo parquet: {e}")

def main():
    url = 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1737.xlsx&terr=NCS&rank=-&query=t/1737/n1/all/v/all/p/all/d/v63%202,v69%202,v2263%202,v2264%202,v2265%202,v2266%2013/l/,,v%2Bp%2Bt&measurecol=true'

    print("Baixando arquivo XLSX do IPCA...")
    bytes_xlsx = baixar_arquivo(url)
    if bytes_xlsx is None:
        print("Abortando por erro no download.")
        return

    df_raw = inspecionar_planilha(bytes_xlsx)
    print("\nColunas após leitura completa:")
    print(df_raw.columns.tolist())

    df_tratado = tratar_planilha(df_raw)
    print("\nDados tratados (exemplo):")
    print(df_tratado.head())

    salvar_em_parquet(df_tratado, 'ipca_tabela1737_tratado.parquet')
    print("Processo finalizado!")

if __name__ == "__main__":
    main()

'''
