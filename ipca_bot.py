import requests
import pandas as pd
import pyarrow
import fastparquet
import json

def obter_dados_ipca(url):
    "obtem os dados da api sidra ibge em formato JSON"
    
    resposta = requests.get(url)
    if resposta.status_code == 200:
        return resposta.json()
    else:
        raise Exception(f"Erro no acesso da URL. Codido:{resposta.status_code}")

'''
def transformar_dados(json_data):
    "transforma os dados JSON em Tabular"
    
    dados = json_data['Valores']
    
    for item in dados:
        linha = {
            'Ano': item['D1C'],
            'Mês': item['D2C'],
            'Localidade': item['D4N'],
            'Valor': item['V']
        }
        registros.append(linha)

    df = pd.DataFrame(registros)
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'].str.replace(',','.'), errors='coerce')
    return df
'''

def transformar_dados(json_data):
    print("transformando dados...")
    try:
        serie = json_data['resultados'][0]['series'][0]['serie']
    except (KeyError, IndexError, TypeError) as e:
        print("Erro ao acessar os dados da série:", e)
        return pd.DataFrame()

    df = pd.DataFrame(serie.items(), columns=["Data", "IPCA"])
    df["Data"] = pd.to_datetime(df["Data"])
    df["IPCA"] = pd.to_numeric(df["IPCA"], errors="coerce")
    return df



def salvar_em_parquet(df, caminho_arquivo):
    
    df.to_parquet(caminho_arquivo, index=False)
    print(f"Arquivo salvo em:{caminho_arquivo}")
'''   
def main():
    url = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    print("Obtendo os dados...")
    json_data = obter_dados_ipca(url)
    
    #print('Conteudo recebido')
    #print(json_data)
    
    print("transdormando dados...")
    df = transformar_dados(json_data)
    
    salvar_em_parquet(df, 'ipca_tabela1337.parquet')
    
    print("Processo concluido")
    
import json
'''
def main():
    url = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    print("Obtendo os dados...")
    json_data = obter_dados_ipca(url)

    print("Exibindo estrutura de alto nível do JSON:")
    print(json.dumps(json_data, indent=2, ensure_ascii=False)[:1000])  # mostra os primeiros 1000 caracteres

    print("transformando dados...")
    df = transformar_dados(json_data)

    if not df.empty:
        salvar_parquet(df)
    else:
        print("DataFrame está vazio. Nenhum arquivo foi salvo.")

    
if __name__ == '__main__':
    main()