Desafio - Bot IPCA

Esse é um projeto de um bot para uma pipeline de dados sobre o IPCA-IBGE com aplicação da arquitetura medalhão (Bronze, Silver, Gold)

Desenvolvimento de um bot que acessa um link e extrai os dados do arquivo e transforma os dados de forma tabular.

Justificativa Técnica

O link disponibilizado no desafio apresenta um formato JSON que traz apenas os metadados para consulta.
Ao analisar os metadados, verifiquei um link do IBGE e acessei ele, pesquisei e achei o link da tabela apresentada nos metadados no JSON.
Encontrei o link de API para download dos dados reais para o desafio.

Para completar o desafio:
- Identifiquei a tabela 1737, encontrei o link para download em formato '.xls'.
- Realizei o tratamento necessário dos dados apresentados com Pandas
- Salvei em formato Parquet
- Implementei a arquitetura medalhão com as camadas Bronze, Silver, Gold.

Estrutura do Projeto

Bronze

Download da Tabela em '.xls' para essa pasta.
"Poderia ser salvo em parquet, mas como haveria necessidade de fazer o mesmo processo feito na camada silver, optei por deixar ele bruto com o formato original."

Silver

Algumas transformações realizadas:
	Padronização de tipo de dados, nome de colunas.
	Criação de uma coluna de monitoramento - timestamp.
	Salvar o dataframe em arquivo Parquet nessa pasta.

Gold

Agregações feitas:

	- Média da variação mensal('variacao_mensal')
	- Media o IPCA ('ipca-indice')
	- Media do acumulado de 12 meses('acumulada_12_meses')
	- Salvar os dados em arquivo Parquet nessa pasta. 

Permitir análises de tendências anuais do IPCA.
