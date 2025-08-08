import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import time

# abrir o navegador
navegador = webdriver.Chrome()

site = "https://sidra.ibge.gov.br/pesquisa/snipc/ipca"

navegador.get(site)
navegador.maximize_window()


#selecionar um elemento na tela
aba_tabela = WebDriverWait(navegador, 10).until(
    EC.presence_of_element_located((By.XPATH, '//a[@data-pagina="tabelas" and text()="Tabelas"]'))
)
navegador.execute_script("arguments[0].click();", aba_tabela)

#clica e e abre a tabela 1737
find_tabela = WebDriverWait(navegador, 10).until(
    EC.presence_of_element_located((By.XPATH, '//a[@title="Abrir Tabela" and text()="1737"]')) 
)
navegador.execute_script("arguments[0].click();", find_tabela)

#encontra os botoes para marcar todas opçoes
botoes = navegador.find_elements(By.CSS_SELECTOR, 'button[data-cmd="marcarTudo"]')

# encontra o botao de gerar o tela de opçoes de download
botao_link = WebDriverWait(navegador, 15).until(
    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-cmd="gerar-link"]'))
) 

botao_visu_donwload = WebDriverWait(navegador, 15).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-target="#opcoes-extras-download"]' ))
)


#print(f"Encontrei {len(botoes)} botões")



# clicar no primeiro
navegador.execute_script("arguments[0].scrollIntoView();", botoes[0])
navegador.execute_script("arguments[0].click();", botoes[0])

# clicar no segundo
navegador.execute_script("arguments[0].scrollIntoView();", botoes[1])
navegador.execute_script("arguments[0].click();", botoes[1])

# clicar botao de LINKs
navegador.execute_script("arguments[0].scrollIntoView();", botao_link)
navegador.execute_script("arguments[0].click();", botao_link)

navegador.execute_script("arguments[0].click();", botao_visu_donwload)

# seleciona o formato do download
seleciona_formato = WebDriverWait(navegador, 15).until(
        EC.presence_of_element_located((By.NAME, "formato-arquivo"))
    )
WebDriverWait(navegador, 10).until(
    EC.element_to_be_clickable((By.NAME, "formato-arquivo"))
)
select = Select(seleciona_formato)
select.select_by_value("br.csv")


#copia a url do associada ao botao
copia_url = WebDriverWait(navegador, 10).until(
    EC.presence_of_all_elements_located(
        (By.CSS_SELECTOR, 'span.input-group-btn > a.btn.btn-default')
    )
)

if len(copia_url) >= 3:
    href_3 = copia_url[2].get_attribute('href')
    print(href_3)
else:
    print(f'esperava 3 elementos mas encontrie {len(copia_url)}')

with open ('url_tabela_1737', 'w', encoding='utf-8') as arquivo:
    arquivo.write(href_3)
#href = copia_url[2].get_attribute('href')

#print("URL do Site para download: ", href)
#seletor = Select(seleciona_formato)
#seletor.select_by_visible_text("br.csv")

#navegador.execute_script("arguments[0].click();", opcao_de_formato[lista_formatos[0]])




