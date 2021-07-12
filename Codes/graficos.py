import psycopg2
import datetime
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

#Conectar ao banco de dados
db_user = 'postgres'
db_password = 'admin'
conn = psycopg2.connect("dbname=grupo_m user="+db_user+" password="+db_password)
cur = conn.cursor()

#Retorna o numero de casos por dia
#Primeiramente, retorna o id de cada cidade
#Porto Alegre
cur.execute('SELECT * FROM city WHERE name LIKE \'Porto Alegre\'')
id_porto_alegre = cur.fetchone()[0]

#Londrina
cur.execute('SELECT * FROM city WHERE name LIKE \'Londrina\'')
id_londrina = cur.fetchone()[0]

#São Carlos
cur.execute('SELECT * FROM city WHERE name LIKE \'São Carlos\'')
id_sao_carlos = cur.fetchone()[0]

#Pato Branco
cur.execute('SELECT * FROM city WHERE name LIKE \'Pato Branco\'')
id_pato_branco = cur.fetchone()[0]

#Santo Antônio da Platina
cur.execute('SELECT * FROM city WHERE name LIKE \'Santo Antônio da Platina\'')
id_santo_antonio = cur.fetchone()[0]

#Cria Array para armazenar os ids das cidades
array_cidade_id = []
array_cidade_id.append(id_porto_alegre)
array_cidade_id.append(id_londrina)
array_cidade_id.append(id_sao_carlos)
array_cidade_id.append(id_pato_branco)
array_cidade_id.append(id_santo_antonio)

# Cria array receber os dados da query da base de dados
query_result = []

#Realiza a consulta para retornar o numero de casos por dia
for cidade_id in array_cidade_id:
    cur.execute('''
        SELECT date_covid_data, new_confirmed 
        FROM covid_data
        WHERE city_id = '''+str(cidade_id)+'''
        ORDER BY date_covid_data;
        ''')
    resultado = cur.fetchall()
    query_result.append(resultado) #Cada cidade tem seus resultados armazenados em um indice do array
    
#Plot dos graficos de numero de casos por dia de cada cidade
dados_eixo_x = [] #Array para armazenar os dados do eixo x (no caso, ira armazenar as datas)
dados_eixo_y = [] #Arrat para armazenar os dados do eixo y (no caso, ira armazenar os casos por dia)

#Porto Alegre
for register in query_result[0]: #Percorre todos os registros da cidade
    dados_eixo_x.append(register[0]) #Indice 0 do registro é a data
    dados_eixo_y.append(register[1]) #Indice 1 do registro é o numero de casos por dia
plt.bar(dados_eixo_x, dados_eixo_y)
dados_eixo_x = []
dados_eixo_y = []

#Londrina
for register in  query_result[1]:
    dados_eixo_x.append(register[0])
    dados_eixo_y.append(register[1])
dados_eixo_x = []
dados_eixo_y = []
plt.bar(dados_eixo_x, dados_eixo_y)

#São Carlos
for register in  query_result[2]:
    dados_eixo_x.append(register[0])
    dados_eixo_y.append(register[1])
dados_eixo_x = []
dados_eixo_y = []
plt.bar(dados_eixo_x, dados_eixo_y)

#Pato Branco
for register in  query_result[3]:
    dados_eixo_x.append(register[0])
    dados_eixo_y.append(register[1])
dados_eixo_x = []
dados_eixo_y = []
plt.bar(dados_eixo_x, dados_eixo_y)

#Santo Antônio da Platina
for register in  query_result[3]:
    dados_eixo_x.append(register[0])
    dados_eixo_y.append(register[1])
plt.bar(dados_eixo_x, dados_eixo_y)

#Plota os graficos
plt.show()

#Apos obter os dados de casos por dia de cada cidade, cria uma consulta
#obtendo informacao de ultimos casos confirmados, ultimos casos por 100 mil habitantes,
#ultimas mortes confirmadas e ultima taxa de morte confirmada de cada uma das cidades
#As semanas escolhidas foram as semanas de 26 a 29

query_result = [] #Limpa o array para armazenar os resultados do banco de dados

#Realiza a consulta para retornar as informacoes de cada cidade
for cidade_id in array_cidade_id:
    cur.execute('''
        SELECT
        DISTINCT ON (cd.epidemiological_week)
        cd.id AS "ID",
        cd.epidemiological_week AS "Semana Epidemiologica",
        c.name AS "Cidade",
        cd.last_available_confirmed AS "Ultimos Casos Confirmados",
        cd.last_available_confirmed_per_100k_inhabitants AS "Ultimos Casos por 100 Mil Habitantes",
        cd.last_available_deaths AS "Ultimas Mortes Confirmadas",
        cd.last_available_death_rate AS "Ultima Taxa de Morte Confirmada"
        FROM covid_data cd
        INNER JOIN city c ON c.id = cd.city_id
        WHERE cd.epidemiological_week BETWEEN 26 AND 29 
        AND cd.city_id = '''+str(cidade_id)+'''
        ORDER BY cd.epidemiological_week, cd.last_available_confirmed desc;
        ''')
    for resultado in cur:
        query_result.append(resultado) #Armazenados em um indice do array
        
# Cria Dataframe para conter os dados sobre covid
df_covid = pd.DataFrame(query_result, columns=[
    'ID',
    'Semana Epidemiologica',
    'Cidade',
    'Ultimos Casos Confirmados',
    'Ultimos Casos por 100 Mil Habitantes',
    'Ultimas Mortes Confirmadas',
    'Ultima Taxa de Morte Confirmada'
    ])

# Realiza os tratamentos necessários para construção dos gráficos

##### GRÁFICO DE CASOS CONFIRMADOS #####
# Cria Dataframe de Últimos Casos Confirmados
df_ultimos_casos_confirmados = df_covid.loc[:, ['ID', 'Cidade', 'Semana Epidemiologica', 'Ultimos Casos Confirmados']]

# Aplica filtros de cidades e retorna valores em array para serem usados nos gráficos
df_ultimos_casos_confirmados_porto_alegre = df_ultimos_casos_confirmados[
    df_ultimos_casos_confirmados['Cidade'] == 'Porto Alegre']

df_ultimos_casos_confirmados_londrina = df_ultimos_casos_confirmados[
    df_ultimos_casos_confirmados['Cidade'] == 'Londrina']

df_ultimos_casos_confirmados_pato_branco = df_ultimos_casos_confirmados[
    df_ultimos_casos_confirmados['Cidade'] == 'Pato Branco']

df_ultimos_casos_confirmados_sao_carlos = df_ultimos_casos_confirmados[
    df_ultimos_casos_confirmados['Cidade'] == 'São Carlos']

df_ultimos_casos_confirmados_santo_antonio_da_platina = df_ultimos_casos_confirmados[
    df_ultimos_casos_confirmados['Cidade'] == 'Santo Antônio da Platina']

# Cria listas contendo os valores de casos confirmados de cada cidade, para serem usadas nos gráficos
lista_confirmados_porto_alegre = df_ultimos_casos_confirmados_porto_alegre['Ultimos Casos Confirmados'].to_list()
lista_confirmados_londrina = df_ultimos_casos_confirmados_londrina['Ultimos Casos Confirmados'].to_list()
lista_confirmados_pato_branco = df_ultimos_casos_confirmados_pato_branco['Ultimos Casos Confirmados'].to_list()
lista_confirmados_sao_carlos = df_ultimos_casos_confirmados_sao_carlos['Ultimos Casos Confirmados'].to_list()
lista_confirmados_santo_antonio_da_platina = df_ultimos_casos_confirmados_santo_antonio_da_platina['Ultimos Casos Confirmados'].to_list()

# Define a largura das barras
bar_width = 0.15

# Aumenta o gráfico
plt.figure(figsize=(15,7.5))

# Definindo as posições das barras
barra1 = np.arange(len(lista_confirmados_porto_alegre))
barra2 = [x + bar_width for x in barra1]
barra3 = [x + bar_width for x in barra2]
barra4 = [x + bar_width for x in barra3]
barra5 = [x + bar_width for x in barra4]

# Criando as barras
plt.bar(barra1, lista_confirmados_porto_alegre, color='red', width=bar_width, label='Porto Alegre')
plt.bar(barra2, lista_confirmados_londrina, color='green', width=bar_width, label='Londrina')
plt.bar(barra3, lista_confirmados_pato_branco, color='blue', width=bar_width, label='Pato Branco')
plt.bar(barra4, lista_confirmados_sao_carlos, color='purple', width=bar_width, label='Londrina')
plt.bar(barra5, lista_confirmados_santo_antonio_da_platina, color='orange', width=bar_width, label='Santo Ant° da Platina')

# Adicionando legendas nas barras
plt.xlabel('Semanas Epidemiológicas')
plt.xticks([r + bar_width for r in range(len(lista_confirmados_porto_alegre))], 
            ['Semana 26', 'Semana 27', 'Semana 28', 'Semana 29',])
plt.ylabel('Casos Confirmados de Covid-19')
plt.title('Representação de Casos Confirmados de Covid-19 entre as Semanas 26 e 29')

# Criando a legenda e exibindo o gráfico
plt.legend()
plt.show()

##### GRÁFICO DE MORTES CONFIRMADAS #####
# Cria Dataframe de Últimas Mortes Confirmadas
df_ultimas_mortes_confirmadas = df_covid.loc[:, ['ID', 'Cidade', 'Semana Epidemiologica', 'Ultimas Mortes Confirmadas']]

# Aplica filtros de cidades e retorna valores em array para serem usados nos gráficos
df_ultimas_mortes_confirmadas_porto_alegre = df_ultimas_mortes_confirmadas[
    df_ultimas_mortes_confirmadas['Cidade'] == 'Porto Alegre']

df_ultimas_mortes_confirmadas_londrina = df_ultimas_mortes_confirmadas[
    df_ultimas_mortes_confirmadas['Cidade'] == 'Londrina']

df_ultimas_mortes_confirmadaspato_branco = df_ultimas_mortes_confirmadas[
    df_ultimas_mortes_confirmadas['Cidade'] == 'Pato Branco']

df_ultimas_mortes_confirmadas_sao_carlos = df_ultimas_mortes_confirmadas[
    df_ultimas_mortes_confirmadas['Cidade'] == 'São Carlos']

df_ultimas_mortes_confirmadas_santo_antonio_da_platina = df_ultimas_mortes_confirmadas[
    df_ultimas_mortes_confirmadas['Cidade'] == 'Santo Antônio da Platina']

# Cria listas contendo os valores de mortes confirmadas de cada cidade, para serem usadas nos gráficos
lista_mortes_confirmadas_porto_alegre = df_ultimas_mortes_confirmadas_porto_alegre['Ultimas Mortes Confirmadas'].to_list()
lista_mortes_confirmadas_londrina = df_ultimas_mortes_confirmadas_londrina['Ultimas Mortes Confirmadas'].to_list()
lista_mortes_confirmadas_pato_branco = df_ultimas_mortes_confirmadaspato_branco['Ultimas Mortes Confirmadas'].to_list()
lista_mortes_confirmadas_sao_carlos = df_ultimas_mortes_confirmadas_sao_carlos['Ultimas Mortes Confirmadas'].to_list()
lista_mortes_confirmadas_santo_antonio_da_platina = df_ultimas_mortes_confirmadas_santo_antonio_da_platina['Ultimas Mortes Confirmadas'].to_list()

# Define a largura das barras
bar_width = 0.15

# Aumenta o gráfico
plt.figure(figsize=(15,7.5))

# Definindo as posições das barras
barra1 = np.arange(len(lista_mortes_confirmadas_porto_alegre))
barra2 = [x + bar_width for x in barra1]
barra3 = [x + bar_width for x in barra2]
barra4 = [x + bar_width for x in barra3]
barra5 = [x + bar_width for x in barra4]

# Criando as barras
plt.bar(barra1, lista_mortes_confirmadas_porto_alegre, color='red', width=bar_width, label='Porto Alegre')
plt.bar(barra2, lista_mortes_confirmadas_londrina, color='green', width=bar_width, label='Londrina')
plt.bar(barra3, lista_mortes_confirmadas_pato_branco, color='blue', width=bar_width, label='Pato Branco')
plt.bar(barra4, lista_mortes_confirmadas_sao_carlos, color='purple', width=bar_width, label='São Carlos')
plt.bar(barra5, lista_mortes_confirmadas_santo_antonio_da_platina, color='orange', width=bar_width, label='Santo Ant° da Platina')

# Adicionando legendas nas barras
plt.xlabel('Semanas Epidemiológicas')
plt.xticks([r + bar_width for r in range(len(lista_mortes_confirmadas_porto_alegre))], 
            ['Semana 26', 'Semana 27', 'Semana 28', 'Semana 29',])
plt.ylabel('Mortes Confirmados por Covid-19')
plt.title('Representação de Mortes Confirmadas de Covid-19 entre as Semanas 26 e 29')

# Criando a legenda e exibindo o gráfico
plt.legend()
plt.show()

##### GRÁFICO DE CASOS CONFIRMADOS POR 100 MIL HABITANTES #####
# Cria Dataframe de Últimos Casos Confirmados
df_ultimos_casos_confirmados_por_100k = df_covid.loc[:, ['ID', 'Cidade', 'Semana Epidemiologica', 'Ultimos Casos por 100 Mil Habitantes']]

# Aplica filtros de cidades e retorna valores em array para serem usados nos gráficos
df_ultimos_casos_confirmados_por_100k_porto_alegre = df_ultimos_casos_confirmados_por_100k[
    df_ultimos_casos_confirmados_por_100k['Cidade'] == 'Porto Alegre']

df_ultimos_casos_confirmados_por_100k_londrina = df_ultimos_casos_confirmados_por_100k[
    df_ultimos_casos_confirmados_por_100k['Cidade'] == 'Londrina']

df_ultimos_casos_confirmados_por_100k_pato_branco = df_ultimos_casos_confirmados_por_100k[
    df_ultimos_casos_confirmados_por_100k['Cidade'] == 'Pato Branco']

df_ultimos_casos_confirmados_por_100k_sao_carlos = df_ultimos_casos_confirmados_por_100k[
    df_ultimos_casos_confirmados_por_100k['Cidade'] == 'São Carlos']

df_ultimos_casos_confirmados_por_100k_santo_antonio_da_platina = df_ultimos_casos_confirmados_por_100k[
    df_ultimos_casos_confirmados_por_100k['Cidade'] == 'Santo Antônio da Platina']

# Cria listas contendo os valores de casos confirmados de cada cidade, para serem usadas nos gráficos
lista_confirmados_por_100k_porto_alegre = df_ultimos_casos_confirmados_por_100k_porto_alegre['Ultimos Casos por 100 Mil Habitantes'].to_list()
lista_confirmados_por_100k_londrina = df_ultimos_casos_confirmados_por_100k_londrina['Ultimos Casos por 100 Mil Habitantes'].to_list()
lista_confirmados_por_100k_pato_branco = df_ultimos_casos_confirmados_por_100k_pato_branco['Ultimos Casos por 100 Mil Habitantes'].to_list()
lista_confirmados_por_100k_sao_carlos = df_ultimos_casos_confirmados_por_100k_sao_carlos['Ultimos Casos por 100 Mil Habitantes'].to_list()
lista_confirmados_por_100k_santo_antonio_da_platina = df_ultimos_casos_confirmados_por_100k_santo_antonio_da_platina['Ultimos Casos por 100 Mil Habitantes'].to_list()

# Define a largura das barras
bar_width = 0.15

# Aumenta o gráfico
plt.figure(figsize=(15,7.5))

# Definindo as posições das barras
barra1 = np.arange(len(lista_confirmados_por_100k_porto_alegre))
barra2 = [x + bar_width for x in barra1]
barra3 = [x + bar_width for x in barra2]
barra4 = [x + bar_width for x in barra3]
barra5 = [x + bar_width for x in barra4]

# Criando as barras
plt.bar(barra1, lista_confirmados_por_100k_porto_alegre, color='red', width=bar_width, label='Porto Alegre')
plt.bar(barra2, lista_confirmados_por_100k_londrina, color='green', width=bar_width, label='Londrina')
plt.bar(barra3, lista_confirmados_por_100k_pato_branco, color='blue', width=bar_width, label='Pato Branco')
plt.bar(barra4, lista_confirmados_por_100k_sao_carlos, color='purple', width=bar_width, label='Londrina')
plt.bar(barra5, lista_confirmados_por_100k_santo_antonio_da_platina, color='orange', width=bar_width, label='Santo Ant° da Platina')

# Adicionando legendas nas barras
plt.xlabel('Semanas Epidemiológicas')
plt.xticks([r + bar_width for r in range(len(lista_confirmados_por_100k_porto_alegre))], 
            ['Semana 26', 'Semana 27', 'Semana 28', 'Semana 29',])
plt.ylabel('Casos Confirmados de Covid-19 por 100 Mil Habitantes')
plt.title('Representação de Casos Confirmados de Covid-19 por 100 mil Habitantes entre as Semanas 26 e 29')

# Criando a legenda e exibindo o gráfico
plt.legend()
plt.show()

##### GRÁFICO DE TAXA DE MORTE #####
# Cria Dataframe de Última Taxa de Morte Confirmada
df_taxa_mortes_confirmadas = df_covid.loc[:, ['ID', 'Cidade', 'Semana Epidemiologica', 'Ultima Taxa de Morte Confirmada']]

# Aplica filtros de cidades e retorna valores em array para serem usados nos gráficos
df_taxa_mortes_confirmadas_porto_alegre = df_taxa_mortes_confirmadas[
    df_taxa_mortes_confirmadas['Cidade'] == 'Porto Alegre']

df_taxa_mortes_confirmadas_londrina = df_taxa_mortes_confirmadas[
    df_taxa_mortes_confirmadas['Cidade'] == 'Londrina']

df_taxa_mortes_confirmadas_pato_branco = df_taxa_mortes_confirmadas[
    df_taxa_mortes_confirmadas['Cidade'] == 'Pato Branco']

df_taxa_mortes_confirmadas_sao_carlos = df_taxa_mortes_confirmadas[
    df_taxa_mortes_confirmadas['Cidade'] == 'São Carlos']

df_taxa_mortes_confirmadas_santo_antonio_da_platina = df_taxa_mortes_confirmadas[
    df_taxa_mortes_confirmadas['Cidade'] == 'Santo Antônio da Platina']

# Cria listas contendo os valores de mortes confirmadas de cada cidade, para serem usadas nos gráficos
lista_taxa_mortes_confirmadas_porto_alegre = df_taxa_mortes_confirmadas_porto_alegre['Ultima Taxa de Morte Confirmada'].to_list()
lista_taxa_mortes_confirmadas_londrina = df_taxa_mortes_confirmadas_londrina['Ultima Taxa de Morte Confirmada'].to_list()
lista_taxa_mortes_confirmadas_pato_branco = df_taxa_mortes_confirmadas_pato_branco['Ultima Taxa de Morte Confirmada'].to_list()
lista_taxa_mortes_confirmadas_sao_carlos = df_taxa_mortes_confirmadas_sao_carlos['Ultima Taxa de Morte Confirmada'].to_list()
lista_taxa_mortes_confirmadas_santo_antonio_da_platina = df_taxa_mortes_confirmadas_santo_antonio_da_platina['Ultima Taxa de Morte Confirmada'].to_list()

# Aumenta o gráfico
plt.figure(figsize=(15,7.5))

# Define a legenda e tamanho de linhas
plt.plot([26, 27, 28, 29], lista_taxa_mortes_confirmadas_porto_alegre, color='red', label='Porto Alegre')
plt.plot([26, 27, 28, 29], lista_taxa_mortes_confirmadas_londrina, color='green', label='Londrina')
plt.plot([26, 27, 28, 29], lista_taxa_mortes_confirmadas_pato_branco, color='blue', label='Pato Branco')
plt.plot([26, 27, 28, 29], lista_taxa_mortes_confirmadas_sao_carlos, color='purple', label='São Carlos')
plt.plot([26, 27, 28, 29], lista_taxa_mortes_confirmadas_santo_antonio_da_platina, color='orange', label='Santo Ant° da Platina')

# Define as labels do gráfico
plt.xlabel('Semanas Epidemiológicas de Covid-19')
plt.ylabel('Taxa de Mortalidade por Covid-19')

# Define o título do gráfico
plt.title("Taxa de Mortalidade por Covid-19 entre as Semanas 26 e 29")

# Configurações do texto
# plt.text(1.00, 1.0, "Cruzamento das Linhas", fontsize=8, horizontalalignment='right')

# Criando a legenda e exibindo o gráfico
plt.legend()
plt.show()
