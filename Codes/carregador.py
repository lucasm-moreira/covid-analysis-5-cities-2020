import psycopg2
import datetime
import pandas as pd

#Conectar ao banco de dados
db_user = 'postgres'
db_password = 'admin'
conn = psycopg2.connect("dbname=grupo_m user="+db_user+" password="+db_password)
cur = conn.cursor()

#Ler os dados do arquivo
dataframe = pd.read_csv("dados_covid.csv")

#Filtro pelas cidades
filtro_cidades = (dataframe['city'] == 'Pato Branco') |\
                    (dataframe['city'] == 'Londrina') |\
                    (dataframe['city'] == 'Porto Alegre') |\
                    (dataframe['city'] == 'São Carlos') |\
                    (dataframe['city'] == 'Santo Antônio da Platina') 
dataframe_cidades = dataframe[filtro_cidades]

#Recupera apenas a cidade e a população estimada
dataframe_cidades_populacao = dataframe_cidades[['city', 'estimated_population_2019']]

#Agrupa pelas cidades
group_dataframe_cidades_populacao = dataframe_cidades_populacao.groupby("city").head(1)

#Percorre os indices do dataframe e insere no banco
for index in range(group_dataframe_cidades_populacao.count()['city']):
    cur.execute('''
                INSERT INTO city (name, estimated_population)
                VALUES (%(name)s, %(estimated_population)s);
                ''',{
                    'name': group_dataframe_cidades_populacao.iloc[index]['city'],
                    'estimated_population': group_dataframe_cidades_populacao.iloc[index]['estimated_population_2019']
                })
    conn.commit()

#Apos inserir na tabela cidade, faz um select para recuperar os valores a serem usados como chave estrangeira
cur.execute('SELECT * FROM city WHERE name LIKE \'Pato Branco\'')
id_pato_branco = cur.fetchone()[0]
cur.execute('SELECT * FROM city WHERE name LIKE \'Londrina\'')
id_londrina = cur.fetchone()[0]
cur.execute('SELECT * FROM city WHERE name LIKE \'Porto Alegre\'')
id_poa = cur.fetchone()[0]
cur.execute('SELECT * FROM city WHERE name LIKE \'São Carlos\'')
id_sao_carlos = cur.fetchone()[0]
cur.execute('SELECT * FROM city WHERE name LIKE \'Santo Antônio da Platina\'')
id_santo_antonio = cur.fetchone()[0]

#Esta funcao faz o mapeamento do nome da cidade, pelo valor da chave estrangeira
def map_city_to_id(city_name):
    if city_name == 'Pato Branco':
        return id_pato_branco
    elif city_name == 'Londrina':
        return id_londrina
    elif city_name == 'Porto Alegre':
        return id_poa
    elif city_name == 'São Carlos':
        return id_sao_carlos
    elif city_name == 'Santo Antônio da Platina':
        return id_santo_antonio

#Percore todos os dados dos dataframe das cidades e insere no banco de dados
for index in range(dataframe_cidades.count()['last_available_confirmed']):
    cur.execute('''
                INSERT INTO covid_data ( date_covid_data, epidemiological_week, last_available_confirmed,
                last_available_confirmed_per_100k_inhabitants, last_available_deaths, last_available_death_rate,
                new_confirmed, new_deaths, city_id)
                VALUES (%(date_covid_data)s, %(epidemiological_week)s, %(last_available_confirmed)s,
                %(last_available_confirmed_per_100k_inhabitants)s,  %(last_available_deaths)s, %(last_available_death_rate)s,
                %(new_confirmed)s, %(new_deaths)s,  %(city_id)s);
                ''',{
                    'date_covid_data': dataframe_cidades.iloc[index]['date'],
                    'epidemiological_week': int(dataframe_cidades.iloc[index]['epidemiological_week']),
                    'last_available_confirmed': int(dataframe_cidades.iloc[index]['last_available_confirmed']),
                    'last_available_confirmed_per_100k_inhabitants': float(dataframe_cidades.iloc[index]['last_available_confirmed_per_100k_inhabitants']),
                    'last_available_deaths': int(dataframe_cidades.iloc[index]['last_available_deaths']),
                    'last_available_death_rate': float(dataframe_cidades.iloc[index]['last_available_death_rate']),
                    'new_confirmed': int(dataframe_cidades.iloc[index]['new_confirmed']),
                    'new_deaths':int(dataframe_cidades.iloc[index]['new_deaths']),
                    'city_id': map_city_to_id(dataframe_cidades.iloc[index]['city'])
                })
    conn.commit()

cur.close()
conn.close()
