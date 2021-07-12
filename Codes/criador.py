import psycopg2
import datetime

db_user = 'postgres'
db_password = 'admin'
conn = psycopg2.connect("dbname=grupo_m user="+db_user+" password="+db_password)
cur = conn.cursor()

#Criar a tabela cidade
cur.execute('''
            CREATE TABLE IF NOT EXISTS city(
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                estimated_population INTEGER
            );
            ''')

#Criar a tabela covid data
cur.execute('''
            CREATE TABLE IF NOT EXISTS covid_data(
                id SERIAL PRIMARY KEY,
                date_covid_data DATE,
                epidemiological_week INTEGER,
                last_available_confirmed INTEGER,
                last_available_confirmed_per_100k_inhabitants REAL,
                last_available_deaths INTEGER,
                last_available_death_rate REAL,
                new_confirmed INTEGER,
                new_deaths INTEGER,
                city_id INTEGER REFERENCES city(id)
            );
            ''')
conn.commit()
cur.close()
conn.close()
