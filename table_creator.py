import psycopg
from time import time
import numpy as np

def create_table(size, cursor):
	query = """
	create table users (
	user_id serial  primary key,
	name varchar(20),
	surname varchar(20),
	age integer,
	country varchar(20), 
	city varchar(20), 
	rating real
	);
	"""
	cursor.execute(query)
	conn.commit()

	ag = np.random.randint(1, 120, size)
	rat = np.random.uniform(0.1, 10.0, size)
	name_arr = ['Nikita', 'Sasha', 'Stepa', 'Mark']
	new_name_arr = np.random.choice(name_arr, size)
	surname_arr = ['Tixomirov', 'Naymov', 'Greezhe', 'Kaplya']
	new_surname_arr = np.random.choice(surname_arr, size)
	country_arr = ['Brazil', 'Bulgaria', 'Russian', 'Germany']
	new_country_arr = np.random.choice(country_arr, size)
	city_arr = ['Moscow', 'Piter', 'Stambyl', 'Frankfurt']
	new_city_arr = np.random.choice(city_arr, size)

	for i in range(size):
		query = f"""
			Insert Into users (name, surname, age, country, city, rating) Values('{new_name_arr[i]}', '{new_surname_arr[i]}', {ag[i]}, '{new_country_arr[i]}', '{new_city_arr[i]}', {rat[i]});
			""" 
		cursor.execute(query)
	conn.commit()

def check_exist(cursor):
	query = """
	drop table If Exists users;
	"""
	cursor.execute(query)
	conn.commit()

def check_table(query, cursor):
	cursor.execute(query)
	result = cursor.fetchall()
	return result

conn = psycopg.connect(
	dbname="mydb", user="myuser", password="password", host="localhost"
)
cur = conn.cursor()

check_exist(cur)
create_table(10000, cur)
# изменяем кол-во записей
times = []
for i in range(10):
	start = time()
	r = check_table("Select name From users Where rating < 5.0;",cur)
# для простого запроса в r = check_table ("Select count(*) From users;")
	end = time () 
	times.append(end-start)

print('Вывод -',r)
print('Время каждого запуска -', times)
print('Среднее время по 10 проходам -',np.mean(times))

cur.close()
conn.close()
#Select AVG (age) From users WHERE (name='Mark' OR name='Sasha') AND country in (SELECT country FROM (SELECT country, AVG(rating) FROM users GROUP BY country ORDER BY country LIMIT 1) AS country_table);
#Select name From users Where rating < 5.0;
