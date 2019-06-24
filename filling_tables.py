import psycopg2
import os

def info_take(request):
	con = psycopg2.connect(
	database="BelTest", 
	user="postgres", 
	password="1327968", 
	host="127.0.0.1", 
	port="5432"
	)
	cur = con.cursor()
	cur.execute(request)
	info = cur.fetchall()
	con.close()
	return info

def info_give(request):
	con = psycopg2.connect(
	database="BelTest", 
	user="postgres", 
	password="1327968", 
	host="127.0.0.1", 
	port="5432"
	)
	cur = con.cursor()
	cur.execute(request)
	con.commit()
	con.close()

def log(message):
	with open(os.path.join(script_dir, 'log.txt'), 'a') as output:
		output.write(message)

def filling_table(check, parse_table, data):
	columns = info_take(f"""SELECT column_name, data_type FROM information_schema.columns
		WHERE information_schema.columns.table_name = '{parse_table}'""")
	columns_names = [x[0] for x in columns]
	columns_types = [x[1] for x in columns]
	data = data.split(', ')
	request = f"""INSERT INTO {parse_table} VALUES ()"""
	for value in data[:-1]:
		if not value.isdigit():
			value = "'" + value + "'"
		request = request[:-1] + value + ', ' + request[-1:]
	request = request[:-1] + data[-1] + request[-1:]
	print(request)
	# info_give(request)

script_dir = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(script_dir, 'parse_data.txt'), 'r', encoding='utf-8') as data:
	lines = data.readlines()
	data.close()

def parsing(lines):
	parse_tables = []
	data = []
	tables_db = info_take("""SELECT table_name FROM information_schema.tables
	WHERE table_schema = 'public'""")
	tables_db = [x[0] for x in tables_db]
	for row in lines:
		if row.find('Таблица:') != -1:
			if len(data) > 0:
				data.append('delimeter for new table')
			parse_table = row[8:row.find('[')].strip()
			if parse_table not in tables_db:
				log(f'Таблица "{parse_table}" не найдена.\n')
				print(f'Таблица "{parse_table}" не найдена.')
				break
			parse_tables.append(parse_table)

			parse_columns = row[row.find('[')+1:row.find(']')].split(',')
			for i, column in enumerate(parse_columns):
				parse_columns[i] = column.strip()
			columns_db = info_take(f"""SELECT column_name, data_type, column_default FROM information_schema.columns
			WHERE information_schema.columns.table_name = '{parse_table}'""")
			columns_db = [x[0] for x in columns_db]
			for column in parse_columns:
				if column not in columns_db:
					log(f'Столбец "{column}" не найден.\n')
					print(f'Столбец "{column}" не найден.')
		else:
			data.append(row.strip())
	return parse_tables, data

parse_tables, data = parsing(lines)
print(parse_tables)
print(data)
for row in data:
	pass
# filling_table(0, parse_table, data)
