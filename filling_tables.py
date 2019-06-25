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

def filling_table(check, parse_table, parse_columns, data):
	columns_db = info_take(f"""SELECT column_name, data_type, column_default FROM information_schema.columns
	WHERE information_schema.columns.table_name = '{parse_table}'""")
	columns_db_names = [x[0] for x in columns_db]
	# columns_db_types = [x[1] for x in columns_db]
	columns_db_default = [x[2] for x in columns_db]
	
	
	col = 0
	def_val = 0
	parse_columns = parse_columns.split(', ')
	while col < len(parse_columns):
		while def_val < len(columns_db_default):	
			if columns_db_default[def_val] is not None:
				parse_columns.insert(def_val, columns_db_names[def_val])
				data = data.split(', ')
				data.insert(def_val, 'DEFAULT')
				data = ', '.join(data)
			def_val += 1
		col += 1
	parse_columns = ', '.join(parse_columns)
	
	
	data = data.split(', ')
	request = f"""INSERT INTO "{parse_table}" () VALUES ()"""
	request = request[:request.find('VALUES')-2] + parse_columns + request[request.find('VALUES')-2:]
	for value in data[:-1]:
		if not value.isdigit() and value != 'DEFAULT':
			value = "'" + value + "'"
		request = request[:-1] + value + ', ' + request[-1:]
	if not data[-1].isdigit() and data[-1] != 'DEFAULT':
		request = request[:-1] + "'" + data[-1] + "'" + request[-1:]
	else:
		request = request[:-1] + data[-1] + request[-1:]
	print(request)
	# info_give(request)

script_dir = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(script_dir, 'parse_data.txt'), 'r', encoding='utf-8') as data:
	lines = data.readlines()
	data.close()

def parsing(lines):
	tables = []
	columns = []
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
			tables.append(parse_table)

			parse_columns = row[row.find('[')+1:row.find(']')].split(',')
			for i, column in enumerate(parse_columns):
				parse_columns[i] = column.strip()
			columns_db = info_take(f"""SELECT column_name, data_type, column_default FROM information_schema.columns
			WHERE information_schema.columns.table_name = '{parse_table}'""")
			columns_db_names = [x[0] for x in columns_db]
			for column in parse_columns:
				if column not in columns_db_names:
					log(f'Столбец "{column}" не найден.\n')
					print(f'Столбец "{column}" не найден.')
					break

			parse_columns = ', '.join(parse_columns)
			columns.append(parse_columns)
		else:
			data.append(row.strip())
	return tables, columns, data

parse_tables, parse_columns, data = parsing(lines)
print(parse_tables)
print(parse_columns)
print(data)

table = 0
row = 0
while table < len(parse_tables):
	while row < len(data):
		if data[row] != 'delimeter for new table':
			filling_table(0, parse_tables[table], parse_columns[table], data[row])
			row += 1
		else:
			row += 1
			break
	table += 1
