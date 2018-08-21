import MAL_api as mal
import argparse
from os import path
import json
import pymysql


def db_content_file():
	if path.exists('db_config.json'):
		with open('db_config.json', 'r') as file:
			content = json.loads(file.read())
	else:
		user = str(input('MySQL DB user: '))
		password = str(input('MySQL DB pass: '))
		database = str(input('MySQL database name: '))
		port = int(input('MySQL port number (empty if none): '))
		port = port if port else None
		content = {
			'user': user,
			'password': password,
			'database': database,
			'port': port
		}
		with open('db_config.json', 'w') as file:
			file.write(json.dumps(content))

	# Connect to DB
	DB = pymysql.connect(host='localhost',
						 user=content['user'],
						 password=content['password'],
						 db=content['database'],
						 charset='utf8mb4',
						 port=content['port'],
						 cursorclass=pymysql.cursors.DictCursor)

	# Run table creation sanity scripts
	sql = {}
	sql['0'] = 'CREATE TABLE IF NOT EXISTS mal_show (' \
			   'idx INT PRIMARY KEY, ' \
			   'show_id INT UNIQUE, ' \
			   'name_ VARCHAR(100), ' \
			   'studio INT, ' \
			   'score FLOAT, ' \
			   'scored_by INT, ' \
			   'season CHAR(6), ' \
	           'type VARCHAR(10)' \
			   ')'

	sql['1'] = 'CREATE TABLE IF NOT EXISTS mal_show_genres (' \
			   'idx INT, ' \
	           'show_id INT, ' \
			   'genre INT' \
			   ')'

	sql['2'] = 'CREATE TABLE IF NOT EXISTS mal_rec (' \
	           'idx INT, ' \
	           'show_id INT, ' \
			   'recommended_id INT, ' \
	           'count INT DEFAULT 0, ' \
	           'PRIMARY KEY (show_id, recommended_id)' \
			   ')'

	sql['3'] = 'CREATE TABLE IF NOT EXISTS mal_producers (' \
			   'idx INT PRIMARY KEY AUTO_INCREMENT, ' \
			   'producer_id INT UNIQUE, ' \
			   'name VARCHAR(20)' \
			   ')'

	try:
		with DB.cursor() as cursor:
			for k in sql:
				cursor.execute(sql[k])
		DB.commit()
	finally:
		DB.close()

	return content


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-p', '--producers', action='store_true', default=False)
	parser.add_argument('-s', '--sleep', action='store_true', default=False)
	parser.add_argument('-v', '--verbose', action='store_true', default=False)
	parser.add_argument('-i', '--i_start', default=-1, type=int)
	parser.add_argument('-f', '--file', action='store_true', default=False)
	parser.add_argument('-d', '--database', action='store_true', default=False)

	args = parser.parse_args()
	
	if not (args.database or args.file):
		raise Exception('File and/or Database must be used')

	
	db_content = db_content_file() if args.database else None

	ex = mal.Extracter(db=db_content, file=args.file)

	if args.producers:
		ex.update_producers(verbose=args.verbose)

	ex.retrieve(start_i=args.i_start, fail_limit=10000, sleep=args.sleep, verbose=args.verbose)
