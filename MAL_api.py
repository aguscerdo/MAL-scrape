from bs4 import BeautifulSoup
# import html
import urllib.request
from urllib.parse import quote
import re
import time
import json
import atexit
import pymysql
import numpy as np

class Extracter:
	def __init__(self, db=None, use_file=False, retry=False):
		self.fail_max = 400
		self.show_id = 0
		self.db = None
		self.show = {}
		self.sleep = True
		self.verbose = True
		self.proceed = 200
		self.file = None
		self.idx = 0
		self.retry=retry
		
		self.db_config = db
		
		self.file=None
		self.use_file = use_file

	def __del__(self):
		if self.db is not None:
			try:
				self.db.close()
			except:
				pass
		if self.file is not None:
			try:
				self.file.close()
			except:
				pass
			
			
	def retrieve(self, init=-1, start_i=0, fail_limit=400, sleep=True, verbose=True):
		# --- Input --- #
		#   + verbose: Print information about actions. Default True
		#   + iter: number of max iterations to run. Default to -1 -> infinite
		#   + start_i: index to start looking from. Overwrites file i
		#   + fail_limit: maximum number of failures sequence allowed.
		#	   Every failure decreases count by 1, every success increases by 1
		#   + sleep: sleep for around 1 second between calls.
		self.__open_file()
		self.__open_db()


		self.sleep = sleep
		if start_i > 0:
			self.show_id = start_i - 1
			
		self.verbose = verbose
		top = self.show_id + init if init > 0 else 0
		
		if fail_limit > 0:
			self.proceed = fail_limit
			self.fail_max = fail_limit
		else:
			self.proceed = self.fail_max

		# Enter main loop
		while self.proceed > 0 and not (top and self.show_id > top):
			ret = self.__url_main()	 # url_main returns 0 on success, 1 on failure
			if not ret:
				# Successful retrieval
				self.__url_rec()
				self.__verbose(self.show)
				self.__increase_idx()
				self.__success_flush()
				self.proceed = min(self.proceed + 1, self.fail_max)
			else:
				self.proceed -= 1

			self.__fail_flush()
			self.__sleepy(0.5)
			self.__increase_show_id()

		self.__close_file()
		self.__close_db()

	def update_producers(self, verbose=False):
		if self.use_file:
			try:
				producer_file = open("data/producers.json", "a+")
			except:
				producer_file = open("data/producers.json", "w+")

			
		if self.db_config is not None:
			self.__open_db()
		
		url = "https://myanimelist.net/anime/producer"
		try:
			response = urllib.request.urlopen(url)
		except:
			print("Could not retrieve producer data")
			return
		soup = BeautifulSoup(response, 'lxml')
		producers = {}
		if self.file is not None:
			producer_file = open("data/producers.json", 'w+')

		for p in soup.find_all('a', href=re.compile(r'producer/\d+/')):
			link = p.get('href')
			pid = re.search(r'\d+', link).group(0)
			name = re.search(r'\d+/.*$', link).group(0).replace('{}'.format(pid), '').replace('/', '')
			producers[pid] = name
			self.__verbose("ID: {} -- NAME: {}".format(pid, name))
				
			if self.use_file:
				producer_file.write('{"id" : %s, "name" : "%s"}\n' % (pid, name))
				
			if self.db is not None:
				self.__insert_to_producers(pid, name)

		if self.use_file is not None:
			producer_file.close()
		if self.db_config is not None:
			self.__close_db()


	# --- URL functions --- #
	def __url_main(self):
		self.state = True
		url = "https://myanimelist.net/anime/{}".format(self.show_id)
		try:
			response = urllib.request.urlopen(url)
		except:
			self.__verbose("Could not retrieve anime: {}".format(self.show_id))
			return 1
		self.soup = BeautifulSoup(response, 'lxml')

		# Main page data
		self.__find_name()
		self.__find_genre()
		self.__get_stats()
		self.__get_studio()
		self.__get_season()
		self.__get_type()

		# Rec page data
		self.__url_rec()

		# Storing actions
		self.__insert_to_db()
		self.__write_show_to_file()
		return 0


	def __url_rec(self):
		try:
			url = self.soup.find('a', href=re.compile(r'/userrecs')).get('href')
			url = quote(url, safe=':/')
			response = urllib.request.urlopen(url)
		except:
			self.__verbose("Could not retrieve recommendations: {}".format(self.show_id))
			self.show['nrecs'] = 0
			self.show['recs'] = None
			return
		soup_rec = BeautifulSoup(response, 'lxml')
		
		# Process a recommendation match of the form '<int>-<int>'
		def rec_id_function(rec):
			rec_url = re.search(r'(/recommendations/anime/)(\d+-\d+)'.format(self.show_id), rec.get('href')).group(2)
			distinct_numbers = re.findall(r'\d+', rec_url)
			rec_id = int(distinct_numbers[0]) if int(distinct_numbers[0]) != self.show_id else int(distinct_numbers[1])
			return rec_id
			
		# Find all links of the form 'recommendations/anime/<int>-<int>'
		recommendations = [rec_id_function(rec)
		                   for rec in soup_rec.find_all('a', href=re.compile('/recommendations/anime/\d+-\d+'))]

		# Get number of recs
		rec_count = [1] * len(recommendations)
		for i, val in enumerate(soup_rec.find_all('a', {'class':"js-similar-recommendations-button"})):
			rec_count[i] = int(val.find('strong').text) + 1

		dictionary = {rec: count for rec, count in zip(recommendations, rec_count)}
			
		self.show['nrecs'] = int(np.sum([int(dictionary[r]) for r in dictionary]))
		self.show['recs'] = dictionary


	# ------------ Parsing Functions ------------ #
	def __find_name(self):
		link = str(self.soup.find('link'))
		self.show['name_'] = re.search(r'\d/([^"]+)', link).group(1)
		# self.show['name'] = self.show['name_'].replace('_', ' ')


	def __find_genre(self):
		genres = [int(re.search(r'genre/(\d+)/', str(a)).group(1))
		          for a in self.soup.find_all("a", href=re.compile(r'genre/(\d+)/'))]
		self.show['genres'] = genres


	def __get_stats(self):
		# Score
		stats = self.soup.find_all('span', itemprop=["ratingValue", "ratingCount"]) # Score, count
		if not stats:
			self.__verbose("Could not retrieve stats: {}".format(self.show_id))
			self.show['score'] = 0
			self.show['scored_by'] = 0
			return
		numbers = re.findall(r'\d+[.,]?\d*', str(stats))
		self.show['score'] = float(numbers[0])
		self.show['scored_by'] = int(numbers[1].replace(',', ''))


	def __get_studio(self):
		studio =  self.soup.find_all("a", href=re.compile(r'producer/'))
		if not studio:
			self.show['studio'] = "0"
			return
		self.show['studio'] = int(re.search(r'/producer/(\d+)/', str(studio[-1])).group(1))
		

	def __get_season(self):
		season =  self.soup.find("a", href=re.compile(r'season/'))
		if not season:  # Non standard date
			season = re.search(r'[a-zA-Z]{3} \d\d, \d{4}\n', self.soup.text)
			if not season:
				self.show['season'] = None
				self.show['year'] = None
			else:
				self.__parse_date(season.group(0))
		else:
			self.__mini_parse_date(season)
			

	def __get_type(self):
		t = self.soup.find('a', href=re.compile('\?type='))
		if not t:
			self.show['type'] = None
			return
		self.show['type'] = t.text


	# ------------ Database functions ------------ #
	def __open_db(self):
		if self.db_config is None:
			return
		
		self.db = pymysql.connect(
			host='localhost',
			user=self.db_config['user'],
			password=self.db_config['password'],
			db=self.db_config['database'],
			charset='utf8mb4',
			port=self.db_config['port'],
			cursorclass=pymysql.cursors.DictCursor
		)

		with self.db.cursor() as cursor:
			sql = 'SELECT MAX(idx) as idx, MAX(show_id) as sid from mal_show'
			cursor.execute(sql)
			max_i = cursor.fetchone()
			
			self.idx = max_i['idx'] if max_i['idx'] is not None else 0
			self.show['idx'] = self.idx
			self.show_id = max_i['sid'] if max_i['sid'] is not None else 0
			self.show['show_id'] = self.show_id
			print("Resuming at index {}".format(self.show_id))
	
	
	def __close_db(self):
		if self.db is not None:
			self.db.commit()
			self.db.close()
	
	
	def __insert_to_producers(self, key, value):
		if self.db is None:
			return 1
		
		sql = 'INSERT INTO mal_producers (producer_id, name) VALUES (%s, %s)'
		with self.db.cursor() as cursor:
			cursor.execute(sql, (key, value))
		self.db.commit()
	
	
	def __insert_to_db(self):
		if self.db is None:
			return
		sql0 = 'REPLACE INTO mal_show ' \
		       '(idx, show_id, name_, studio, score, scored_by, season, year, type, nrec) VALUES ' \
		       '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
		
		sql1 = 'REPLACE INTO mal_rec ' \
		       '(idx, show_id, recommended_id, count) VALUES ' \
		       '(%s, %s, %s, %s)'
		
		sql2 = 'REPLACE INTO mal_show_genres ' \
		       '(idx, show_id, genre) VALUES ' \
		       '(%s, %s, %s) '
		
		with self.db.cursor() as cursor:
			cursor.execute(sql0,
			               (self.idx, self.show_id, self.show['name_'], self.show['studio'],
			                self.show['score'], float(self.show['scored_by']),
			                self.show['season'], self.show['year'],
			                self.show['type'], self.show['nrecs']))
			
			if self.show['recs'] is not None:
				for rec in self.show['recs']:
					cursor.execute(sql1, (self.idx, self.show_id, rec, self.show['recs'][rec]))
			
			if self.show['genres'] is not None:
				for genre in self.show['genres']:
					cursor.execute(sql2, (self.idx, self.show_id, genre))
						
						
	# ------------ File functions ------------ #
	def __open_file(self):
		if not self.use_file:
			return
		try:
			with open("data/shows.json", "r") as f:
				self.show_id = re.search(r"\"id\": (\d+),", (f.readlines()[-1])).group(1)
				self.show_id = int(self.show_id)
				self.file = open("data/shows.json", "a+")
				self.__verbose("Resuming at index {}".format(self.show_id))
		except:
			self.file = open("data/shows.json", "w+")
	
	
	def __close_file(self):
		if not self.use_file:
			return
		self.file.close()
		
		
	def __write_show_to_file(self):
		if self.file is not None:
			self.file.write("{}\n".format(json.dumps(self.show)).replace("'", '"'))


	# ------------ Other functions ------------ #
	def __success_flush(self):
		if not self.idx % 10:
			self.__verbose("--- Flushing... ---")
			if self.file is not None:
				self.file.flush()
			if self.db is not None:
				self.db.commit()
				
				
	def __fail_flush(self):
		if not self.show_id % 50:
			self.__verbose("--- Flushing... ---")
			if self.file is not None:
				self.file.flush()
			if self.db is not None:
				self.db.commit()
				
				
	def __increase_show_id(self):
		self.show_id += 1
		self.show['show_id'] = self.show_id
		
	
	def __increase_idx(self):
		self.idx += 1
		self.show['idx'] = self.idx
		
		
	def __parse_date(self, str):
		month = re.search(r'[a-zA-Z]{3}', str).group(0)
		self.show['year'] = int(re.search(r'\d{4}', str).group(0))

		if month in ['Jan', 'Feb', 'Mar']:
			self.show['season'] = 'Winter'
		elif month in ['Apr', 'May', 'Jun']:
			self.show['season'] = 'Spring'
		elif month in ['Jul', 'Aug', 'Sep']:
			self.show['season'] = 'Summer'
		else:
			self.show['season'] = 'Fall'
		
	
	def __mini_parse_date(self, season):
		season = season.text
		self.show['year'] = int(re.search(r'\d+$', season).group(0))
		
		if season[0] == 'S':
			if season[1] == 'p':
				self.show['season'] = 'Spring'
			else:
				self.show['season'] = 'Summer'
		elif season[0] == 'W':
			self.show['season'] = 'Winter'
		elif season[0] == 'F':
			self.show['season'] = 'Fall'
		else:
			self.show['season'] = None
		

	def __sleepy(self, time_s):
		if self.sleep:
			time.sleep(time_s)


	def __verbose(self, s):
		if self.verbose:
			print(s)
			
	@staticmethod
	def __decode(s):
		return quote(s)


""" Show Parameters

id : Show ID number
name : Name of show
name_ : Name of show with '_' instead of ' '
recs : list of linked ids
	* linked id : [id, count]
studio : main Studio producer
score : score rating (%f)
scored_by : people that rated the show
season : Season of premiere. Format [F|W|Sp|Su]{year}

END
"""



