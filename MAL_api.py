from bs4 import BeautifulSoup
# import html
import urllib.request
from urllib.parse import quote
import re
import time
import json
import atexit




class Extracter:
    def __init__(self, db_name=None, DB=False, file=False):
        self.fail_max = 400
        self.i = 0
        self.mode_db = DB
        self.mode_file = file
        if DB:
            self.db = db_name
            self.__open_db()
            # TODO Open DB
        if file:
            try:
                with open("shows.json", "r") as f:
                    self.i = re.search(r"\"id\": (\d+),", (f.readlines()[-1])).group(1)
                    self.i = int(self.i)
                    self.file = open("shows.json", "a+")
                    print("Resuming at index {}".format(self.i))
            except:
                self.file = open("shows.json", "w+")



    def retrieve(self, iter=-1, start_i=0, fail_limit=400, sleep=True, verbose=True):
        # --- Input --- #
        #   + verbose: Print information about actions. Default True
        #   + iter: number of max iterations to run. Default to -1 -> infinite
        #   + start_i: index to start looking from. Overwrites file i
        #   + fail_limit: maximum number of failures sequence allowed. Every failure decreases count by 1, every success increases by 1
        #   + sleep: sleep for around 1 second between calls.


        self.sleep = sleep
        if  start_i > 0:
            self.i = start_i - 1
        self.verbose = verbose


        if iter > 0:
            top = self.i + iter
        else:
            top = 0
        self.try_again = True

        if fail_limit > 0:
            self.proceed = fail_limit
            self.fail_max = fail_limit
        else:
            self.proceed = self.fail_max


        while self.proceed > 0 and not (top and self.i > top):
            if self.mode_file and self.i % 10 == 0:
                self.file.flush()

            self.show = {}
            self.__get_index()
            ret = self.__url_main()     # url_main returns 0 on success, 1 on failure
            if not ret:
                self.__url_rec()
                if self.verbose:
                    self.__talk(self.show)
                # self.__insert_to_shows()
                self.__sleepy(0.25)
            else:
                self.__sleepy(0.5)
                if self.try_again:
                    self.__sleepy(0.5)
                    self.try_again = False
                    self.i -= 1
                    self.proceed = min(self.proceed+2, self.fail_max)
                    continue

            self.proceed = min(self.proceed+1, self.fail_max)
            self.try_again = True

        if self.mode_file:
            self.file.close()


    def update_producers(self, verbose=False):
        url = "https://myanimelist.net/anime/producer"
        try:
            response = urllib.request.urlopen(url)
        except:
            print("Could not retrieve producer data")
            return
        soup = BeautifulSoup(response, 'lxml')
        producers = {}

        producer_file = open("producers.json", 'w+')

        for p in soup.find_all('a', href=re.compile(r'producer/\d+/')):
            link = p.get('href')
            id = re.search(r'\d+', link).group(0)
            name = re.search(r'\d+/.*$', link).group(0).replace('{}'.format(id), '').replace('/', '')
            producers[id] = name
            if verbose:
                print("ID: {} -- NAME: {}".format(id, name))
            producer_file.write('{"id" : %s, "name" : "%s"}\n' % (id, name))
            self.__insert_to_producers(id, name)

        producer_file.close()


    # --- URL functions --- #
    def __url_main(self):
        self.state = True
        url = "https://myanimelist.net/anime/{}".format(self.show['id'])
        try:
            response = urllib.request.urlopen(url)
        except:
            self.proceed -= 2
            self.__talk("{} - Could not retrieve anime: {}".format(self.proceed, self.i))
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

        # DB actions
        # TODO DB
        # self.__write_to_db()

        self.__write_to_file()

        return 0


    def __url_rec(self):
        url = self.soup.find('a', href=re.compile(r'/userrecs')).get('href')
        url = quote(url, safe=':/')
        try:
            response = urllib.request.urlopen(url)
        except:
            self.__talk("Could not retrieve recommendations: {}".format(self.i))
            self.show['recs'] = {}
            return
        soup_rec = BeautifulSoup(response, 'lxml')
        recommendations = []
        # Get all links
        for rec in soup_rec.find_all('a', href=re.compile('/\d+-\d+')):
            id = re.search(r'\d+-\d+'.format(self.i), rec.get('href')).group(0).replace("{}".format(self.i), "").replace("-", '')
            recommendations.append(id)

        # Get number of recs
        rec_count = [1] * len(recommendations)
        for i, val in enumerate(soup_rec.find_all('a', {'class':"js-similar-recommendations-button"})):
            rec_count[i] = int(val.find('strong').text) + 1

        dictionary = {}
        for rec, count in zip(recommendations, rec_count):
            dictionary[rec] = count

        self.show['recs'] = dictionary

    # ------------ Parsing Functions ------------ #

    def __find_name(self):
        link = str(self.soup.find('link'))
        self.show['name_'] = re.search(r'\d/([^"]+)', link).group(1)
        # self.show['name'] = self.show['name_'].replace('_', ' ')


    def __find_genre(self):
        genres = []
        for a in self.soup.find_all("a", href=re.compile(r'genre/(\d+)/')):
            genres.append(re.search(r'genre/(\d+)/', str(a)).group(1))
        self.show['genres'] = genres


    def __get_stats(self):
        # Score
        stats = self.soup.find_all('span', itemprop=["ratingValue", "ratingCount"]) # Score, count
        if not stats:
            self.__talk("Could not retrieve stats: {}".format(self.i))
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
        self.show['studio'] = re.search(r'/producer/(\d+)/', str(studio[-1])).group(1)


    def __get_season(self):
        season =  self.soup.find("a", href=re.compile(r'season/'))
        if not season:
            self.show['season'] = "X0000"
            return
        season = season.text
        year = re.search(r'\d+$', season).group(0)

        if season[0] == 'S':
            s = season[0:1]
        else:
            s = season[0]
        self.show['season'] = "{}{}".format(s, year)

    def __get_type(self):
        t = self.soup.find('a', href=re.compile('\?type='))
        if not t:
            self.show['type'] = "None"
            return
        self.show['type'] = t.text



    # ------------ Database/File functions ------------ #
    def __open_db(self):
        print("TODO")


    def __get_index(self):  #TODO getid
        self.i += 1
        self.show['id'] = self.i


    def __insert_to_producers(self, key, value):
        if not self.mode_db:
            return 1

        prompt = 'INSERT INTO Producers ({}, "{}");'.format(key, value)
        #TODO


    def __write_to_file(self):
        if self.mode_file:
            self.file.write("{}\n".format(self.show).replace("'", '"'))
        if self.i % 10 == 0:
            self.__talk("Flushed...")
            self.file.flush()

    # ------------ Other functions ------------ #

    def __sleepy(self, time_s):
        if self.sleep:
            time.sleep(time_s)

    def __talk(self, s):
        if self.verbose:
            print(s)

    def __decode(self, s):
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

END """



