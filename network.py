import networkx as nx
import scipy.sparse
import json
import pymysql
import numpy as np
import matplotlib.pyplot as plt
import pickle

class MALNetwork:
	def __init__(self, db=None, min_rec=5):
		self.db = None
		self.sparse = None
		self.graph = nx.Graph()
		self.sparse = None
		self.min_rec=min_rec
		if db is not None:
			self.__open_db(db)
	
	
	def build_network(self):
		# DB only for now
		print('Building network')
		if self.db is not None:
			self.__populate_nodes()
			self.__populate_edges()
	
	def save_graph(self, path='data/graph.pickle'):
		print('Saving graph to: {}'.format(path))
		with open(path, 'wb') as file:
			pickle.dump(self.graph, file)
	
	def load_graph(self, path='data/graph.pickle'):
		print('Loading graph from: {}'.format(path))
		with open(path, 'rb') as file:
			self.graph = pickle.load(file)
			
	def plot_graph(self, path='data/graph.png'):
		print('Drawing Graph')
		c_range = self.graph.nodes(data='nrec', default=1)
		c_range = np.sqrt(c_range)[:,1]
		c_range = c_range / np.max(c_range)
		
		print('\tSpring layout...')
		layout = nx.shell_layout(self.graph)#, k=0.11, iterations=1)
		
		# Formatting plot
		plt.title('MyAnimeList Recommendations web')

		print('\tForming plot...')
		cmap = plt.cm.inferno_r
		nx.draw(self.graph, layout, node_size=1, alpha=0.25, width=0.03, node_color=c_range, cmap=cmap)
		
		print('\tSaving high-res graph...')
		plt.savefig(path, dpi=3000)
		
		plt.show()
		print('-- Finished drawing graph --')
	
	
	
	def get_sparse(self):
		print('Getting sparse matrix')
		self.sparse = nx.to_scipy_sparse_matrix(self.graph)
	
	def save_sparse(self, path='data/sparse.pickle'):
		print('Saving sparse from: {}'.format(path))
		with open(path, 'wb') as file:
			pickle.dump(self.sparse, file)
			
	def load_sparse(self, path='data/sparse.pickle'):
		print('Loading graph from: {}'.format(path))
		with open(path, 'rb') as file:
			self.sparse = pickle.load(file)
	
	def plot_sparse(self, path='data/sparse.png'):
		print('Plotting sparse matrix')
		plt.spy(self.sparse)
		# plt.colorbar()
		plt.savefig(path)
		
		
	# ---- Helpers ---- #
	# DB/File Helpers
	def __open_db(self, content):
		self.db = pymysql.connect(
			host='localhost',
			user=content['user'],
			password=content['password'],
			db=content['database'],
			charset='utf8mb4',
			port=content['port'],
			cursorclass=pymysql.cursors.DictCursor
		)

		
	def __populate_nodes(self):
		# Populate graph with nodes containing all data stores in a show
		sql = 'SELECT * FROM mal_show WHERE (idx BETWEEN %s AND %s) AND nrec > %s'
		
		i = 0
		delta = 250
		while True:
			print('Populating nodes %d - %d...' % (i, i + delta-1))
			i += delta
			with self.db.cursor() as cursor:
				cursor.execute(sql, (i, i + delta-1, self.min_rec))
				response = cursor.fetchall()
				if not response:
					break
			
			# Iterate through rows creating graph
			for row in response:
				show_id = row.pop('show_id')
				nrec = row.pop('nrec')
				self.graph.add_node(show_id, nrec=nrec, dict=row)
			
		print('-- Finished populating nodes --')
		
		
	def __populate_edges(self):
		sql = 'SELECT * FROM mal_rec WHERE show_id in ' \
		       '(SELECT show_id FROM mal_show WHERE nrec > %s AND nrec < 30)'

		print('Populating edges...')
		with self.db.cursor() as cursor:
			cursor.execute(sql, (self.min_rec))
			response = cursor.fetchall()
		
		self.graph.add_weighted_edges_from([[row['show_id'], row['recommended_id'], row['count']]
			                                    for row in response])
	
	print('-- Finished populating edges --')
