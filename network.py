import networkx
import scipy.sparse
import json

def create_sparse_matrix(path="data/shows.json"):
	matrix = scipy.sparse.coo_matrix()
	with open(path, 'r') as file:
		for line in file.readlines():
			show = json.loads(line)
			print(show)


def create_graph():
	pass

create_sparse_matrix()