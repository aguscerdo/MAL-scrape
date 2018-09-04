import network
import json

def main(db):
	net = network.MALNetwork(db=db, min_rec=0)
	net.build_network()
	net.save_graph()
	# net.load_graph()
	net.plot_graph()
	
	# net.get_sparse()
	# net.save_sparse()
	# net.load_sparse()
	# net.plot_sparse()


if __name__ == '__main__':
	with open('db_config.json', 'r') as file:
		db = json.loads(file.read())
	main(db)