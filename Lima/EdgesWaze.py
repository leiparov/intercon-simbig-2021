import geopandas as gpd
import pandas as pd
from shapely import wkt
import json
import pandas as pd
import osmnx as ox
import matplotlib.pyplot as plt
import osmnx as ox
import networkx as nx
import GOSTnets as gn
import logging
import datetime
import WazeRouteCalculator
from multiprocessing import Pool, cpu_count



######### Logger Inicio
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
log_filename = f"logs/SanBorjaEdgesWaze1000_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

logFormatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(name)s :: %(process)d :: %(message)s')
logger = logging.getLogger()

fileHandler = logging.FileHandler(log_filename)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

######### Logger Fin
logger.info(f"EdgesWaze Process Started at {datetime.datetime.now()}")

logger.info("Loading NODES file")
nodesfile = "data/GrafoSanBorja_nodes.csv"
edgesfile = "data/GrafoSanBorja_edges.csv"

nodes = pd.read_csv(nodesfile, index_col=[0])
nodes['geometry'] = nodes['geometry'].apply(wkt.loads)
nodes = gpd.GeoDataFrame(nodes, crs='epsg:4326')
# nodes

logger.info("Loading EDGES file")
edges = pd.read_csv(edgesfile, index_col=[0,1,2])
edges['geometry'] = edges['geometry'].apply(wkt.loads)
edges = gpd.GeoDataFrame(edges, crs='epsg:4326')
# edges.head()


def getTimeDelta ():
    """
    hour: hour to leave at
    return: minutes
    """
    now = datetime.datetime.now()
    target_time = datetime.datetime(2023, 8, 7, 10, 0, 0, 0) #Lunes 07/08 a las 08:00
    time_diff =  target_time - now
    minutes = int(time_diff.total_seconds()/60)
    return minutes, time_diff

def getWazeRouteInfo(node_a, node_b):
    """
    node_a: node origin from graph
    node_b: node destination from graph
    """
    try:
        logger.info(f"Waze info for node_a {node_a} to node_b {node_b} started.")

        point_a_lat, point_a_lon = nodes.loc[node_a,:].geometry.y , nodes.loc[node_a,:].geometry.x
        point_b_lat, point_b_lon = nodes.loc[node_b,:].geometry.y , nodes.loc[node_b,:].geometry.x
        from_address = '%s, %s' % (point_a_lat, point_a_lon)

        to_address = '%s, %s' % (point_b_lat, point_b_lon)
        from_address , to_address

        route = WazeRouteCalculator.WazeRouteCalculator(from_address, to_address)
        
        time_delta, time_diff = getTimeDelta() #Calculamos time_delta para setear las 08:00 del día

        date4waze = datetime.datetime.now() + datetime.timedelta(minutes=time_delta)
        logger.info(f"Waze - Fixed traffic date at: {date4waze}" )
        
        minutes, distance = route.calc_route_info(time_delta=time_delta)
        
        logger.info(f"Waze info for node_a {node_a} to node_b {node_b} completed.")
    except Exception as e:
        logger.info(f"Waze - Error for node_a {node_a} to node_b {node_b}.")
        logger.info(e)
        minutes, distance = -1, -1
    return minutes, distance


def process_row(row):
    result = getWazeRouteInfo(row.u, row.v)
    with open('SanBorjaEdgesWaze1000_Recupero01.txt', 'a') as f:
        linea = f"{row.name};{row.u};{row.v};{result}\n"
        f.write(linea)
    return result

def split_dataframe(df, chunk_size):
    chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
    return chunks

def apply_chunk(chunk):
    return chunk.apply(process_row, axis=1)

########## Recupero Inicio ##########
# edgeswaze = pd.read_csv('LimaEdgesWaze1400_Final.txt', sep=';', index_col=0,names=['u','v','wazeinfo'])
# print(edgeswaze.shape)
# recupero_index = edgeswaze[edgeswaze['wazeinfo'] == '(-1, -1)'].index
# logger.info("Recupero de Edges")
# logger.info(len(recupero_index))
########## Recupero Fin ##########



# Define the chunk size based on your dataset size and available resources
chunk_size = 1000
edges2 = edges.reset_index()
inputdf = edges2
# inputdf = edges2.iloc[recupero_index, :]

data_chunks = split_dataframe(inputdf, chunk_size)

# Get the number of available CPU cores
# num_cores = cpu_count()
num_cores = int(inputdf.shape[0]/chunk_size)+1
if num_cores > cpu_count():
    num_cores = cpu_count()
print(f"Num Cores --> {num_cores}")

# # Create a multiprocessing pool with the number of cores
with Pool(num_cores) as pool:
    logger.info("Multiprocessing started.")
    results = pool.map(apply_chunk, data_chunks)
    logger.info("Multiprocessing completed.")

logger.info(f"EdgesWaze Process Finished at {datetime.datetime.now()}")
