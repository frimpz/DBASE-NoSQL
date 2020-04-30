from collections import defaultdict

# Biclique Enumeration for NoSql Assignment 4
# By Frimpong Boadu
# Implemented IMBEA Approach to biclique enumeration.
from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("Biclique Enumeration") \
        .getOrCreate()


# Function is used to retrieve all the neighbours of vertex v
def get_neighbours(v):
    return neighbours[v]


# Function to enumerate bicliques
def biclique(_l, _r, _p, _q):
    global bicliques
    while len(_p) > 0:
        # select a team node
        x = _p[0]
        r_comp = _r.union({x})
        l_comp = get_neighbours(x).intersection(_l)
        l_comp_prim = _l.difference(l_comp)
        c = {x}
        p_comp = set()
        q_comp = set()
        is_max = True
        for v in _q:
            neigh_v = l_comp.intersection(get_neighbours(v))
            if len(neigh_v) == len(l_comp):
                is_max = False
                break
            elif len(neigh_v) > 0:
                q_comp = q_comp.union({v})
        if is_max:
            for v in _p:
                if v is not x:
                    neigh_v = l_comp.intersection(get_neighbours(v))
                    if len(neigh_v) == len(l_comp):
                        r_comp = r_comp.union({v})
                        s = l_comp_prim.intersection(get_neighbours(v))
                        if len(s) == 0:
                            c = c.union({v})
                    elif len(neigh_v) > 0:
                        p_comp = p_comp.union({v})
            if len(r_comp) > 1:
                bicliques.append((l_comp, r_comp))
            if len(p_comp) > 0:
                biclique(l_comp, r_comp, list(p_comp), q_comp)
        _q = _q.union(c)
        _p = list(set(_p).difference(c))


df = spark.read.load("hdfs://localhost:9000/user/baseball/Batting.csv", format="csv", header='true').select("playerID", "teamID", "yearID")
df.createOrReplaceTempView("relation")


# collecting all player and teams for 2018
edges = spark.sql("SELECT  playerID, teamID FROM relation WHERE yearID  == '2018' ")\
    .select("playerID", "teamID").collect()

# Bipartite graph, with players and teams.
G = list([(x['playerID'], x['teamID']) for x in edges])


# list of bicliques
bicliques = []


# get all player nodes into player node set
# get all team nodes into team node sets
# add all neighbours of each team in memory, this reduces computation from about 20 mins to less than 1 min
neighbours = defaultdict(set)
player_nodes = set()
team_nodes = set()
for player, team in G:
    player_nodes.add(player)
    team_nodes.add(team)
    neighbours[team].add(player)

# l set of player nodes.
# r set of team nodes that belongs to the current biclique, initially empty.
# p set of team nodes, sorted by non decreasing order of common neighbourhood size.
# set of vertices used to determine maximality

l = player_nodes
r = set()
p = list(team_nodes)
q = set()

biclique(l, r, p, q)

# save to hdfs
rdd = spark.sparkContext.parallelize(bicliques)
rdd.saveAsTextFile("hdfs://localhost:9000/user/boadu/spark")


spark.stop()
