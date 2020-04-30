# Biclique enumeration with motifs
# In this code, motifs was used in finding the neighbours of the teams, (code on line 103)
# In the previous assignment, taking the neighbours from the dataframe each time, the code took about 20 minutes.
# but storing the graph in a list and the neighbours in a dictionary took about 2 minutes.
# In this implementation, using motifs alone to find the neighbours takes about 10minutes
# BUt using motifs with dictionary takes about 6 minutes.
# Overall I believe the previous implementation was faster because the dataframe was queried a few times and
# the whole graph stored in a list.
# I believe iteration over the list is faster than querying the graphframe multiple times for the neighbours.


import os
import sys
from collections import defaultdict

# Spark configuration for graph frames.
# Please uncomment this part to set configuration for graph frame

#SPARK_HOME = "/home/frimpz/spark-3.0.0"
#os.environ["SPARK_HOME"] = SPARK_HOME
#os.environ["PYSPARK_SUBMIT_ARGS"] = (
#    "--packages graphframes:graphframes:0.8.0-spark3.0-s_2.12 pyspark-shell"
#)
#sys.path.append(os.path.join(SPARK_HOME, "python"))
#sys.path.append(os.path.join(SPARK_HOME, "python/lib/py4j-0.10.8.1-src.zip"))
#import findspark
#findspark.init()

# configuration ends here.

from pyspark.sql import SparkSession
from graphframes import *

spark = SparkSession \
        .builder \
        .appName("Biclique Enumeration") \
        .getOrCreate()


def get_neighbours(src):
    teams_neighbours = _neighbors.filter('a.id == "' + src + '"').select('b').distinct().collect()
    return set([row['b'].id for row in teams_neighbours])


# Function to enumerate bicliques
def biclique(_l, _r, _p, _q):
    global bicliques
    while len(_p) > 0:
        # select a team node
        x = _p[0]
        r_comp = _r.union({x})
        l_comp = neighbours[x].intersection(_l)
        l_comp_prim = _l.difference(l_comp)
        c = {x}
        p_comp = set()
        q_comp = set()
        is_max = True
        for v in _q:
            neigh_v = l_comp.intersection(neighbours[v])
            if len(neigh_v) == len(l_comp):
                is_max = False
                break
            elif len(neigh_v) > 0:
                q_comp = q_comp.union({v})
        if is_max:
            for v in _p:
                if v is not x:
                    neigh_v = l_comp.intersection(neighbours[v])
                    if len(neigh_v) == len(l_comp):
                        r_comp = r_comp.union({v})
                        s = l_comp_prim.intersection(neighbours[v])
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


df = spark.read.load("hdfs://localhost:8020/user/baseball/Batting.csv", format="csv", header='true').select("playerID", "teamID", "yearID", "stint")
df.createOrReplaceTempView("relation")


edges = spark.sql("SELECT stint as id,  teamID as src, playerID as dst  FROM relation WHERE yearID  == '2018' ")\
    .select("id", "src", "dst")
players = spark.sql("SELECT distinct playerID as id FROM relation WHERE yearID  == '2018' ")\
    .select("id")
teams = spark.sql("SELECT distinct teamID as id FROM relation WHERE yearID  == '2018' ")\
    .select("id")

vertices = players.union(teams)


bicliques = []
g = GraphFrame(vertices, edges)
player_nodes = set([row.id for row in players.collect()])
team_nodes = [row.id for row in teams.collect()]


_neighbors = g.find("(a)-[]->(b)")
neighbours = defaultdict(set)


for x in team_nodes:
    neighbours[x] = get_neighbours(x)


l = player_nodes
r = set()
p = list(team_nodes)
q = set()
biclique(l, r, p, q)


# save to hdfs
rdd = spark.sparkContext.parallelize(bicliques)
rdd.saveAsTextFile("hdfs://localhost:8020/user/boadu/graphframe")


spark.stop()