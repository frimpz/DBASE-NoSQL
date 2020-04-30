from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("python_codes").setMaster("local")
sc = SparkContext(conf=conf)


def compare(l):
    try:
        if to_int(l[1][0][1]) > to_int(l[1][1][1]):
            return l
    except:
        pass


def to_int(x):
    try:
        return float(x)
    except ValueError:
        return 0.0


# File reading
batting = sc.textFile("hdfs://localhost:9000/user/baseball/Batting.csv")
people = sc.textFile("hdfs://localhost:9000/user/baseball/People.csv")
teams = sc.textFile("hdfs://localhost:9000/user/baseball/Teams.csv")
teamFranch = sc.textFile("hdfs://localhost:9000/user/baseball/TeamsFranchises.csv")


batting_split = batting.map(lambda line: line.split(",")).map(lambda ss: (ss[0], (ss[1], ss[11])))
people_split = people.map(lambda line: line.split(",")).map(lambda ss: (ss[0], (ss[13] + " " + ss[14])))
batting_people_join = batting_split.join(people_split).map(lambda l: (l[0] + " " + l[1][0][0], l[1][0][1])).reduceByKey(lambda a, b: int(a) + int(b))
batting_people_join_temp = batting_split.join(people_split).map(lambda l: (l[0] + " " + l[1][0][0], (l[1][0][0], l[1][0][1], l[1][1])))

hr_players = batting_people_join_temp.join(batting_people_join).map(lambda l: (l[1][0][0], (l[1][0][2], l[1][1])))

teams_split = teams.map(lambda line: line.split(",")).map(lambda ss: (ss[3], (ss[0], ss[19])))
teamFranch_split = teamFranch.map(lambda line: line.split(",")).map(lambda ss: (ss[0], (ss[1])))
teams_franch_join = teams_split.join(teamFranch_split).map(lambda l: (l[1][0][0], (l[1][1], l[1][0][1])))


final = hr_players.join(teams_franch_join).filter(compare).distinct().map(lambda l: l[1][1][0] +" "+ l[1][0][0] +" "+ l[0])
# write final output to hdfs
final.coalesce(1).saveAsTextFile("hdfs://localhost:9000/user/boadu/spark")





