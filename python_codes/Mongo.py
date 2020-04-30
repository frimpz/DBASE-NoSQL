from pymongo import MongoClient
client = MongoClient()

db = client.baseball


# find all
def find_query(collection, params):
    return collection.find(params)


def insert_many(collection, data_list):
    collection.drop()
    collection.insert_many(data_list)


# Sum of player hits
def group_batting_query():
    return db.Batting.aggregate(
        [
            {
                '$match': {
                    'yearID': {
                        '$gte': 00
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'player': '$playerID',
                        'year': '$yearID'
                        },
                    'hr': {'$sum': '$HR'}
                }
            },
            {
                '$lookup':
                    {
                        'from': 'People',
                        'localField': '_id.player',
                        'foreignField': 'playerID',
                        'as': 'person'
                    }
            },
            {
                '$project':
                    {
                        'id': '$_id.player',
                        'first_name': '$person.nameFirst',
                        'last_name': '$person.nameLast',
                        'year': '$_id.year',
                        'hr': '$hr'
                    }
            }
        ]
    )


# Sum of team hits
def project_team_query():
    return db.Teams.aggregate(
        [
            {
                '$match': {
                    'yearID': {
                        '$gte': 00
                    }
                }
            },
            {
                '$lookup':
                    {
                        'from': 'TeamsFranchises',
                        'localField': 'franchID',
                        'foreignField': 'franchID',
                        'as': 'team_franchise'
                    }
            },
            {
                '$project':
                    {
                        'team': '$teamID',
                        'year': '$yearID',
                        'hr': '$HR',
                        'team_name': '$team_franchise.franchName',
                    }
            }
        ]
    )


# Join Batting and Team Hits
def join_batting_team_query():
    return db.temp_bat.aggregate(
        [
            {
                '$match': {
                    'year': {
                        '$gte': 00
                    }
                }
            },
            {
                '$lookup':
                    {
                        'from': 'temp_team',
                        'localField': 'year',
                        'foreignField': 'year',
                        'as': 'team'
                    }
            },
            {
                "$unwind": "$team"
            },
            {
                '$project':
                    {
                        'player_id': '$id',
                        'player_yr': '$year',
                        'player_hr': '$hr',
                        'fname': '$fname',
                        'lname': '$lname',
                        'team_id': '$team.id',
                        'team_yr': '$team.year',
                        'team_hr': '$team.hr'
                    }
            },
            {
                '$match':
                    {
                        '$expr':
                            {
                                '$gt': ['$player_hr', '$team_hr']
                            }
                    }
            }
        ]
    )


#results = group_batting_query()
#temp_batting = []
#for row in results:
#    temp_batting.append({
#       'id': row['id'],
#       'fname': row['first_name'][0],
#       'lname': row['last_name'][0],
#       'year': row['year'],
#       'hr': row['hr']
#    })


#results = project_team_query()
#temp_team = []
#for row in results:
#    temp_team.append({
#      'id': row['team'],
#      'team_name': row['team_name'][0],
#      'year': row['year'],
#      'hr': row['hr'],
#    })


# insert_many(db.temp_bat, temp_batting)
#insert_many(db.temp_team, temp_team)


params = {}
#results = find_query(db.temp_bat, params)
#for row in results:
#    print(row)


#results = find_query(db.temp_team, params)
#for row in results:
#    print(row)


results = join_batting_team_query()
int= 0
for row in results:
    int = int + 1
    print(row)

print(int)




