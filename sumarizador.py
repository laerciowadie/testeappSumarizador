from cassandra.cluster import Cluster
from pysparkling import Context
from datetime import datetime
import random


cluster = Cluster()
session = cluster.connect('twitterapp')
tweetsRDD = iter(session.execute("select * from tweet"))
print('#### 5 usuarios com mais seguidores ####')

def inserirTopFiveUsers(x):
        session.execute("insert into topfiveusers (uuid, username, userfollowers) values (%s, %s, %s)", (random.randrange(10000, 30000), x.username, x.userfollowers))

mostFollowersRDD = sorted(tweetsRDD, key=lambda tweet: tweet[5], reverse=True)
count = 0
currentUser = ''
for i in mostFollowersRDD:
        if(currentUser != i.username):
                print(i.username, i.userfollowers)
                inserirTopFiveUsers(i)
                count+=1
                currentUser = i.username
        if(count ==5):
                break

print('#### quantidade de lang=pt por hashtag #####')

def inserirByTag(x):
        print(x)
        session.execute("insert into resumebytag (uuid, hashtag, count) values (%s, %s, %s)", (random.randrange(10000, 30000), x[0], x[1]))

testeRDD = filter(lambda x: x[3] == 'pt', mostFollowersRDD)
teste2RDD = map(lambda x: (x[2], 1), testeRDD)

sc = Context()

teste3RDD = sc.parallelize(teste2RDD)

teste4RDD = teste3RDD.reduceByKey(lambda accum, n: accum + n)
teste4RDD.foreach(inserirByTag)


print('#### total de postagens/hora do dia #####')

def inserirByDayHour(x):
        print(x)
        session.execute("insert into resumebydayhour (uuid, dayhour, count) values (%s, %s, %s)", (random.randrange(10000, 30000), x[0], x[1]))

teste5RDD = sc.parallelize(mostFollowersRDD)

def agruparDate(x):
        diaHora = '{:%Y-%m-%d %H}'.format(x[1])
        return (diaHora,1)

teste6RDD = teste5RDD.map(agruparDate).reduceByKey(lambda accum, n: accum + n)
teste6RDD.foreach(inserirByDayHour)


