import pandas as pd


#Add name of file here
data_file = 'zto.csv'
sales = pd.read_csv(data_file)

file = open("zto.txt","w")
file.write("date close volume open high low\n")
for i,j in sales.iterrows():
    file.write("%s %s %s %s %s %s\n" %(j['date'],j['close'],j['volume'],j['open'],j['high'],j['low']))