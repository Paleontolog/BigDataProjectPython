import time
import pandas as pd
import json
import os

data = pd.read_csv(r"D:\PycharmProjects\BIG_DATA\gun-violence-data_01-2013_03-2018.csv")

cur = os.path.abspath(".")
cur = cur[:cur.rfind("\\")]
for i in os.listdir(f"{cur}/emulation"):
    os.remove(f"{cur}\\emulation\\{i}")

def write_f(line):
    with open(r"{}\emulation\{}.json".format(cur, line["incident_id"]), "w") as w:
        json.dump(line, w)
    time.sleep(1)


for row in data.iterrows():
    write_f(dict(row[1]))
