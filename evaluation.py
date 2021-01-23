import pandas as pd
import numpy as np

pd.set_option("display.max_rows", None, "display.max_columns", None)

k = 10

ranking = pd.read_csv("result1.txt", sep="\t", header=None)
ranking.columns = ['owner_user_id', 'score']
ranking['rel'] = 0
judgements = pd.read_csv('Resources/GoldenSet.csv')
judgements.columns = ['query', 'owner_user_id']
judgements = judgements.query('query == "sockets"')
ca_rel_list = judgements['owner_user_id'].tolist()
ca_rel_list = [str(id) for id in ca_rel_list]

rel_ret_count = 0

for index, row in ranking.iterrows():
    if row['owner_user_id'] in ca_rel_list:
        rel_ret_count += 1
        ranking.at[index, "rel"] = 1


rel = ranking.head(k)

_pAtk = np.sum(rel['rel']) / k
print(rel.head(10))
print("=============================================")
print(":: P@10 Metric for this ranking is => ", _pAtk)