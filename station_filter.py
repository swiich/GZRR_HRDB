import pandas as pd
import re


def split(x):
    if ' ' in x:
        obj = re.match(r'(.*/.*/.* .*:.*:00)(.*)', x)
        return obj
    else:
        obj = re.match(r'(00:00)(.*)', x)
        return obj


df = pd.read_csv('sta.csv', encoding='UTF-8', error_bad_lines=False, header=None)
print(df.loc[48089])
# df[15] = df[15].astype(str)
# df[15] = df[15].apply(split)
# print(df[15].loc[48085:48090])

# for i,v in enumerate(df[15]):
#     print(v, i)
#     print(v.group(1))
# string = '00:0025.428379999999997'
# import re
# obj = re.match(r'(00:00)(.*)', string)
# print(obj.group(1), obj.group(2))
