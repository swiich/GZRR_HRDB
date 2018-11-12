# coding=utf-8

import sys

for i in sys.stdin:
    detail = i.strip().split('\t')
    print(detail[0])
    for k, v in eval(i[4]).items():
        if v/i[3] > 0.03:
            if (pow(10, (eval(i[5])[k]/10 - 107)/10)) / 1000 > i[-1]:
                with open('/illegal', 'a') as f:
                    f.write(str(k)+','+'1'+','+i[0]+','+i[2]+','+i[1])
                    f.write('\n')
