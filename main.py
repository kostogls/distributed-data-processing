from datetime import datetime
import time
import redis_config as rc
import redis
from redis_semi import semi_join
from redis_phj import get_db_records, phj
from db import db_insertion

# connect to db
rCC = rc.RedisConnectionConfig()
r1 = redis.Redis(host=rCC.redis1Host, port=rCC.redis1Port, db=rCC.db1)
r2 = redis.Redis(host=rCC.redis2Host, port=rCC.redis2Port, db=rCC.db2)

# set global parameters
MAX_DIFF = 10000000
NUM_RECORDS1 = 20000
EXTRANUM_RECORDS2 = 1000
SAME_THRES = 0.5
RANDOMNUM_LIMIT = 100000

removePairsBefore = False

if __name__ == '__main__':
    startTime = int(time.time() * 1000)
    db_insertion(NUM_RECORDS1, EXTRANUM_RECORDS2, SAME_THRES, RANDOMNUM_LIMIT)
    print("Insertion time:", int(time.time() * 1000) - startTime, "ms")

    ######## run semi join #########

    startTime = int(time.time() * 1000)
    res = semi_join(r1, r2, max_diff=MAX_DIFF, lazy=True)
    # print(res)
    print(len(res))
    print("Execution time:", int(time.time() * 1000) - startTime, "ms")

    ######## run PHJ #########
    keys1, keys2, tuples1, tuples2 = get_db_records(r1, r2)
    startTime = int(time.time() * 1000)
    jr = phj(tuples1, tuples2, max_diff=MAX_DIFF, lazy=True)
    # print(jr)
    print(len(jr))

    print("Execution time:", int(time.time() * 1000) - startTime, "ms")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
