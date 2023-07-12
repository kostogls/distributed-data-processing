import redis
import random
from datetime import datetime, timedelta
import redis_config as rc

rCC= rc.RedisConnectionConfig()
r1 = redis1 = redis.Redis(host=rCC.redis1Host, port=rCC.redis1Port, db=rCC.db1)
r2 = redis.Redis(host=rCC.redis2Host, port=rCC.redis2Port, db=rCC.db2)

# Flush (delete) all data from db1
r1.flushdb()

# Flush (delete) all data from db2
r2.flushdb()

def db_insertion(NUM_RECORDS1, EXTRANUM_RECORDS2, SAME_THRES, RANDOMNUM_LIMIT):
    kwargs1 = {'year': 1980, 'month':1, 'day':1}
    kwargs2 = {'year': 2022, 'month':12, 'day':31}

    # Generate and store random records
    keys1 = []
    keys2 = []

    for i in range(NUM_RECORDS1):
        # Generate a random timestamp within a range
        start_date = datetime(**kwargs1)
        end_date = datetime(**kwargs2)
        random_date1 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        random_date2 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

        timestamp1 = random_date1.timestamp()
        timestamp2 = random_date2.timestamp()

        # Generate random key and values

        key1 = random.randint(1, RANDOMNUM_LIMIT)

        key2 = key1 if random.random() < SAME_THRES else random.randint(RANDOMNUM_LIMIT, RANDOMNUM_LIMIT+1000)

        # store the record in the Redis databases
        r1.set(key1, timestamp1)
        r2.set(key2, timestamp2)
        keys1.append(key1)
        keys2.append(key2)

    for i in range(EXTRANUM_RECORDS2):
        start_date = datetime(**kwargs1)
        end_date = datetime(**kwargs2)
        random_date2 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        timestamp2 = random_date2.timestamp()

        key2 = random.randint(1, RANDOMNUM_LIMIT)
        r2.set(key2, timestamp2)
        keys2.append(key2)

    print("#Tuples in db1: ", len(keys1))
    print("#Tuples in db2: ", len(keys2))
    print("#same keys", len(list(set(keys1).intersection(keys2))))
    #
    print("Data insertion completed!")
