import redis
import time
import redis_config as rc


def semi_join(redis1, redis2, max_diff, lazy=True):
    # get keys of the small relation
    keys_of_small_relation = get_keys_of_small_relation(redis1, redis2)
    join_result = []

    # for each key in the small relation
    for key in keys_of_small_relation:
        # try to fetch value for each key
        value1 = redis1.get(key)
        value2 = redis2.get(key)

        # if key exists in both relations, join them
        if value1 is not None and value2 is not None:
            value1 = value1.decode()
            value2 = value2.decode()
            if lazy is True:
                if abs(float(value1) - float(value2)) <= max_diff:
                    join_result.append((key, value1, value2))
            else:
                join_result.append((key, value1, value2))

    return join_result


def get_keys_of_small_relation(redis1, redis2):
    # get number of keys from both relations
    n_redis1_keys = redis1.dbsize()
    n_redis2_keys = redis2.dbsize()

    # return keys of the smallest one
    if n_redis1_keys >= n_redis2_keys:
        keys_of_small_relation = redis2.keys('*')
    elif n_redis1_keys < n_redis2_keys:
        keys_of_small_relation = redis1.keys('*')

    return keys_of_small_relation

