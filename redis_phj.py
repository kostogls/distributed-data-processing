import redis

# read db tuples
def get_db_records(r1,r2):
    keys1 = r1.keys('*')
    keys2 = r2.keys('*')

    tuples1 = []
    tuples2 = []

    for key in keys1:
        tupla = (key, r1.get(key))
        tuples1.append(tupla)

    for key in keys2:
        tupla = (key, r2.get(key))
        tuples2.append(tupla)

    return keys1, keys2, tuples1, tuples2


def probe_insert(tpl, probe, insert, max_diff, lazy):
    # key of probe tuple
    probe_key = tpl[0]
    # insert tpl in the insert hash table for the probe key
    insert[probe_key] = tpl
    # store result as tuple
    result = ()
    # if probe key exists in probe hash table, get tuples
    if probe_key in probe:
        # check difference threshold if lazy method to save result tuple
        if lazy is True:
            time_diff = abs(float(probe[probe_key][1].decode()) - float(tpl[1].decode()))
            if time_diff < max_diff:
                result = (probe_key, probe[probe_key][1], tpl[1])
        else:
            result = (probe_key, probe[probe_key][1], tpl[1])

    return result


def phj(tuples1, tuples2, max_diff, lazy):
    # hash tables for db tuples
    htable1 = {}
    htable2 = {}
    # idxs to read db tuples
    idx1 = 0
    idx2 = 0

    join_result = []
    # while there are still tuples in the biggest db, probe and insert tuples in table1 and in table2
    while idx1 < len(tuples1) or idx2 < len(tuples2):
        if idx1 < len(tuples1):
            tpl = tuples1[idx1]
            result = probe_insert(tpl, htable2, htable1, max_diff, lazy)
            if result:
                join_result.append(result)
            idx1 = idx1 + 1

        if idx2 < len(tuples2):
            tpl = tuples2[idx2]
            result = probe_insert(tpl, htable1, htable2, max_diff, lazy)
            if result:
                join_result.append(result)
            idx2 = idx2 + 1

    return join_result

