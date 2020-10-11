# -*- coding: utf-8 -*-
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
import hashlib


def balance_join(spark, left, right, join_key, id_col, config):
    def _draw_sketch(keys):
        indicate_udfs = [
            F.udf(lambda k: 1 if k % 2 == 0 else -1, T.IntegerType()),
            F.udf(lambda k: 1 if k % 2 == 1 else -1, T.IntegerType()),
        ]
        for i in range(len(hash_udfs)):
            keys = keys.withColumn('h' + str(i), hash_udfs[i]('d_key')) \
                .withColumn('g' + str(i), indicate_udfs[i % 2]('d_key'))

        schema = T.StructType([T.StructField("hash_val", T.IntegerType(), True)])
        hash_list = [(i,) for i in range(0, config['hash']['range'])]
        sketch = spark.createDataFrame(hash_list, schema=schema)
        for i in range(len(hash_udfs)):
            hash_keys = keys.select('h' + str(i), 'g' + str(i)) \
                .groupBy('h' + str(i)) \
                .agg(F.sum('g' + str(i)).alias(str(i))) \
                .withColumnRenamed('h' + str(i), 'hash_val')
            sketch = sketch.join(hash_keys, 'hash_val', 'left')
        sketch = sketch.fillna(0)
        return sketch

    def _estimate_workload(sketch_l, sketch_r):
        for i in range(len(hash_udfs)):
            sketch_l = sketch_l.withColumnRenamed(str(i), 'l' + str(i))
            sketch_r = sketch_r.withColumnRenamed(str(i), 'r' + str(i))
        sketch = sketch_l.join(sketch_r, 'hash_val')
        sketch.cache()
        sum_cols = [
            F.sum((F.col('l' + str(i)) * F.col('r' + str(i)))) for i in range(len(hash_udfs))
        ]
        total_load = sketch.select(sum_cols).first()
        workloads = [l for l in enumerate(total_load)]
        workloads.sort(key=lambda x: x[1])
        median_idx = workloads[int(len(hash_udfs) / 2)][0]
        median_load_total = workloads[int(len(hash_udfs) / 2)][1]
        avg_load = int(median_load_total / n_workers)
        median_load = sketch.select('hash_val',
                                    (F.col('l' + str(median_idx)) * F.col('r' + str(median_idx))).alias('load')) \
            .withColumn('avg_load', F.lit(avg_load)) \
            .withColumn('n_part', F.ceil(F.col('load') / F.col('avg_load'))) \
            .select('hash_val', 'n_part')
        return hash_udfs[median_idx], median_load

    _hash2int_udf = F.udf(lambda s: int(hashlib.sha1(s.encode('utf-8')).hexdigest(), 16) % config['hash']['range'],
                          T.IntegerType())

    hash_udfs = []
    for a, b in config['hash']['params']:
        udf = F.udf(lambda k: (a * k + b) % config['hash']['range'], T.IntegerType())
        hash_udfs.append(udf)

    n_workers = config['n_workers']
    # todo: join multiple keys
    left = left.withColumn('d_key', _hash2int_udf(join_key))
    left.cache()
    right = right.withColumn('d_key', _hash2int_udf(join_key))
    sketch_left = _draw_sketch(left)
    sketch_right = _draw_sketch(right)
    hash_udf, workload = _estimate_workload(sketch_left, sketch_right)
    rank_window = Window.partitionBy('hash_val').orderBy(id_col)
    get_suffix_udf = F.udf(lambda m, p: str(m) if p > 1 else '', T.StringType())
    left = left.withColumn('hash_val', hash_udf('d_key')) \
        .join(workload, 'hash_val', 'left') \
        .withColumn('rank', F.rank().over(rank_window)) \
        .withColumn('mod', F.col('rank') % F.col('n_part')) \
        .withColumn('suffix', get_suffix_udf('mod', 'n_part')) \
        .withColumn('nk', F.concat_ws('$', join_key, 'suffix')) \
        .drop('rank', 'mod', 'hash_val', 'd_key')
    left.cache()
    right = left.select(join_key, 'suffix') \
        .dropDuplicates() \
        .join(right, join_key) \
        .fillna('') \
        .withColumn('nk', F.concat_ws('$', join_key, 'suffix')) \
        .drop('suffix')
    join_result = left.drop('suffix').join(right, 'nk').drop('nk', 'n_part', 'd_key')
    return join_result
