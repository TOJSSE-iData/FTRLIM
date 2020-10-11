# -*- coding: utf-8 -*-
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
from collections import defaultdict
from .balance import balance_join


def find_unique(left, right, id_col, key_col, l_db, r_db):
    def cache_df(d):
        if not d.is_cached:
            d.cache()
            return True
        return False

    un_cache_l = cache_df(left)
    un_cache_r = cache_df(right)

    count_window = Window.partitionBy(key_col).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    single_l = left.withColumn('cnt', F.size(F.collect_set(id_col).over(count_window))) \
        .filter(F.col('cnt') == 1) \
        .drop('cnt') \
        .withColumn('db', F.lit(l_db))
    single_r = right.withColumn('cnt', F.size(F.collect_set(id_col).over(count_window))) \
        .filter(F.col('cnt') == 1) \
        .drop('cnt') \
        .withColumn('db', F.lit(r_db))
    unique = single_l.unionByName(single_r) \
        .withColumn('cnt', F.size(F.collect_set('db').over(count_window))) \
        .filter(F.col('cnt') == 2) \
        .drop('cnt')
    # unique.cache()
    #
    # unique_l = unique.filter(F.col('db') == l_db) \
    #     .drop('db')
    # unique_r = unique.filter(F.col('db') == r_db) \
    #     .drop('db')

    if un_cache_l:
        left.unpersist()
    if un_cache_r:
        right.unpersist()

    # return unique_l, unique_r
    return unique


def partition(spark, partition_config):
    unique_table = partition_config['unique_to']
    spark.sql("DROP TABLE IF EXISTS {}".format(unique_table))

    source = partition_config['source_tag']
    target = partition_config['target_tag']

    full = {}
    full[source] = spark.read.table(partition_config['full_source']) \
        .select('sub') \
        .dropDuplicates() \
        .withColumn('key', F.lit(''))
    full[target] = spark.read.table(partition_config['full_target']) \
        .select('sub') \
        .dropDuplicates() \
        .withColumn('key', F.lit(''))

    for p_conf in partition_config['partition_by']:
        no_attr = {}
        for src in [source, target]:
            p_by = spark.read.table(p_conf[src]) \
                .select('sub', F.lower(F.col('obj')).alias('new_key'))
            profile = full[src].join(p_by, 'sub', 'left')
            profile.cache()
            no_attr[src] = profile.filter(F.col('new_key').isNull()) \
                .select('sub', 'key')
            f = profile.filter(F.col('new_key').isNotNull()) \
                .select('sub', 'key', F.concat_ws('$', 'key', 'new_key').alias('new_key'))
            f.cache()
            full[src] = f

        unique = find_unique(full[source], full[target], 'sub', 'new_key', source, target) \
            .select('sub', 'db', F.col('new_key').alias('key'))
        unique.write.saveAsTable(unique_table, mode='append')

        # deal with ns
        t_k = full[target].select('key', 'new_key') \
            .dropDuplicates()
        n_s = no_attr[source].withColumn('new_key', F.lit('NULL')) \
            .select('sub', 'key', F.concat_ws('$', 'key', 'new_key').alias('new_key'))
        n_s_t = no_attr[source].join(t_k, 'key')
        # deal with nt
        s_k = full[source].select('key', 'new_key') \
            .dropDuplicates()
        n_t = no_attr[target].withColumn('new_key', F.lit('NULL')) \
            .select('sub', 'key', F.concat_ws('$', 'key', 'new_key').alias('new_key'))
        n_t_s = no_attr[target].join(s_k, 'key')

        full[source] = full[source].unionByName(n_s) \
            .unionByName(n_s_t) \
            .select('sub', F.col('new_key').alias('key'))
        full[target] = full[target].unionByName(n_t) \
            .unionByName(n_t_s) \
            .select('sub', F.col('new_key').alias('key'))

    full_s = full[source].withColumn('db', F.lit(source))
    full_t = full[target].withColumn('db', F.lit(target))
    result = full_s.unionByName(full_t)
    result.write.saveAsTable(partition_config['partition_to'], mode='overwrite')
    spark.catalog.clearCache()


def block(spark, block_config):
    def combine_words(words):
        keys = defaultdict(list)
        for w in words:
            keys[w[1]].append(w[0])
        ranks = sorted(list(keys.keys()))
        if len(ranks) == 1:
            res = keys[ranks[0]]
        else:
            res = []
            for i, r in enumerate(ranks[:-1]):
                for x in keys[r]:
                    for j in range(i + 1, len(ranks)):
                        for y in keys[ranks[j]]:
                            res.append(x + '@' + y)
        return res

    rm_empty_udf = F.udf(lambda x: [s for s in x if s != ''], T.ArrayType(T.StringType()))
    combine_words_udf = F.udf(lambda x: combine_words(x), T.ArrayType(T.StringType()))
    sum_int_l_udf = F.udf(lambda x: sum(x), T.IntegerType())

    source = block_config['source_tag']
    target = block_config['target_tag']

    full = {}

    if 'partition_table' in block_config.keys():
        full[source] = spark.read.table(block_config['partition_table']) \
            .filter(F.col('db') == source) \
            .select('sub', 'key')
        full[target] = spark.read.table(block_config['partition_table']) \
            .filter(F.col('db') == target) \
            .select('sub', 'key')
    else:
        full[source] = spark.read.table(block_config['full_source']) \
            .select('sub') \
            .dropDuplicates() \
            .withColumn('key', F.lit(''))
        full[target] = spark.read.table(block_config['full_target']) \
            .select('sub') \
            .dropDuplicates() \
            .withColumn('key', F.lit(''))
        spark.sql('drop table if exists {}'.format(block_config['unique_to']))

    row_window = Window.partitionBy('sub').orderBy('obj')
    rank_window = Window.partitionBy('sub', 'row').orderBy('count')

    for b_conf in block_config['block_by']:
        b_bys = {}
        counts = {}
        for src in [source, target]:
            pattern = b_conf[src]['pattern']
            max_len = b_conf[src]['max_len']
            b = spark.read.table(b_conf[src]['table']) \
                .select('sub', F.lower(F.col('obj')).alias('obj')) \
                .withColumn('row', F.row_number().over(row_window)) \
                .withColumn('split', rm_empty_udf(F.split('obj', pattern))) \
                .withColumn('threshold', F.lit(max_len)) \
                .withColumn('word', F.explode('split')) \
                .drop('obj', 'split')
            b.cache()
            b_bys[src] = b
            wc = b.groupBy('word') \
                .count()
            counts[src] = wc

        word_frequency = counts[source].unionByName(counts[target]) \
            .groupBy('word') \
            .agg(F.collect_list('count').alias('counts')) \
            .select('word', sum_int_l_udf('counts').alias('count'))

        no_attr = {}
        for src in [source, target]:
            b_by = b_bys[src].join(word_frequency, 'word') \
                .withColumn('rank', F.dense_rank().over(rank_window)) \
                .filter(F.col('rank') <= F.col('threshold')) \
                .drop('threshold', 'count') \
                .groupBy('sub', 'row') \
                .agg(F.collect_set(F.struct('word', 'rank')).alias('words')) \
                .withColumn('comb', combine_words_udf('words')) \
                .select('sub', F.explode('comb').alias('new_key'))
            profile = full[src].join(b_by, 'sub', 'left')
            profile.cache()
            n = profile.filter(F.col('new_key').isNull()) \
                .select('sub', 'key')
            n.cache()
            no_attr[src] = n
            f = profile.filter(F.col('new_key').isNotNull()) \
                .select('sub', 'key', F.concat_ws('$', 'key', 'new_key').alias('new_key'))
            f.cache()
            full[src] = f

        unique = find_unique(full[source], full[target], 'sub', 'new_key', source, target) \
            .select('sub', 'db', F.col('new_key').alias('key'))
        unique.write.saveAsTable(block_config['unique_to'], mode='append')

        # deal with ns
        t_k = full[target].select('key', 'new_key') \
            .dropDuplicates()
        n_s = no_attr[source].withColumn('new_key', F.lit('NULL')) \
            .select('sub', 'key', F.concat_ws('$', 'key', 'new_key').alias('new_key'))
        n_s_t = no_attr[source].join(t_k, 'key')
        # deal with nt
        s_k = full[source].select('key', 'new_key') \
            .dropDuplicates()
        n_t = no_attr[target].withColumn('new_key', F.lit('NULL')) \
            .select('sub', 'key', F.concat_ws('$', 'key', 'new_key').alias('new_key'))
        n_t_s = no_attr[target].join(s_k, 'key')

        full[source] = full[source].unionByName(n_s) \
            .unionByName(n_s_t) \
            .select('sub', F.col('new_key').alias('key'))
        full[target] = full[target].unionByName(n_t) \
            .unionByName(n_t_s) \
            .select('sub', F.col('new_key').alias('key'))

    full_s = full[source].withColumn('db', F.lit(source))
    full_t = full[target].withColumn('db', F.lit(target))
    result = full_s.unionByName(full_t)
    result.write.saveAsTable(block_config['candidate_to'], mode='overwrite')

    # db_count_window = Window.partitionBy(F.col('key')).rangeBetween(Window.unboundedPreceding,
    #                                                                 Window.unboundedFollowing)
    # result = full.withColumn('db_count', F.size(F.collect_set(F.col('db')).over(db_count_window))) \
    #     .filter(F.col('db_count') > 1) \
    #     .drop('db_count')

    unique_block = spark.read.table(block_config['unique_to'])
    unique_pair = generate_block(spark, unique_block, source, target, 'key', 'unique', block_config['balance'])
    candidate_block = spark.read.table(block_config['candidate_to'])
    candidate_pair = generate_block(spark, candidate_block, source, target, 'key', 'candidate', block_config['balance'])
    unique_pair.write.saveAsTable(block_config['block_to']['unique'], mode='overwrite')
    candidate_pair.write.saveAsTable(block_config['block_to']['candidate'], mode='overwrite')
    spark.catalog.clearCache()


def generate_block(spark, d, source, target, k, block_type, balance_conf):
    d1 = d.filter(d['db'] == source) \
        .withColumnRenamed('sub', source)
    d2 = d.filter(d['db'] == target) \
        .withColumnRenamed('sub', target)
    if block_type == 'unique':
        d3 = d1.join(d2, k) \
            .select(source, target) \
            .dropDuplicates()
    else:
        d3 = balance_join(spark, d1, d2, k, source, balance_conf) \
            .select(source, target) \
            .dropDuplicates()
    return d3
