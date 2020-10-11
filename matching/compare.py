# -*- coding: utf-8 -*-
import re
import pyspark.sql.functions as F
import pyspark.sql.types as T
import difflib


def calc_sim(x, y, z):
    if x is not None and y is not None:
        return [difflib.SequenceMatcher(None, x, y).ratio()]
    else:
        return [float(z)]


def clean_attr(raw, junk):
    j_ptn = re.compile(junk)
    return re.sub(j_ptn, '', raw)


def calc_jaccard(x, y, z):
    if x is None or y is None:
        return [float(z)]
    nx = len(x)
    ny = len(y)
    if nx * ny == 0:
        return [float(z)]
    d = 0
    compared = set()
    for xx in x:
        for yy in y:
            if yy in compared:
                continue
            sim = calc_sim(xx, yy, 0.0)
            if sim[0] >= 0.8:
                d += 1
                compared.add(yy)
                break
    j = d / (nx + ny - d)
    return [j]


def compare(spark, compare_config):
    def concat_arr(x, y):
        x.extend(y)
        return x

    clean_attr_udf = F.udf(lambda x, y: clean_attr(x, y), T.StringType())
    calc_sim_udf = F.udf(lambda x, y, z: calc_sim(x, y, z), T.ArrayType(T.FloatType()))
    calc_jaccard_udf = F.udf(lambda x, y, z: calc_jaccard(x, y, z), T.ArrayType(T.FloatType()))
    cancat_arr_udf = F.udf(lambda x, y: concat_arr(x, y), T.ArrayType(T.FloatType()))

    source = compare_config['source_tag']
    target = compare_config['target_tag']

    for compare_type, compare_from in compare_config['compare_from'].items():
        pairs = spark.read.table(compare_from)

        for a_conf in compare_config['compare_by']['attribute']:
            ignore = a_conf['ignore']
            s = spark.read.table(a_conf[source]) \
                .select('sub', F.lower(F.col('obj')).alias('lower')) \
                .select(F.col('sub').alias(source), clean_attr_udf(F.col('lower'), F.lit(ignore)).alias('s_obj'))
            t = spark.read.table(a_conf[target]) \
                .select('sub', F.lower(F.col('obj')).alias('lower')) \
                .select(F.col('sub').alias(target), clean_attr_udf(F.col('lower'), F.lit(ignore)).alias('t_obj'))
            pairs = pairs.join(s, source, 'left') \
                .join(t, target, 'left') \
                .withColumn('default', F.lit(a_conf['default'])) \
                .withColumn('tmp_sim', calc_sim_udf(F.col('s_obj'), F.col('t_obj'), F.col('default')))
            if 'sim_v' in pairs.columns:
                pairs = pairs.select(source, target, cancat_arr_udf('sim_v', 'tmp_sim').alias('sim_v'))
            else:
                pairs = pairs.select(source, target, F.col('tmp_sim').alias('sim_v'))

        for r_conf in compare_config['compare_by']['relation']:
            s_rel = spark.read.table(r_conf[source]['table']) \
                .withColumnRenamed(r_conf[source]['from_field'], source) \
                .select(source, F.lower(F.col(r_conf[source]['to_field'])).alias(r_conf[source]['to_field']))
            t_rel = spark.read.table(r_conf[target]['table']) \
                .withColumnRenamed(r_conf[target]['from_field'], target) \
                .select(target, F.lower(F.col(r_conf[target]['to_field'])).alias(r_conf[target]['to_field']))
            s_rel_attr = s_rel.groupBy(source).agg(F.collect_set(F.col(r_conf[source]['to_field'])).alias('s_attr'))
            t_rel_attr = t_rel.groupBy(target).agg(F.collect_set(F.col(r_conf[target]['to_field'])).alias('t_attr'))
            pairs = pairs.join(s_rel_attr, source, 'left') \
                .join(t_rel_attr, target, 'left') \
                .withColumn('default', F.lit(r_conf['default'])) \
                .withColumn('jaccard', calc_jaccard_udf(F.col('s_attr'), F.col('t_attr'), F.col('default')))
            if 'sim_v' in pairs.columns:
                pairs = pairs.select(source, target, cancat_arr_udf('sim_v', 'jaccard').alias('sim_v'))
            else:
                pairs = pairs.select(source, target, F.col('jaccard').alias('sim_v'))

        pairs.write.saveAsTable(compare_config['compare_to'][compare_type], mode='overwrite')
        spark.catalog.clearCache()
