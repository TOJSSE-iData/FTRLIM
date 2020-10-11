# -*- coding: utf-8 -*-
import pyspark.sql.functions as F


def merge(spark, merge_config):
    source = merge_config['source_tag']
    target = merge_config['target_tag']
    match = spark.read.table(merge_config['merge_from'])
    match.cache()
    for attribute in merge_config['attribute']:
        rule_udf = attribute['rule']
        sa = spark.read.table(attribute[source]) \
            .select(F.col('sub').alias(source), F.col('obj').alias('sa'))
        ta = spark.read.table(attribute[target]) \
            .select(F.col('sub').alias(target), F.col('obj').alias('ta'))
        res = match.join(sa, source, 'left') \
            .join(ta, target, 'left') \
            .select(source, target, rule_udf('sa', 'ta').alias(attribute['field']))
        res.write.saveAsTable(attribute['merge_to'], mode='overwrite')
    for relation in merge_config['relation']:
        rule_udf = relation['rule']
        sr = spark.read.table(relation[source]['table']) \
            .groupBy(relation[source]['from_field']) \
            .agg(F.collect_set(relation[source]['to_field']).alias('sr')) \
            .withColumnRenamed(relation[source]['from_field'], source)
        tr = spark.read.table(relation[target]['table']) \
            .groupBy(relation[target]['from_field']) \
            .agg(F.collect_set(relation[target]['to_field']).alias('tr')) \
            .withColumnRenamed(relation[target]['from_field'], target)
        res = match.join(sr, source, 'left') \
            .join(tr, target, 'left') \
            .select(source, target, rule_udf('sr', 'tr').alias('rule')) \
            .select(source, target, F.explode('rule').alias(relation['field']))
        res.write.saveAsTable(relation['merge_to'], mode='overwrite')
