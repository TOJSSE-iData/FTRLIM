# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from matching.block import partition, block
from matching.compare import compare
from matching.ftrl import FTRL
from matching.check import train, update, predict, match
from conf.config_trsp_company import *
from mylog.log4j import Log4j

spark = SparkSession.builder \
    .appName('Knowledge Graph Instance Matching') \
    .config(conf=SparkConf()) \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

logger = Log4j(spark)

print("Start partition ...")
# partition(spark, partition_configs)
print("Block partition.")

print("Start blocking ...")
# block(spark, block_configs)
print("Block completed.")
 
print("Start comparing ...")
# compare(spark, compare_configs)
print("Comparison completed.")

ref = spark.read.table('dev.trsp_align')

print('Building model...')
if ftrl_configs['pre_train']:
    ftrl = FTRL.load_model(ftrl_configs['model_path'])
else:
    ftrl = FTRL(
        dim=ftrl_configs['dim'],
        l1=ftrl_configs['l1'],
        l2=ftrl_configs['l2'],
        alpha=ftrl_configs['alpha'],
        beta=ftrl_configs['beta'],
        max_iter=ftrl_configs['max_iter'],
        eta=ftrl_configs['eta'],
        epochs=ftrl_configs['epochs']
    )
    predict(spark, ftrl, predict_configs)
    ftrl = train(spark, ftrl, train_configs, ref)

check_more = -1
scanner = sc._gateway.jvm.java.util.Scanner
sys_in = getattr(sc._gateway.jvm.java.lang.System, 'in')
while True:
    while check_more != 0 and check_more != 1:
        try:
            print("Want to update model? (1 for yes 0 for no):")
            check_more = int(scanner(sys_in).nextLine())
        except ValueError:
            pass
    if check_more:
        ftrl = update(spark, ftrl, update_configs)
    else:
        ftrl.save(ftrl_configs['model_path'])
        break

print('Start predicting...')
predict(spark, ftrl, predict_configs)
print('Prediction completed.')

print('Start matching...')
match(spark, match_configs)
print('Matching completed.')

print('Start evaluation...')
print('\n')

ref_count = ref.count()
unique = spark.read.table(block_configs['block_to']['unique']).select('source', 'target')
print('unique: {}'.format(unique.count()))
candidate = spark.read.table(block_configs['block_to']['candidate']).select('source', 'target')
print('candidate: {}'.format(candidate.count()))
hit = candidate.unionByName(unique).dropDuplicates().join(ref, ['source', 'target'])
print('hit: {}'.format(hit.count()))
print('unique hit: {}'.format(unique.join(hit, ['source', 'target']).count()))
print('\n')
no_update = spark.read.table(match_configs['match_to']).dropDuplicates()
no_update_count = no_update.count()
no_update_hit = no_update.join(ref, ['source', 'target'])
no_update_hit_count = no_update_hit.count()
precision = no_update_hit_count / no_update_count
recall = no_update_hit_count / ref_count
f1 = 2 * precision * recall / (precision + recall)
print('matched: {}'.format(no_update_count))
print('hit: {}'.format(no_update_hit_count))
print('ref: {}'.format(ref_count))
print('no update precision: {0}%'.format(precision * 100))
print('no update recall: {0}%'.format(recall * 100))
print('no update f1: {0}%'.format(f1 * 100))
print('\n')
checked = spark.read.table(match_configs['check_from']) \
    .select('source', 'target', 'is_match')
filter_ref = ref.join(checked, ['source', 'target'], 'left_anti')
f_ref_count = filter_ref.count()
filter_nu = no_update.join(checked, ['source', 'target'], 'left_anti')
f_nu_count = filter_nu.count()
filter_hit = filter_nu.join(filter_ref, ['source', 'target'])
f_hit_count = filter_hit.count()
f_pre = f_hit_count / f_nu_count
f_rec = f_hit_count / f_ref_count
f_f1 = 2 * f_pre * f_rec / (f_pre + f_rec)
print('filter matched: {}'.format(f_nu_count))
print('filter hit: {}'.format(f_hit_count))
print('filter ref: {}'.format(f_ref_count))
print('filter no update precision: {0}%'.format(f_pre * 100))
print('filter no update recall: {0}%'.format(f_rec * 100))
print('filter no update f1: {0}%'.format(f_f1 * 100))
print('\n')
print('Evaluation completed')

# print('Start merging...')
# merge(spark, merge_configs)
# print('Merging completed.')
