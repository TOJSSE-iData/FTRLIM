import random
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import Row, Window

from .ftrl import FTRL, Corpus


def train(spark, ftrl: FTRL, train_config, ref):
    source = train_config['source_tag']
    target = train_config['target_tag']

    rank_window = Window.partitionBy(source).orderBy(F.desc('score'))

    aliases = [d['alias'] for d in train_config['display']]

    pos_size = int(train_config['size'] / 2) + 1
    thresh = train_config['threshold']
    pos = spark.read.table(train_config['score_from']['candidate']) \
        .filter(F.col('score') >= thresh) \
        .withColumn('rank', F.rank().over(rank_window)) \
        .filter(F.col('rank') == 1) \
        .drop('rank') \
        .dropDuplicates()
    pos.cache()
    pos_ratio = min(pos_size / pos.count(), 1.0)
    pos = pos.sample(pos_ratio) \
        .limit(pos_size)

    neg_size = train_config['size'] - pos_size
    neg = spark.read.table(train_config['score_from']['candidate']) \
        .filter(F.col('score') < thresh) \
        .dropDuplicates()
    neg.cache()
    neg_ratio = min(neg_size / neg.count(), 1.0)
    neg = neg.sample(neg_ratio) \
        .limit(neg_size)

    batch = pos.unionByName(neg)
    # for i, a_conf in enumerate(train_config['display']):
    #     alias = aliases[i]
    #     s = spark.read.table(a_conf[source]) \
    #         .select(F.col('sub').alias(source), F.col('obj').alias('s_' + alias))
    #     t = spark.read.table(a_conf[target]) \
    #         .select(F.col('sub').alias(target), F.col('obj').alias('t_' + alias))
    #     batch = batch.join(s, source, 'left') \
    #         .join(t, target, 'left') \
    #         .fillna('')
    # to_check = batch.collect()
    #
    # if len(to_check) > train_config['size']:
    #     to_check = random.sample(to_check, train_config['size'])
    #
    # checked = check_batch(spark, to_check, aliases, source, target)
    # train_set = spark.createDataFrame(checked)
    # train_set.write.saveAsTable(train_config['train_to'], mode='overwrite')

    tmp_ref = ref.withColumn('is_match', F.lit(1.0))
    train_set = batch.select(source, target, 'sim_v') \
        .join(tmp_ref, [source, target], 'left') \
        .fillna(0.0)
    train_set.write.saveAsTable(train_config['train_to'], mode='overwrite')
    checked = train_set.collect()

    train_vectors = []
    for r in checked:
        sim = r['sim_v']
        sim.append(r['is_match'])
        train_vectors.append(sim)

    corpus = Corpus(ftrl.dim, train_vectors)

    ftrl.train(corpus)

    return ftrl


def update(spark, ftrl_model, update_config):
    source = update_config['source_tag']
    target = update_config['target_tag']

    aliases = [d['alias'] for d in update_config['display']]

    batch = spark.read.table(update_config['score_from']) \
        .filter((F.col('score') >= update_config['min_score']) & (F.col('score') <= update_config['max_score'])) \
        .limit(update_config['batch_size'])

    for i, a_conf in enumerate(update_config['display']):
        alias = aliases[i]
        s = spark.read.table(a_conf[source]) \
            .select(F.col('sub').alias(source), F.col('obj').alias('s_' + alias))
        t = spark.read.table(a_conf[target]) \
            .select(F.col('sub').alias(target), F.col('obj').alias('t_' + alias))
        batch = batch.join(s, source, 'left') \
            .join(t, target, 'left') \
            .fillna('')
    to_check = batch.collect()

    checked = check_batch(spark, to_check, aliases, source, target)

    schema = spark.read.table(update_config['update_to']).schema
    res = spark.createDataFrame(checked, schema) \
        .dropDuplicates()
    tmp_table = update_config['update_to'] + '_update_tmp'
    updated = spark.read.table(update_config['update_to']) \
        .join(res, [source, target], 'left_anti') \
        .unionByName(res)
    updated.write.saveAsTable(tmp_table, mode='overwrite')
    spark.read.table(tmp_table) \
        .write.saveAsTable(update_config['update_to'], mode='overwrite')
    spark.sql('drop table if exists {}'.format(tmp_table))

    update_vectors = []
    for r in checked:
        sim = r['sim_v']
        sim.append(r['is_match'])
        update_vectors.append(sim)

    corpus = Corpus(ftrl_model.dim, update_vectors)
    for x, y in corpus:
        if y > 0.5:  # y == 1
            ftrl_model.update(x, y)
        else:  # y == 0
            if random.random() <= update_config['update_rate']:
                ftrl_model.update(x, y)

    return ftrl_model


def correct(spark, ftrl_model, sid, tid, is_match, update_config):
    if is_match:
        is_match = 1.0
    else:
        is_match = 0.0
    source = update_config['source_tag']
    target = update_config['target_tag']
    schema = spark.read.table(update_config['score_from']) \
        .schema
    score = spark.read.table(update_config['score_from']) \
        .filter(F.col(source) == sid) \
        .filter(F.col(target) == tid)
    score.cache()
    if score.count() == 0:
        d = {
            source: sid,
            target: tid,
            'sim_v': [],
            'is_match': is_match
        }
        new_sample = [Row(**d)]
        res = spark.createDataFrame(new_sample, schema)
    else:
        new_sample = score.collect()[0]
        sim_v = new_sample['sim_v']
        if is_match > 0.5:  # y == 1
            ftrl_model.update(sim_v, is_match)
        else:  # y == 0
            if random.random() <= update_config['update_rate']:
                ftrl_model.update(sim_v, is_match)
        res = score.drop('is_match', 'score') \
            .withColumn('is_match', F.lit(is_match))

    corrected = spark.read.table(update_config['update_to']) \
        .filter((F.col(source) != sid) & (F.col(target) != tid)) \
        .unionByName(res) \
        .dropDuplicates()
    tmp_table = update_config['update_to'] + '_correct_tmp'
    corrected.write.saveAsTable(tmp_table, mode='overwrite')
    spark.read.table(tmp_table) \
        .write.saveAsTable(update_config['update_to'], mode='overwrite')
    spark.sql('drop table if exists {}'.format(tmp_table))
    return ftrl_model


def check_batch(spark, batch, aliases, source, target):
    sc = spark.sparkContext
    scanner = sc._gateway.jvm.java.util.Scanner
    sys_in = getattr(sc._gateway.jvm.java.lang.System, 'in')

    checked = []
    for i, record in enumerate(batch):
        print("No.{0} of {1}\t {2} vs. {3}".format(i + 1, len(batch), record[source], record[target]))
        for alias in aliases:
            sa = 's_' + alias
            ta = 't_' + alias
            print('{0}: {1}'.format(sa, record[sa]))
            print('{0}: {1}'.format(ta, record[ta]))
        is_match = -1
        while is_match != 0 and is_match != 1:
            try:
                print("'is match?(1 for match 0 for not):")
                is_match = int(scanner(sys_in).nextLine())
            except ValueError:
                pass
            res = {
                source: record[source],
                target: record[target],
                'sim_v': record['sim_v'],
                'is_match': is_match
            }
            checked.append(Row(**res))
    return checked


def predict(spark, ftrl_model, predict_config):
    def decision_func(mdl, sim):
        return float(mdl.predict(sim))

    def real_predict(mdl):
        return F.udf(lambda x: decision_func(mdl, x), T.FloatType())

    for predict_type, predict_from in predict_config['predict_from'].items():
        score = spark.read.table(predict_from) \
            .withColumn('score', real_predict(ftrl_model)(F.col('sim_v')))
        score.write.saveAsTable(predict_config['score_to'][predict_type], mode='overwrite')


def match(spark, match_config):
    def filter_match(table, thresh):
        m = spark.read.table(table) \
            .filter(F.col('score') > thresh) \
            .withColumn('s_rank', F.rank().over(s_rank_window)) \
            .filter(F.col('s_rank') == 1) \
            .select(source, target)
        return m

    source = match_config['source_tag']
    target = match_config['target_tag']

    s_rank_window = Window.partitionBy(source).orderBy(F.desc('score'))
    t_rank_window = Window.partitionBy(target).orderBy(F.desc('score'))
    is_121 = F.udf(lambda x, y: True if x * y == 1 else False, T.BooleanType())

    um = filter_match(match_config['match_from']['unique'], match_config['threshold']['unique'])
    cm = filter_match(match_config['match_from']['candidate'], match_config['threshold']['candidate'])

    check = spark.read.table(match_config['check_from']) \
        .select(source, target, 'is_match') \
        .dropDuplicates()
    correct = check.filter(F.col('is_match') == 1) \
        .select(source, target)
    incorrect = check.filter(F.col('is_match') == 0) \
        .select(source, target)

    res = um.unionByName(cm) \
        .dropDuplicates() \
        .join(incorrect, [source, target], 'left_anti') \
        .unionByName(correct) \
        .dropDuplicates()

    res.write.saveAsTable(match_config['match_to'], mode='overwrite')
