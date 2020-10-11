# -*- coding: utf-8 -*-

_SOURCE = 'source'
_TARGET = 'target'

block_configs = {
    'full_source': "source.ba_company_name",
    'full_target': "target.ba_company_name",
    'candidate_to': 'dev.ba_blockc_tmp',
    'unique_to': 'dev.ba_blocku_tmp',
    'block_to': {
        'unique': 'dev.ba_pairu_tmp',
        'candidate': 'dev.ba_pairc_tmp'
    },
    'source_tag': _SOURCE,
    'target_tag': _TARGET,
    'block_by': [
        {
            _SOURCE: {
                'table': "source.ba_company_name",
                'pattern': "[^0-9A-Za-z.]+",
                'max_len': 1,
            },
            _TARGET: {
                'table': "target.ba_company_name",
                'pattern': "[^0-9A-Za-z.]+",
                'max_len': 1,
            }
        },
    ],
    'balance': {
        'n_workers': 10,
        'hash': {
            'range': 100,
            'params': [
                (3, 75),
                (11, 83),
                (67, 55),
                (43, 28),
            ]
        }
    }
}

compare_configs = {
    'source_tag': _SOURCE,
    'target_tag': _TARGET,
    'compare_from': block_configs['block_to'],
    'compare_by': {
        'attribute': [
        ],
        'relation': [
            {
                _SOURCE: {
                    'table': "source.ba_company_name",
                    'from_field': 'sub',
                    'to_field': 'obj',
                },
                _TARGET: {
                    'table': "target.ba_company_name",
                    'from_field': 'sub',
                    'to_field': 'obj',
                },
                'default': 0.126
            },
            {
                _SOURCE: {
                    'table': "source.ba_company_address",
                    'from_field': 'sub',
                    'to_field': 'obj',
                },
                _TARGET: {
                    'table': "target.ba_company_address",
                    'from_field': 'sub',
                    'to_field': 'obj',
                },
                'default': 0.124,
            },
            {
                _SOURCE: {
                    'table': 'source.ba_company_url',
                    'from_field': 'sub',
                    'to_field': 'obj',
                },
                _TARGET: {
                    'table': 'target.ba_company_url',
                    'from_field': 'sub',
                    'to_field': 'obj',
                },
                'default': 0.077,
            },
            {
                _SOURCE: {
                    'table': 'source.ba_executive',
                    'from_field': 'sub',
                    'to_field': 'obj',
                },
                _TARGET: {
                    'table': 'target.ba_executive',
                    'from_field': 'sub',
                    'to_field': 'obj',
                },
                'default': 0.312,
            },

        ],
    },
    'compare_to': {
        'unique': 'dev.ba_simu_tmp',
        'candidate': 'dev.ba_simc_tmp'
    }
}

predict_configs = {
    'predict_from': compare_configs['compare_to'],
    'score_to': {
        'unique': 'dev.ba_scoreu_tmp',
        'candidate': 'dev.ba_scorec_tmp'
    },
    'threshold': {
        'unique': 0.4,
        'candidate': 0.5,
    },
}
train_configs = {
    'source_tag': _SOURCE,
    'target_tag': _TARGET,
    'size': 200,
    'display': [
    ],
    'score_from': predict_configs['score_to'],
    'threshold': {
        'train': 0.5,
        'unique': 0.4,
        'candidate': 0.5,
    },
    'train_to': 'dev.ba_check_tmp',
}

update_configs = {
    'source_tag': _SOURCE,
    'target_tag': _TARGET,
    'display': [
    ],
    'score_from': predict_configs['score_to']['candidate'],
    'update_to': train_configs['train_to'],
    'min_score': 0.4,
    'max_score': 0.6,
    'batch_size': 50,
    'update_rate': 0.02,
}

ftrl_configs = {
    'pre_train': True,
    'model_path': 'saved/ba.json',
    'dim': len(compare_configs['compare_by']['attribute']) + len(
        compare_configs['compare_by']['relation']),
    'l1': 0,
    'l2': 0.5,
    'alpha': 0.02,
    'beta': 1,
    'max_iter': 3000,
    'eta': 0.01,
    'epochs': 100,
}

match_configs = {
    'source_tag': _SOURCE,
    'target_tag': _TARGET,
    'match_from': predict_configs['score_to'],
    'match_to': 'res.ba_match',
    'threshold': predict_configs['threshold'],
    'check_from': train_configs['train_to'],
}
