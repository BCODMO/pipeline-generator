import os

TEST_NAME = 'test-pipeline'
TEST_TITLE = 'Test Pipeline'
TEST_DESCRIPTION = 'Testing code to generate pipeline yaml files'
TEST_PATH = os.environ['TEST_PATH']
TEST_DATA_URL = TEST_PATH + 'test_data/test_data.csv'
TEST_CONCAT = {
    'datapackage': {
        'url': TEST_PATH + 'test_data/test_concat.csv',
        'name': 'concat',
    },
    'fields': {
        'Genomic_Method': [],
        'Taxon': [],
        'Species': [],
        'Lander': [],
        'Lab_Intenifier': [],
        'Block_Bone': [],
        'Individuals': [],
        'Lat': [],
        'Long': [],
        'Depth': [],
        'TestRound': [],
        'TestDate': [],
        'TestDatetime': [],
        'TestDateYear': [],
    },
}
TEST_DELETE_FIELDS = ['Lander', 'Block_Bone']
TEST_SORT_BY = '{Taxon}'

TEST_COMBINE_FIELDS = {
    'output_field': 'Taxon-Species',
    'with': '{Taxon} {Species}',
}

TEST_ROUND_FIELD = {
    'field': 'TestRound',
    'digits': 2,
}

TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD = {
    'input_field': 'Lat',
    'output_field': 'Lat-converted',
    'format': 'degrees-decimal_minutes',
    'pattern': '(?P<directional>.*) (?P<degrees>.*)o (?P<decimal_minutes>.*)',
}

TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS = {
    'input_field': 'Long',
    'output_field': 'Long-converted',
    'format': 'degrees-minutes-seconds',
    'pattern': '(?P<directional>.*) (?P<degrees>.*)o (?P<minutes>.*) (?P<seconds>.*)',
}

TEST_CONVERT_DATE_DATE = {
    'input_field': 'TestDate',
    'input_format': '%Y-%m-%d',
    'input_timezone': 'US/Eastern',
    'output_field': 'TestDateConverted',
    'output_format': '%Y-%m-%d %H:%M:%S',
    'output_timezone': 'UTC',
}
TEST_CONVERT_DATE_MONTH_DAY = {
    'input_field': 'TestDateYear',
    'input_format': '%m-%d',
    'input_timezone': 'US/Eastern',
    'output_field': 'TestDateYearConverted',
    'output_format': '%Y-%m-%d %H:%M:%S',
    'output_timezone': 'UTC',
    'year': 2015,
}

TEST_BOOLEAN_ADD_COMPUTED_FIELD = {
    'number': [
        {
            'value': 'SHOULD NEVER BE THIS',
            'boolean': '{TestRound} > 50'
        },
        {
            'value': 'SHOULD BE THIS',
            'boolean': '{TestRound} < 50 && {TestRound} < 9999999'
        },
    ],
}

TEST_SPLIT_COLUMNS = {
    'input_field': 'Species',
    'output_fields': ['split1', 'split2'],
    'pattern': '(.*) (.*)',
}

TEST_ADD_SCHEMA_METADATA = {
    'resources': ['default'],
    'missingKeys': ['nd', ''],
    'missingValues': ['nd', ''],
}

TEST_SAVE_PATH = TEST_PATH + 'data/'

TEST_STEPS = [
#    {
#        "run": "add_resource",
#        "parameters": {
#            "name": "default",
#            "url": TEST_DATA_URL,
#        },
#    },
#    {
#        "run": "stream_remote_resources",
#        "cache": True,
#    },
    {
        "run": "load",
        "parameters": {
            "name": "default",
            "from": TEST_DATA_URL,
            "validate": False,
        },
        "cache": True,
    },
    {
        "run": "load",
        "parameters": {
            "name": TEST_CONCAT['datapackage']['name'],
            "from": TEST_CONCAT['datapackage']['url'],
            "validate": False,
        },
        "cache": True,
    },
    {
        "run": "concatenate",
        "parameters": {
            "fields": TEST_CONCAT['fields'],
            "target": {
                "name": "default",
                "path": "data/default"
            },
        },
    },
    {
        "run": "delete_fields",
        "parameters": {
            "fields": TEST_DELETE_FIELDS,
        },
    },
    {
        "run": "sort",
        "parameters": {
            "resources": None,
            "sort-by": TEST_SORT_BY,
        },
    },
    {
        "run": "add_computed_field",
        "parameters": {
            "fields": [
                {
                    "operation": "format",
                    "target": TEST_COMBINE_FIELDS['output_field'],
                    "with": TEST_COMBINE_FIELDS['with'],
                },
            ],
        },
    },
    {
        "run": "bcodmo_pipeline_processors.round_fields",
        "parameters": {
            "fields": [
                {
                    "digits": TEST_ROUND_FIELD['digits'],
                    "name": TEST_ROUND_FIELD['field'],
                },
            ],
        },
    },
    {
        "run": "bcodmo_pipeline_processors.convert_to_decimal_degrees",
        "parameters": {
            "fields": [
                {
                    "format": TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['format'],
                    "input_field": TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['input_field'],
                    "output_field": TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['output_field'],
                    "pattern": TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['pattern'],
                },
            ],
        },
    },
    {
        "run": "bcodmo_pipeline_processors.convert_to_decimal_degrees",
        "parameters": {
            "fields": [
                {
                    "format": TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['format'],
                    "input_field": TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['input_field'],
                    "output_field": TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['output_field'],
                    "pattern": TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['pattern'],
                },
            ],
        },
    },
    {
        "run": "bcodmo_pipeline_processors.convert_date",
        "parameters": {
            "fields": [
                {
                    "input_field": TEST_CONVERT_DATE_DATE['input_field'],
                    "input_format": TEST_CONVERT_DATE_DATE['input_format'],
                    "input_timezone": TEST_CONVERT_DATE_DATE['input_timezone'],
                    "output_field": TEST_CONVERT_DATE_DATE['output_field'],
                    "output_format": TEST_CONVERT_DATE_DATE['output_format'],
                    "output_timezone": TEST_CONVERT_DATE_DATE['output_timezone'],
                }
            ]
        },
    },
    {
        "run": "bcodmo_pipeline_processors.convert_date",
        "parameters": {
            "fields": [
                {
                    "input_field": TEST_CONVERT_DATE_MONTH_DAY['input_field'],
                    "input_format": TEST_CONVERT_DATE_MONTH_DAY['input_format'],
                    "input_timezone": TEST_CONVERT_DATE_MONTH_DAY['input_timezone'],
                    "output_field": TEST_CONVERT_DATE_MONTH_DAY['output_field'],
                    "output_format": TEST_CONVERT_DATE_MONTH_DAY['output_format'],
                    "output_timezone": TEST_CONVERT_DATE_MONTH_DAY['output_timezone'],
                    "year": TEST_CONVERT_DATE_MONTH_DAY['year'],
                }
            ]
        },
    },
    {
        "run": "bcodmo_pipeline_processors.boolean_add_computed_field",
        "parameters": {
            "fields": [
                {
                    'target': 'bool_computed_field',
                    'functions': [
                        {
                            'value': TEST_BOOLEAN_ADD_COMPUTED_FIELD['number'][0]['value'],
                            'boolean': TEST_BOOLEAN_ADD_COMPUTED_FIELD['number'][0]['boolean'],
                        },
                        {
                            'value': TEST_BOOLEAN_ADD_COMPUTED_FIELD['number'][1]['value'],
                            'boolean': TEST_BOOLEAN_ADD_COMPUTED_FIELD['number'][1]['boolean'],
                        },
                    ],
                },
            ],
        },
    },
    #{
    #    "run": "bcodmo_pipeline_processors.infer_types",
    #},
    {
        "run": "bcodmo_pipeline_processors.add_schema_metadata",
        "parameters": TEST_ADD_SCHEMA_METADATA,
    },
    {
        "run": "bcodmo_pipeline_processors.split_column",
        "parameters": {
            "fields": [{
                'input_field': TEST_SPLIT_COLUMNS['input_field'],
                'output_fields': TEST_SPLIT_COLUMNS['output_fields'],
                'pattern': TEST_SPLIT_COLUMNS['pattern'],
            }]
        },
    },
    {
        "run": "dump_to_path",
        "parameters": {
            "out-path": TEST_SAVE_PATH,
        },
    }
]

FIXED_WIDTH_TEST_DATA_URL = TEST_PATH + 'test_data/test_data.gof'
FIXED_WIDTH_TEST_STEPS = [
    {
        'run': 'add_resource',
        'parameters': {
            'name': 'default',
            'url': FIXED_WIDTH_TEST_DATA_URL,
            'format': 'bcodmo.fixedwidth',
            'width': [8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8],
            'skip_rows': [4, 5],
            'headers': [2, 3],
        },
    },
    {
        'run': 'stream_remote_resources',
        'cache': False,
    },
]
