import os

TEST_NAME = 'test-pipeline'
TEST_TITLE = 'Test Pipeline'
TEST_DESCRIPTION = 'Testing code to generate pipeline yaml files'
TEST_PATH = os.environ['TEST_PATH']
TEST_DATAPACKAGE_URL = TEST_PATH + 'csv/test_data.csv'
TEST_CONCAT = {
    'datapackage': {
        'url': TEST_PATH + 'csv/test_concat.csv',
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
TEST_SORT_FIELD = 'Taxon'

TEST_COMBINE_FIELDS = {
    'output_field': 'Taxon-Species',
    'fields': ['Taxon', 'Species'],
}

TEST_ROUND_FIELD = {
    'field': 'TestRound',
    'digits': 2,
}

TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD = {
    'input_field': 'Lat',
    'output_field': 'Lat-converted',
    'format': 'degrees-decimal_minutes',
    'pattern': '%directional% %degrees%o %decimal_minutes%',
}

TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS = {
    'input_field': 'Long',
    'output_field': 'Long-converted',
    'format': 'degrees-minutes-seconds',
    'pattern': '%directional% %degrees%o %minutes% %seconds%',
}

TEST_CONVERT_DATE_DATE = {
    'input_field': 'TestDate',
    'input_format': '%Y-%m-%d',
    'input_timezone': 'US/Eastern',
    'output_field': 'TestDateConverted',
    'output_format': '%Y-%m-%dT%H:%M:%SZ',
    'output_timezone': 'UTC',
}
TEST_CONVERT_DATE_MONTH_DAY = {
    'input_field': 'TestDateYear',
    'input_format': '%m-%d',
    'input_timezone': 'US/Eastern',
    'output_field': 'TestDateYearConverted',
    'year': 2015,
}

TEST_SAVE_PATH = TEST_PATH + 'data/'
