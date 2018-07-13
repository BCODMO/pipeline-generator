import os

TEST_NAME = 'test-pipeline'
TEST_TITLE = 'Test Pipeline'
TEST_DESCRIPTION = 'Testing code to generate pipeline yaml files'
TEST_DATAPACKAGE_URL = os.environ['TEST_PATH'] + 'csv/test_data.csv'
TEST_CONCAT = {
    'datapackage': {
        'url': os.environ['TEST_PATH'] + 'csv/test_concat.csv',
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

TEST_SAVE_PATH = os.environ['TEST_PATH'] + 'data/'

EXPECTED_INTRO = f'''{TEST_NAME}:
  title: {TEST_TITLE}
  description: {TEST_DESCRIPTION}
  pipeline:'''

EXPECTED_STEPS = [
f'''
    -
      run: add_resource
      parameters:
        url: {TEST_DATAPACKAGE_URL}
        name: default''',
f'''
    -
      run: stream_remote_resources
      cache: True''',
f'''
    -
      run: add_resource
      parameters:
        url: {TEST_CONCAT['datapackage']['url']}
        name: {TEST_CONCAT['datapackage']['name']}''',
f'''
    -
      run: stream_remote_resources
      cache: True''',
f'''
    -
      run: concatenate
      parameters:
        target:
          name: default
          path: data/default
        fields:
          {list(TEST_CONCAT['fields'].keys())[0]}: []
          {list(TEST_CONCAT['fields'].keys())[1]}: []
          {list(TEST_CONCAT['fields'].keys())[2]}: []
          {list(TEST_CONCAT['fields'].keys())[3]}: []
          {list(TEST_CONCAT['fields'].keys())[4]}: []
          {list(TEST_CONCAT['fields'].keys())[5]}: []
          {list(TEST_CONCAT['fields'].keys())[6]}: []
          {list(TEST_CONCAT['fields'].keys())[7]}: []
          {list(TEST_CONCAT['fields'].keys())[8]}: []
          {list(TEST_CONCAT['fields'].keys())[9]}: []
          {list(TEST_CONCAT['fields'].keys())[10]}: []
          {list(TEST_CONCAT['fields'].keys())[11]}: []
          {list(TEST_CONCAT['fields'].keys())[12]}: []
          {list(TEST_CONCAT['fields'].keys())[13]}: []''',
f'''
    -
      run: delete_fields
      parameters:
        fields:
          - {TEST_DELETE_FIELDS[0]}
          - {TEST_DELETE_FIELDS[1]}''',
f'''
    -
      run: sort
      parameters:
        resources:
          - default
        sort-by: "{{{TEST_SORT_FIELD}}}"''',
f'''
    -
      run: add_computed_field
      parameters:
        fields:
          -
            operation: format
            target: {TEST_COMBINE_FIELDS['output_field']}
            with: "{{{TEST_COMBINE_FIELDS['fields'][0]}}} {{{TEST_COMBINE_FIELDS['fields'][1]}}}"''',
f'''
    -
      run: bcodmo-pipeline-processors.round_fields
      parameters:
        fields:
          -
            name: {TEST_ROUND_FIELD['field']}
            digits: {TEST_ROUND_FIELD['digits']}''',
f'''
    -
      run: bcodmo-pipeline-processors.convert_to_decimal_degrees
      parameters:
        fields:
          -
            input_field: {TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['input_field']}
            output_field: {TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['output_field']}
            format: "{TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['format']}"
            pattern: "{TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['pattern']}"''',
f'''
    -
      run: bcodmo-pipeline-processors.convert_to_decimal_degrees
      parameters:
        fields:
          -
            input_field: {TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['input_field']}
            output_field: {TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['output_field']}
            format: "{TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['format']}"
            pattern: "{TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['pattern']}"''',
f'''
    -
      run: bcodmo-pipeline-processors.convert_date
      parameters:
        fields:
          -
            input_field: "{TEST_CONVERT_DATE_DATE['input_field']}"
            input_format: "{TEST_CONVERT_DATE_DATE['input_format']}"
            output_field: "{TEST_CONVERT_DATE_DATE['output_field']}"
            output_format: "{TEST_CONVERT_DATE_DATE['output_format']}"
            output_timezone: "{TEST_CONVERT_DATE_DATE['output_timezone']}"
            input_timezone: "{TEST_CONVERT_DATE_DATE['input_timezone']}"''',
f'''
    -
      run: bcodmo-pipeline-processors.convert_date
      parameters:
        fields:
          -
            input_field: "{TEST_CONVERT_DATE_MONTH_DAY['input_field']}"
            input_format: "{TEST_CONVERT_DATE_MONTH_DAY['input_format']}"
            output_field: "{TEST_CONVERT_DATE_MONTH_DAY['output_field']}"
            output_format: "%Y-%m-%dT%H:%M:%SZ"
            output_timezone: "UTC"
            input_timezone: "{TEST_CONVERT_DATE_DATE['input_timezone']}"
            year: "{TEST_CONVERT_DATE_MONTH_DAY['year']}"''',
f'''
    -
      run: bcodmo-pipeline-processors.infer_types''',
f'''
    -
      run: dump.to_path
      parameters:
        out-path: {TEST_SAVE_PATH}''',
]
