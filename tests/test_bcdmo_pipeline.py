import csv
import io
import json
import logging
import os
from subprocess import run, PIPE

from bcodmo_pipeline import BcodmoPipeline
from .constants import *

logging.basicConfig(
    level=logging.DEBUG,
)

logger = logging.getLogger(__name__)


class TestBcodmoPipeline():

    def setup_class(self):
        self.shared_data = {}
        self.pipeline = BcodmoPipeline(name=TEST_NAME, title=TEST_TITLE, description=TEST_DESCRIPTION)

        # Add resource
        self.pipeline.add_resource(TEST_DATAPACKAGE_URL)

        # Concat
        self.pipeline.add_resource(
            TEST_CONCAT['datapackage']['url'],
            name=TEST_CONCAT['datapackage']['name'],
        )
        self.pipeline.concatenate(TEST_CONCAT['fields'])

        # Delete fields
        self.pipeline.delete_fields(TEST_DELETE_FIELDS)

        # Sort
        self.pipeline.sort(TEST_SORT_FIELD)

        # Combine
        self.pipeline.combine_fields(
            TEST_COMBINE_FIELDS['output_field'],
            TEST_COMBINE_FIELDS['fields'],
        )

        # Round
        self.pipeline.round_field(
            TEST_ROUND_FIELD['field'],
            TEST_ROUND_FIELD['digits'],
        )

        # Convert to decimal degrees
        self.pipeline.convert_field_decimal_degrees(
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['input_field'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['output_field'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['format'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['pattern'],
        )
        self.pipeline.convert_field_decimal_degrees(
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['input_field'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['output_field'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['format'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['pattern'],
        )

        # Convert date time
        self.pipeline.convert_date(
            TEST_CONVERT_DATE_DATE['input_field'],
            TEST_CONVERT_DATE_DATE['output_field'],
            TEST_CONVERT_DATE_DATE['input_format'],
            input_timezone=TEST_CONVERT_DATE_DATE['input_timezone'],
            output_format=TEST_CONVERT_DATE_DATE['output_format'],
            output_timezone=TEST_CONVERT_DATE_DATE['output_timezone'],
        )
        self.pipeline.convert_date(
            TEST_CONVERT_DATE_MONTH_DAY['input_field'],
            TEST_CONVERT_DATE_MONTH_DAY['output_field'],
            TEST_CONVERT_DATE_MONTH_DAY['input_format'],
            input_timezone=TEST_CONVERT_DATE_DATE['input_timezone'],
            year=TEST_CONVERT_DATE_MONTH_DAY['year'],
        )

        # Infer types
        self.pipeline.infer_types()

        # Save datapackage
        self.pipeline.save_datapackage(TEST_SAVE_PATH)

    def test_save(self):
        '''
        Make sure the pipeline-spec.yaml file is succesfully saved to disc
        '''
        logger.info('Testing save pipeline-spec.yaml file')
        fname = TEST_SAVE_PATH + 'pipeline-spec.yaml'

        # Remove the file
        os.remove(fname)

        # Save to the file
        self.pipeline.save_to_file(TEST_SAVE_PATH + 'pipeline-spec.yaml')
        assert os.path.isfile(fname)

    def test_dpp(self):
        '''
        Test actually running the pipeline with the pipeline-spec.yaml
        file that was generated
        '''
        logger.info('Testing running dpp on the completed yaml file')

        # Remove the old files
        if os.path.isfile(TEST_SAVE_PATH + 'datapackage.json'):
            os.remove(TEST_SAVE_PATH + 'datapackage.json')
        if os.path.isfile(TEST_SAVE_PATH + 'data/default.csv'):
            os.remove(TEST_SAVE_PATH + 'data/default.csv')

        # Call dpp
        completed_process = run(
            f'cd {TEST_SAVE_PATH} && dpp run --no-use-cache --verbose ./test-pipeline',
            shell=True,
        )

        assert completed_process.returncode == 0
        assert os.path.isfile(TEST_SAVE_PATH + 'datapackage.json')
        assert os.path.isfile(TEST_SAVE_PATH + 'data/default.csv')

    def test_setup_obj(self):
        ''' Set up the objects for the rest of the tests to use '''
        with open(TEST_SAVE_PATH + 'datapackage.json') as f:
            self.shared_data['datapackage_data'] = json.load(f)

        with open(TEST_SAVE_PATH + '/data/default.csv') as f:
            reader = csv.reader(f)
            next(reader)
            self.shared_data['first_row'] = next(reader)
            self.shared_data['second_row'] = next(reader)

    def test_concatenate(self):
        logger.info('Testing concatenate')

        # Original file had 23 rows, add an extra row with concatenate
        assert self.shared_data['datapackage_data']['count_of_rows'] == 24

        # The name of the concatenated row
        assert self.shared_data['second_row'][12] == 'Aannelid Test'

    def test_delete_fields(self):
        logger.info('Testing delete')
        assert self.shared_data['datapackage_data']['resources'][0]['schema']['fields'][3]['name'] != 'Lander'
        assert self.shared_data['datapackage_data']['resources'][0]['schema']['fields'][5]['name'] != 'Block Bone'

    def test_sort_field(self):
        logger.info('Testing sort')
        assert self.shared_data['first_row'][12] == 'AAAAnnelid Amage sp.'

    def test_combine_fields(self):
        logger.info('Testing combine fields')
        assert self.shared_data['datapackage_data']['resources'][0]['schema']['fields'][12]['name'] == 'Taxon-Species'

    def test_round_field(self):
        logger.info('Testing round field')
        # Confirm the value was rounded properly
        assert self.shared_data['first_row'][8] == '3.24'

    def test_convert_field_decimal_degrees(self):
        logger.info('Testing convert field to decimal degrees')
        # Confirm lat was succesfully converted
        assert self.shared_data['first_row'][13] == '47.27001666666666324090328998863697052001953125'
        # Confirm long was succesfully converted
        assert self.shared_data['first_row'][14] == '-127.599166666666661740237032063305377960205078125'

    def test_convert_date(self):
        logger.info('Testing convert datetime')
        # Confirm that the datetime was succesfully converted
        assert self.shared_data['first_row'][15] == '2017-07-14 04:00:00'

    def test_infer_types(self):
        logger.info('Testing infer types')
        assert self.shared_data['datapackage_data']['resources'][0]['schema']['fields'][13]['type'] == 'number'
        assert self.shared_data['datapackage_data']['resources'][0]['schema']['fields'][14]['type'] == 'number'
        assert self.shared_data['datapackage_data']['resources'][0]['schema']['fields'][9]['type'] == 'date'
        assert self.shared_data['datapackage_data']['resources'][0]['schema']['fields'][10]['type'] == 'datetime'
        assert self.shared_data['datapackage_data']['resources'][0]['schema']['fields'][15]['type'] == 'datetime'


    def teardown_class(self):
        pass

def test_parse_pipeline_spec():
    with open(TEST_PATH + 'pipeline-spec-test.yaml', 'r') as fd:
        file_contents = fd.read()
    pipeline = BcodmoPipeline(pipeline_spec=file_contents)
    pipeline.save_to_file(TEST_SAVE_PATH + 'pipeline-spec-test-copy.yaml')
    with open(TEST_SAVE_PATH + 'pipeline-spec-test-copy.yaml', 'r') as fd:
        assert fd.read() == file_contents

#TODO Write tests for all lat lon formats

