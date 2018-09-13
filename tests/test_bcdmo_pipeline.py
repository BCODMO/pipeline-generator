import csv
import io
import json
import logging
import os
from subprocess import run
import yaml

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

        for step in TEST_STEPS:
            self.pipeline.add_generic(step)


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
            data = json.load(f)
            self.shared_data['datapackage_data'] = data
            self.shared_data['fields'] = data['resources'][0]['schema']['fields']

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
        assert self.shared_data['fields'][3]['name'] != 'Lander'
        assert self.shared_data['fields'][5]['name'] != 'Block Bone'

    def test_sort_field(self):
        logger.info('Testing sort')
        assert self.shared_data['first_row'][12] == 'AAAAnnelid Amage sp.'

    def test_combine_fields(self):
        logger.info('Testing combine fields')
        assert self.shared_data['fields'][12]['name'] == 'Taxon-Species'

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
        assert self.shared_data['first_row'][16] == '2015-04-16 04:56:00'

    def test_boolean_add_computed_field(self):
        logger.info('Testing boolean add computed field')
        # Confirm the value was rounded properly
        assert self.shared_data['first_row'][17] == TEST_BOOLEAN_ADD_COMPUTED_FIELD['number'][1]['value']


    def test_infer_types(self):
        logger.info('Testing infer types')
        assert self.shared_data['fields'][13]['type'] == 'number'
        assert self.shared_data['fields'][14]['type'] == 'number'
        assert self.shared_data['fields'][9]['type'] == 'date'
        assert self.shared_data['fields'][10]['type'] == 'datetime'
        assert self.shared_data['fields'][15]['type'] == 'datetime'

    def test_add_resource_metadata(self):
        resources = self.shared_data['datapackage_data']['resources']
        assert resources[0]['schema']['missingKeys'] == TEST_ADD_SCHEMA_METADATA['missingKeys']



    def test_run_pipeline(self):
        res = self.pipeline.run_pipeline()
        logger.info(res)
        assert res['status_code'] == 0
        # Assert that the datapackage is the same
        with open(TEST_SAVE_PATH + 'datapackage.json') as f:
            data = json.load(f)
            assert data == res['datapackage']

        # Assert that the first line is the same
        with open(TEST_SAVE_PATH + '/data/default.csv') as f:
            reader = csv.reader(f)
            header = next(reader)
            row = next(reader)
            assert header == res['resources']['default']['header']
            assert row == res['resources']['default']['rows'][0]


    def teardown_class(self):
        pass

def test_parse_pipeline_spec():
    with open(TEST_PATH + 'pipeline-spec-test.yaml', 'r') as fd:
        file_contents = fd.read()
        file_contents_obj = yaml.load(file_contents)
    pipeline = BcodmoPipeline(pipeline_spec=file_contents)
    pipeline.save_to_file(TEST_SAVE_PATH + 'pipeline-spec-test-copy.yaml')
    with open(TEST_SAVE_PATH + 'pipeline-spec-test-copy.yaml', 'r') as fd:
        new_file_contents_obj = yaml.load(fd.read())

        assert new_file_contents_obj == file_contents_obj

#TODO Write tests for all lat lon formats

