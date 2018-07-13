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
        self.pipeline = BcodmoPipeline(TEST_NAME, TEST_TITLE, TEST_DESCRIPTION)

    def test_add_resource(self):
        logger.info('Testing add resource')
        self.pipeline.add_resource(TEST_DATAPACKAGE_URL)
        steps = self.pipeline.get_steps()

        assert steps[len(steps) - 2] == EXPECTED_STEPS[len(steps) - 2]
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_concatenate(self):
        logger.info('Testing concatenating resources')
        self.pipeline.add_resource(
            TEST_CONCAT['datapackage']['url'],
            name=TEST_CONCAT['datapackage']['name'],
        )
        self.pipeline.concatenate(TEST_CONCAT['fields'])

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_delete_fields(self):
        logger.info('Testing delete fields')
        self.pipeline.delete_fields(TEST_DELETE_FIELDS)

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_sort_field(self):
        logger.info('Testing sort field')
        self.pipeline.sort(TEST_SORT_FIELD)

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_combine_fields(self):
        logger.info('Testing combine fields')
        self.pipeline.combine_fields(
            TEST_COMBINE_FIELDS['output_field'],
            TEST_COMBINE_FIELDS['fields'],
        )

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_round_field(self):
        logger.info('Testing round field')
        self.pipeline.round_field(
            TEST_ROUND_FIELD['field'],
            TEST_ROUND_FIELD['digits'],
        )

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_convert_field_decimal_degrees(self):
        logger.info('Testing convert field to decimal degrees')
        self.pipeline.convert_field_decimal_degrees(
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['input_field'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['output_field'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['format'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DD['pattern'],
        )

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

        self.pipeline.convert_field_decimal_degrees(
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['input_field'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['output_field'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['format'],
            TEST_CONVERT_FIELD_DECIMAL_DEGREES_FROM_DMS['pattern'],
        )

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_convert_date(self):
        logger.info('Testing convert datetime')
        self.pipeline.convert_date(
            TEST_CONVERT_DATE_DATE['input_field'],
            TEST_CONVERT_DATE_DATE['output_field'],
            TEST_CONVERT_DATE_DATE['input_format'],
            input_timezone=TEST_CONVERT_DATE_DATE['input_timezone'],
            output_format=TEST_CONVERT_DATE_DATE['output_format'],
            output_timezone=TEST_CONVERT_DATE_DATE['output_timezone'],
        )

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

        self.pipeline.convert_date(
            TEST_CONVERT_DATE_MONTH_DAY['input_field'],
            TEST_CONVERT_DATE_MONTH_DAY['output_field'],
            TEST_CONVERT_DATE_MONTH_DAY['input_format'],
            input_timezone=TEST_CONVERT_DATE_DATE['input_timezone'],
            year=TEST_CONVERT_DATE_MONTH_DAY['year'],
        )

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_infer_types(self):
        logger.info('Testing infer types')
        self.pipeline.infer_types()

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_save_datapackage(self):
        logger.info('Testing saving to datapackage step')
        self.pipeline.save_datapackage(TEST_SAVE_PATH)

        steps = self.pipeline.get_steps()
        assert steps[len(steps) - 1] == EXPECTED_STEPS[len(steps) - 1]

    def test_save(self):
        '''
        Make sure the pipeline-spec.yaml file is succesfully saved to disc
        '''
        logger.info('Testing save')

        # Clear the file
        fd = open(TEST_SAVE_PATH + 'pipeline-spec.yaml', 'w')
        fd.write('')
        fd.close()

        # Save to the file
        self.pipeline.save_to_file(TEST_SAVE_PATH + 'pipeline-spec.yaml')
        with open(TEST_SAVE_PATH + 'pipeline-spec.yaml', 'r') as fd:
            file_contents = fd.read()
        assert file_contents == EXPECTED_INTRO + ''.join(EXPECTED_STEPS)

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
        self.pipeline.save_to_file(TEST_SAVE_PATH + 'pipeline-spec.yaml')

        # Call dpp
        completed_process = run(
            f'cd {TEST_SAVE_PATH} && dpp run --no-use-cache --verbose ./test-pipeline',
            shell=True,
        )

        assert completed_process.returncode == 0
        assert os.path.isfile(TEST_SAVE_PATH + 'datapackage.json')
        assert os.path.isfile(TEST_SAVE_PATH + 'data/default.csv')


        # Test metadata
        with open(TEST_SAVE_PATH + 'datapackage.json') as f:
            data = json.load(f)

        assert data['count_of_rows'] == 24
        assert len(data['resources'][0]['schema']['fields']) == 17
        assert data['resources'][0]['schema']['fields'][12]['name'] == 'Taxon-Species'
        # Confirm lat was typed correctly
        assert data['resources'][0]['schema']['fields'][13]['name'] == 'Lat-converted'
        assert data['resources'][0]['schema']['fields'][13]['type'] == 'number'
        # Confirm long was typed correctly
        assert data['resources'][0]['schema']['fields'][14]['name'] == 'Long-converted'
        assert data['resources'][0]['schema']['fields'][14]['type'] == 'number'

        # Confirm that date was typed properly
        assert data['resources'][0]['schema']['fields'][9]['name'] == 'TestDate'
        assert data['resources'][0]['schema']['fields'][9]['type'] == 'date'

        # Confirm that datetime was typed properly
        assert data['resources'][0]['schema']['fields'][10]['name'] == 'TestDatetime'
        assert data['resources'][0]['schema']['fields'][10]['type'] == 'datetime'
        assert data['resources'][0]['schema']['fields'][15]['type'] == 'datetime'

        # Test CSV values
        with open(TEST_SAVE_PATH + '/data/default.csv') as f:
            reader = csv.reader(f)
            counter = 0
            next(reader)  # Get past the header row

            # Use the first row
            row = next(reader)
            # Confirm we are in the correct row
            assert row[12] == 'AAAAnnelid Amage sp.'
            # Confirm the value was rounded properly
            assert row[8] == '3.24'
            # Confirm lat was succesfully converted
            assert row[13] == '47.27001666666666324090328998863697052001953125'
            # Confirm long was succesfully converted
            assert row[14] == '-127.599166666666661740237032063305377960205078125'
            # Confirm that the datetime was succesfully converted
            assert row[15] == '2017-07-14 04:00:00'

            # Test the second row to confirm concat worked
            row = next(reader)
            assert row[12] == 'Aannelid Test'



    def teardown_class(self):
        logger.info(self.pipeline.intro + ''.join(self.pipeline.get_steps()))

#TODO Write tests for all lat lon formats
