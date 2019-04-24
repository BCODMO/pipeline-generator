import logging
import pytest

from bcodmo_pipeline import BcodmoPipeline
from .constants import *

logging.basicConfig(
    level=logging.DEBUG,
)

logger = logging.getLogger(__name__)

TEST_DEV = True

class TestBcodmoPipeline():

    def setup_class(self):
        self.shared_data = { 'cache_id': None, 'res': None }


    def run_pipeline(self, step_nums):
        pipeline = BcodmoPipeline(
            name=TEST_NAME,
            title=TEST_TITLE,
            description=TEST_DESCRIPTION,
        )
        for step_num in step_nums:
            pipeline.add_step(TEST_STEPS[step_num])

        res = pipeline.run_pipeline(
            cache_id=self.shared_data['cache_id'],
            verbose=True,
        )
        self.shared_data['cache_id'] = res['cache_id']
        self.shared_data['res'] = res
        if 'error_text' in res:
            logger.info(res['error_text'])
        assert res['status_code'] == 0
        r_name = res['datapackage']['resources'][0]['name']
        return (
            res['resources'][r_name]['rows'],
            res['datapackage']['resources'][0]['schema']['fields'],
            res,
        )


    @pytest.mark.skipif(TEST_DEV, reason='test development')
    @pytest.mark.skip(reason='standard_processor')
    def test_load(self):
        _, _, res = self.run_pipeline([0, 1])
        assert len(res['datapackage']['resources']) == 2
        assert res['datapackage']['resources'][0]['name'] == 'default'
        fields = res['datapackage']['resources'][0]['schema']['fields']
        assert len(fields) == 14
        assert fields[0]['name'] == 'Genomic_Method'
        rows1 = res['resources']['default']['rows']
        rows2 = res['resources']['concat']['rows']
        assert len(rows1) == 23
        assert len(rows2) == 1

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_concatenate_res(self):
        rows, fields, res = self.run_pipeline([0, 1, 2])
        assert len(res['datapackage']['resources']) == 1
        assert res['datapackage']['resources'][0]['name'] == 'concat_res'
        assert len(rows) == 24
        assert len(fields) == 15
        assert fields[14]['name'] == 'source_field'
        assert rows[0][14] == 'default'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_concatenate_path(self):
        rows, fields, res = self.run_pipeline([0, 1, 3])
        assert len(res['datapackage']['resources']) == 1
        assert res['datapackage']['resources'][0]['name'] == 'concat_res'
        assert len(rows) == 24
        assert len(fields) == 15
        assert fields[14]['name'] == 'source_field'
        assert rows[0][14] == TEST_DATA_URL
        assert rows[23][14] == TEST_CONCAT['datapackage']['url']

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_concatenate_file(self):
        rows, fields, res = self.run_pipeline([0, 1, 4])
        assert len(res['datapackage']['resources']) == 1
        assert res['datapackage']['resources'][0]['name'] == 'concat_res'
        assert len(rows) == 24
        assert len(fields) == 15
        assert fields[14]['name'] == 'source_field'
        assert rows[0][14] == 'test_data.csv'
        assert rows[23][14] == 'test_concat.csv'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    @pytest.mark.skip(reason='standard_processor')
    def test_delete_fields(self):
        _, prev_fields, _ = self.run_pipeline([0])
        assert prev_fields[3]['name'] == 'Lander'
        _, fields, _ = self.run_pipeline([0, 5])
        assert fields[3]['name'] != 'Lander'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    @pytest.mark.skip(reason='standard_processor')
    def test_sort(self):
        rows, _, _= self.run_pipeline([0, 6])
        assert rows[0][1] == 'AAAAnnelid'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    @pytest.mark.skip(reason='standard_processor')
    def test_combine_fields(self):
        rows, fields, _ = self.run_pipeline([0, 7])
        assert fields[14]['name'] == 'Taxon-Species'
        assert rows[0][14] == 'Bivalve Xylophaga cf. washingtona'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_round_field(self):
        rows, _, _ = self.run_pipeline([0, 8])
        assert rows[0][10] == '1.52'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_convert_field_decimal_degree(self):
        rows, _, _ = self.run_pipeline([0, 9, 10])
        assert rows[0][14] == '43.90870000000000317186277243308722972869873046875'
        assert rows[0][15] == '-125.17305555555554974489496089518070220947265625'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_convert_date(self):
        rows, _, _ = self.run_pipeline([0, 11, 12])
        assert rows[0][14] == '2017-07-03 04:00:00'
        assert rows[0][15] == '2015-03-03 04:56:00'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_boolean_add_computed_field(self):
        rows, _, _ = self.run_pipeline([0, 13])
        assert rows[0][14] == 'SHOULD BE THIS'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_add_resource_metadata(self):
        _, _, res = self.run_pipeline([0, 14])
        assert res['datapackage']['resources'][0]['schema']['missingKeys'] == ['nd', '']

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_split_column(self):
        rows, fields, _ = self.run_pipeline([0])
        assert fields[2]['name'] == 'Species'

        rows, fields, _ = self.run_pipeline([0, 15])
        # Test the delete_input parameter
        assert fields[2]['name'] != 'Species'
        assert rows[0][13] == 'Xylophaga cf.'
        assert rows[0][14] == 'washingtona'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_reorder(self):
        _, fields, _ = self.run_pipeline([0, 16])

        assert [f['name'] for f in fields] == TEST_REORDER_FIELDS['fields']


    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_rename(self):
        _, fields, _ = self.run_pipeline([0, 17])
        assert fields[7]['name'] == 'Latnew_name'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_fixedwidth(self):
        _, _, res = self.run_pipeline([18])
        assert 'fixedwidth' in res['resources']
        rows = res['resources']['fixedwidth']['rows']
        fields = res['datapackage']['resources'][0]['schema']['fields']
        assert fields[0]['name'] == 'STNNBR nan'
        assert rows[0][0] == '2'
        assert rows[0][1] == '3'
        assert rows[0][2] == '12'

    def teardown_class(self):
        pass
