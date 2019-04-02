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


    def run_pipeline(self, step_num):
        pipeline = BcodmoPipeline(
            name=TEST_NAME,
            title=TEST_TITLE,
            description=TEST_DESCRIPTION,
        )
        for step in TEST_STEPS[0:step_num]:
            pipeline.add_step(step)

        res = pipeline.run_pipeline(
            cache_id=self.shared_data['cache_id'],
            #verbose=True,
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
        _, _, res = self.run_pipeline(2)
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
    @pytest.mark.skip(reason='standard_processor')
    def test_concatenate(self):
        rows, _, res = self.run_pipeline(3)
        assert len(res['datapackage']['resources']) == 1
        assert res['datapackage']['resources'][0]['name'] == 'default'
        assert len(rows) == 24

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    @pytest.mark.skip(reason='standard_processor')
    def test_delete_fields(self):
        _, prev_fields, _ = self.run_pipeline(3)
        assert prev_fields[3]['name'] == 'Lander'
        _, fields, _ = self.run_pipeline(4)
        assert fields[3]['name'] != 'Lander'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    @pytest.mark.skip(reason='standard_processor')
    def test_sort(self):
        rows, _, _= self.run_pipeline(5)
        assert rows[0][1] == 'AAAAnnelid'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    @pytest.mark.skip(reason='standard_processor')
    def test_combine_fields(self):
        rows, fields, _ = self.run_pipeline(6)
        assert fields[12]['name'] == 'Taxon-Species'
        assert rows[0][12] == 'AAAAnnelid Amage sp.'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_round_field(self):
        rows, _, _ = self.run_pipeline(7)
        assert rows[0][8] == '3.24'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_convert_field_decimal_degree(self):
        rows, _, _ = self.run_pipeline(9)
        assert rows[0][13] == '47.27001666666666324090328998863697052001953125'
        assert rows[0][14] == '-127.599166666666661740237032063305377960205078125'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_convert_date(self):
        rows, _, _ = self.run_pipeline(11)
        assert rows[0][15] == '2017-07-14 04:00:00'
        assert rows[0][16] == '2015-04-16 04:56:00'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_boolean_add_computed_field(self):
        rows, _, _ = self.run_pipeline(12)
        assert rows[0][17] == 'SHOULD BE THIS'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_add_resource_metadata(self):
        _, _, res = self.run_pipeline(13)
        assert res['datapackage']['resources'][0]['schema']['missingKeys'] == ['nd', '']

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_split_column(self):
        rows, fields, _ = self.run_pipeline(13)
        assert fields[2]['name'] == 'Species'

        rows, fields, _ = self.run_pipeline(14)
        # Test the delete_input parameter
        assert fields[2]['name'] != 'Species'
        assert rows[0][17] == 'Amage'
        assert rows[0][18] == 'sp.'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_reorder(self):
        _, fields, _ = self.run_pipeline(15)

        assert [f['name'] for f in fields] == TEST_REORDER_FIELDS['fields']

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_reorder(self):
        _, fields, _ = self.run_pipeline(15)

        assert [f['name'] for f in fields] == TEST_REORDER_FIELDS['fields']

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_rename(self):
        _, fields, _ = self.run_pipeline(16)
        assert fields[0]['name'] == 'Latnew_name'

    @pytest.mark.skipif(TEST_DEV, reason='test development')
    def test_fixedwidth(self):
        _, _, res = self.run_pipeline(16)
        assert 'fixedwidth' not in res['resources']

        _, _, res = self.run_pipeline(17)
        assert 'fixedwidth' in res['resources']
        rows = res['resources']['fixedwidth']['rows']
        fields = res['datapackage']['resources'][1]['schema']['fields']
        assert fields[0]['name'] == 'STNNBR nan'
        assert rows[0][0] == '2'
        assert rows[0][1] == '3'
        assert rows[0][2] == '12'

    def teardown_class(self):
        pass
