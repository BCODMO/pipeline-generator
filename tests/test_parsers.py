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


class TestParsers():

    def setup_class(self):
        pass

    def test_fixed_width(self):
        pipeline = BcodmoPipeline(
            name=TEST_NAME, title=TEST_TITLE, description=TEST_DESCRIPTION)

        pipeline = BcodmoPipeline(
            name='test_fixed_width',
            title='test_fixed_width',
            description='test fixed width',
        )
        for step in FIXED_WIDTH_TEST_STEPS:
            pipeline.add_step(step)
        res = pipeline.run_pipeline()
        logger.info(res['cache_id'])
        logger.info(res['resources']['default']['rows'][0])
        logger.info(res['resources']['default']['header'])
        assert res['resources']['default']['rows'][0][0] == '2.0'
        assert res['resources']['default']['rows'][0][1] == '3.0'
        assert res['resources']['default']['rows'][0][2] == '12.0'
        assert res['resources']['default']['header'][0] == 'STNNBR nan'
        assert res['status_code'] == 0


    def teardown_class(self):
        pass

