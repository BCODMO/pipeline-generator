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
        logger.info(res)
        assert res['status_code'] == 0


    def teardown_class(self):
        pass

