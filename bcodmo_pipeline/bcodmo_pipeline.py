import csv
import io
import json
import logging
import os
import shutil
from subprocess import (
    check_output,
    STDOUT,
    CalledProcessError,
)
import time
import uuid
import yaml
import re

from .constants import VALID_OBJECTS

logging.basicConfig(
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

# Day in seconds
DAY = 60 * 60 * 24

class BcodmoPipeline:
    def __init__(self, *args, **kwargs):
        if 'pipeline_spec' in kwargs:
            self.name, self.title, \
                self.description, self._steps \
                = self._parse_pipeline_spec(kwargs['pipeline_spec'])
        else:
            self.name = kwargs['name']
            self.title = kwargs['title']
            self.description = kwargs['description']
            self._steps = []
            if 'steps' in kwargs:
                for step in kwargs['steps']:
                    self.add_generic(step)

    def save_to_file(self, file_path, steps=None):
        if not steps:
            steps = self._steps
        with open(file_path, 'w') as fd:
            num_chars = fd.write(self._get_yaml_format(steps=steps))

    def get_yaml(self):
        return self._get_yaml_format()

    def get_object(self):
        return {
            self.name: {
                'title': self.title,
                'description': self.description,
                'pipeline': self._steps,
            }
        }

    def add_generic(self, obj):
        self._confirm_valid(obj)
        self._steps.append(obj)

    def run_pipeline(self, cache_id=None, verbose=False, num_rows=1):
        '''
        Runs the datapackage pipelines for this pipeline

        - On fail, return error message
        - On success, return datapackage.json contents and the
        first line of each resulting csv

        - On both fail and success return a status code and a
        unique id that can be passed back in to this function
        to use the cache
        '''
        if not cache_id:
            cache_id = str(uuid.uuid1())

        # We have to check the cache_id value since it's
        # potentially being passed in from the outside
        pattern = re.compile(
            r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}'
            r'-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        )
        if not pattern.match(cache_id):
            raise Exception('The unique ID that was provided was not in uuid format')

        ''' IMPORTANT '''
        # If the file structure between this file and the tmp folder
        # ever changes this code must change
        file_path = os.path.dirname(os.path.realpath(__file__))
        path = f'{file_path}/tmp/{cache_id}'
        # Create the directory and file
        if not os.path.exists(path):
            os.makedirs(path)
        try:
            # Create a new save step so we can access the data here
            new_save_step = {
                'run': 'dump.to_path',
                'parameters': {
                    'out-path': path,
                }
            }
            new_steps = self._steps + [new_save_step]
            self.save_to_file(f'{path}/pipeline-spec.yaml', steps=new_steps)

            try:
                # Remove the data folder
                shutil.rmtree(f'{path}/data', ignore_errors=True)
                if verbose:
                    verbose_string = '--verbose'
                else:
                    verbose_string = ''
                completed_process = check_output(
                    f'cd {path}/.. && dpp run {verbose_string} ./{cache_id}/{self.name}',
                    shell=True,
                    stderr=STDOUT,
                    universal_newlines=True,
                )
            except CalledProcessError as e:
                return {
                    'status_code': e.returncode,
                    'error_text': e.output,
                    'cache_id': cache_id,
                }

            with open(f'{path}/datapackage.json') as f:
                datapackage = json.load(f)

            resources = {}
            # Go through all of outputted data
            data_folder = f'{path}/data'
            if os.path.exists(data_folder):
                for fname in os.listdir(data_folder):
                    resource_name, ext = os.path.splitext(fname)
                    # TODO support json format?
                    if ext != '.csv':
                        raise Exception(f'Non csv formats are not supported: {fname}')
                    data_file_path = f'{data_folder}/{fname}'
                    with open(data_file_path) as f:
                        reader = csv.reader(f)
                        header = next(reader)
                        rows = []
                        try:
                            if num_rows >= 0:
                                for i in range(num_rows):
                                    rows.append(next(reader))
                            else:
                                while True:
                                    row = next(reader)
                                    rows.append(row)
                        except StopIteration:
                            pass
                        resources[resource_name] = {
                            'header': header,
                            'rows': rows,
                        }

            return {
                'status_code': 0,
                'cache_id': cache_id,
                'datapackage': datapackage,
                'resources': resources,
            }
        finally:
            try:
                # Clean up the directory, deleting old folders
                cur_time = time.time()
                dirs = [
                    folder_name for folder_name in os.listdir(f'{file_path}/tmp')
                    if not folder_name.startswith('.')
                ]
                for folder_name in dirs:
                    folder = f'{file_path}/tmp/{folder_name}'
                    st = os.stat(folder)
                    modified_time = st.st_mtime
                    age = cur_time - modified_time

                    if age > DAY:
                        shutil.rmtree(folder)
            except Exception as e:
                logger.info(f'There was an error trying to clean up folder: {str(e)}')
                logger.error(vars(e))

    def _confirm_valid(self, obj):
        ''' Confirm that an object is valid in the pipeline '''
        if type(obj) != dict:
            raise Exception('Object must be a dictionary')

        # Confirm that the processor name is correct
        proc_name = obj['run']
        if proc_name not in VALID_OBJECTS.keys():
            raise Exception(f'{proc_name} is not a valid processor name')
        rules = VALID_OBJECTS[proc_name]

        # Confirm validity of top level keys
        for key in obj.keys():
            if key not in rules['valid_top_keys']:
                raise Exception(f'{key} not a valid top level key for {proc_name}')

        # Confirm validity of parameters keys
        if 'valid_parameter_keys' in rules and 'parameters' in obj:
            for param_key in obj['parameters'].keys():
                if param_key not in rules['valid_parameter_keys']:
                    raise Exception(
                        f'{param_key} not a valid parameter key for {proc_name}'
                    )

        # Confirm validity of fields keys
        if 'valid_fields_keys' in rules and 'fields' in obj['parameters']:
            for field in obj['parameters']['fields']:
                for fields_key in field.keys():
                    if fields_key not in rules['valid_fields_keys']:
                        raise Exception(f'{fields_key} not a valid fields key for {proc_name}')

        return True


    def _get_yaml_format(self, steps=None):
        if not steps:
            steps = self._steps
        return yaml.dump({
            self.name: {
                'title': self.title,
                'description': self.description,
                'pipeline': steps,
            }
        })

    def _parse_pipeline_spec(self, pipeline_spec):
        stream = io.StringIO(pipeline_spec)
        try:
            res = yaml.load(stream)

            # Get the name
            if type(res) != dict or len(res.keys()) != 1:
                raise Exception('Improperly formatted pipeline-spec.yaml file - must have a single key dictionary as the root')
            name = list(res.keys())[0]

            # Get the title
            if 'title' not in res[name]:
                raise Exception('Title not found while parsing file')
            # Get the description
            if 'description' not in res[name]:
                raise Exception('Description not found while parsing file')
            title = res[name]['title']
            description = res[name]['description']

            # Get the pipeline
            if 'pipeline' not in res[name]:
                raise Exception('Pipeline not found while parsing file')
            pipeline = res[name]['pipeline']

            # Parse the pipeline
            if not type(pipeline) == list:
                raise Exception('Pipeline in file must be a list')
            steps = []
            for step in pipeline:
                steps.append(step)




        except yaml.YAMLError as e:
            raise e
        return name, title, description, steps

