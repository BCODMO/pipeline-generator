import csv
import io
import json
import logging
import os
import sys
import shutil
from subprocess import (
    check_output,
    STDOUT,
    CalledProcessError,
    call,
)
import time
import uuid
import yaml
import re

import bcodmo_processors

logging.basicConfig(
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

# Day in seconds
DAY = 60 * 60 * 24

class BcodmoPipeline:
    def __init__(self, *args, **kwargs):
        '''
            Bcodmo pipeline class object

            Two options for initialization
                - pipeline_spec (string)
                    A string representing the pipeline-spec.yaml file

                OR

                - name (string)
                    The name of the pipeline
                - title (string)
                    The title of the pipeline
                - description (string)
                    The description of the pipeline
                - steps (list)
                    A list of steps representing the pipeline
                    These steps will be validated using the constants.py file
        '''

        # Attributes for virtualenv
        self.prev_os_path = None
        self.prev_sys_path = None
        self.prev_prefix = None

        if 'pipeline_spec' in kwargs:
            self.name, self.title, \
                self.description, self.version, self._steps \
                = self._parse_pipeline_spec(kwargs['pipeline_spec'])
        else:
            self.name = kwargs['name']
            self.title = kwargs['title']
            self.description = kwargs['description']
            self.version = kwargs['version']
            self._steps = []
            if 'steps' in kwargs:
                for step in kwargs['steps']:
                    self.add_step(step)

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
                'version': self.version,
                'pipeline': self._steps,
            }
        }

    def add_step(self, obj):
        self._confirm_valid(obj)
        self._steps.append(obj)

    def run_pipeline(self, cache_id=None, verbose=False, num_rows=-1):
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
        results_folder = f'{path}/results'
        # Create the directory and file
        if not os.path.exists(path):
            os.makedirs(path)
        try:


            self.save_to_file(f'{path}/pipeline-spec.yaml.original', steps=self._steps)
            # Create a new save step so we can access the data here
            new_save_step = {
                'run': 'dump_to_path',
                'parameters': {
                    'out-path': results_folder,
                }
            }
            new_steps = self._steps + [new_save_step]
            self.save_to_file(f'{path}/pipeline-spec.yaml', steps=new_steps)

            try:
                # Remove the results folder
                shutil.rmtree(results_folder, ignore_errors=True)
                if verbose:
                    verbose_string = '--verbose'
                else:
                    verbose_string = ''

                dpp_command_path, processor_path = self._get_version_paths(self.version)
                os.environ['DPP_PROCESSOR_PATH'] = processor_path
                try:
                    self._activate_virtualenv(self.version)
                    completed_process = check_output(
                        f'cd {path}/.. && {dpp_command_path} run {verbose_string} ./{cache_id}/{self.name}',
                        shell=True,
                        stderr=STDOUT,
                        universal_newlines=True,
                    )
                finally:
                    self._deactivate_virtualenv()
            except CalledProcessError as e:
                error_text = e.output
                new_error_text = ''
                if not verbose:
                    # Attempt to parse the error into something more readable
                    prev_line = None
                    enter_error_detail = False
                    for line in e.output.splitlines():
                        if line.startswith('| ERROR') and enter_error_detail:
                            new_error_text += f'\n{line}'

                        if line == '+--------':
                            if enter_error_detail:
                                new_error_text += f'\n{prev_line}'
                                new_error_text += f'\n{line}'
                                break
                            else:
                                new_error_text = line
                                enter_error_detail = True
                        prev_line = line
                    if new_error_text:
                        error_text = new_error_text
                return {
                    'status_code': e.returncode,
                    'error_text': error_text,
                    'cache_id': cache_id,
                }

            with open(f'{results_folder}/datapackage.json') as f:
                datapackage = json.load(f)

            resources = {}
            # Go through all of outputted data
            if os.path.exists(results_folder):
                for root, dirs, files in os.walk(results_folder):
                    for fname in files:
                        if fname == 'datapackage.json':
                            continue
                        resource_name, ext = os.path.splitext(fname)
                        # TODO support json format?
                        if ext != '.csv':
                            raise Exception(f'Non csv formats are not supported: {fname}')
                        data_file_path = os.path.join(root, fname)
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
        if 'run' not in obj:
            raise Exception(f'"run" must be a key of the step object: {obj.keys()}')
        return True


    def _get_yaml_format(self, steps=None):
        if not steps:
            steps = self._steps
        return yaml.dump({
            self.name: {
                'title': self.title,
                'description': self.description,
                'version': self.version,
                'pipeline': steps,
            }
        }, sort_keys=False)

    def _parse_pipeline_spec(self, pipeline_spec):
        '''
        Parse the pipeline-spec.yaml string that was passed into this class
        '''
        stream = io.StringIO(pipeline_spec)
        try:
            res = yaml.load(stream)

            # Get the name
            if type(res) != dict or len(res.keys()) != 1:
                raise Exception('Improperly formatted pipeline-spec.yaml file - must have a single key dictionary as the root')
            name = list(res.keys())[0]

            # Get the title
            if 'title' not in res[name]:
                title = name
            else:
                title = res[name]['title']
            # Get the description
            if 'description' not in res[name]:
                description = ''
            else:
                description = res[name]['description']

            # Get the version
            if 'version' not in res[name]:
                version = ''
            else:
                version = res[name]['version']

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
        return name, title, description, version, steps

    def _get_virtualenv_dir(self, version):
        if self.version:
            virtualenv_dir = os.path.join('/home/virtualenvs', self.version)
            if os.path.exists(virtualenv_dir):
                return virtualenv_dir
        return None

    def _get_version_paths(self, version):
        # workon the python virtualenv associated with this version
        virtualenv_dir = self._get_virtualenv_dir(version)
        if virtualenv_dir:
            dpp_command_path = os.path.join(virtualenv_dir, 'bin', 'dpp')
            lib_path = os.path.join(virtualenv_dir, 'lib')
            # Assume only one python version in the virtualenv lib folder
            python_version = os.listdir(lib_path)[0]
            processor_path = os.path.join(lib_path, python_version, 'site-packages', 'bcodmo_processors')

            return dpp_command_path, processor_path

        # If no specific dpp command found for this version, just return 'dpp'
        return 'dpp', os.path.dirname(bcodmo_processors.__file__)

    def _activate_virtualenv(self, version):
        # Activate the virtual env
        virtualenv_dir = self._get_virtualenv_dir(version)
        if virtualenv_dir:
            activate_file = os.path.join(virtualenv_dir, "bin", "activate_this.py")

            # Set the env variable for later deactivation
            self.prev_os_path = os.environ['PATH']
            self.prev_sys_path = list(sys.path)
            self.prev_prefix = sys.prefix

            with open(activate_file) as f:
                    code = compile(f.read(), activate_file, "exec")
                    exec(code, { '__file__': activate_file })
            return None
        self.prev_os_path = None
        self.prev_sys_path = None
        self.prev_prefix = None
        return None


    def _deactivate_virtualenv(self):
        # Deactivate the virtualenv
        if self.prev_os_path and self.prev_sys_path and self.prev_prefix:
            os.environ['PATH'] = self.prev_os_path
            sys.path[:0] = self.prev_sys_path
            sys.prefix = self.prev_prefix


