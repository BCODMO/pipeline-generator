import csv
import asyncio
import psutil
import threading
import io
import json
from contextlib import redirect_stderr
import logging
import os
import sys
import shutil
import signal
from datapackage_pipelines.manager import run_pipelines
from datapackage_pipelines.manager.tasks import async_execute_pipeline
from datapackage_pipelines.status import status_mgr
from datapackage_pipelines import pipelines
from datapackage_pipelines.utilities.execution_id import gen_execution_id
import subprocess
import time
import uuid
import yaml
import re

import bcodmo_processors

logging.basicConfig(
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

FILE_PATH = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = f'{FILE_PATH}/tmp'

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

    def save_to_file(self, save_file_path, steps=None):
        if not steps:
            steps = self._steps
        with open(save_file_path, 'w') as fd:
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

    @staticmethod
    def delete_pipeline_data(cache_id):
        folder = f'{FILE_PATH}/tmp/{cache_id}'
        shutil.rmtree(folder)
        return {
            'status_code': 0,
        }


    @staticmethod
    def get_pipeline_data(cache_id, num_rows=-1):
        try:
            num_rows = int(num_rows)
        except ValueError:
            raise Exception(f'The passed in parameter num_rows "{num_rows}" was not able to be parsed into an integer')
        datapackage = {}
        yaml = None
        resources = {}

        cache_folder = f'{FILE_PATH}/tmp/{cache_id}'
        pipeline_spec_file = f'{cache_folder}/pipeline-spec.yaml'
        if os.path.exists(pipeline_spec_file):
            with open(pipeline_spec_file) as f:
                yaml = f.read()

        data_folder = f'{cache_folder}/results'
        if os.path.exists(data_folder):
            datapackage_file = f'{data_folder}/datapackage.json'
            if os.path.exists(datapackage_file):
                with open(datapackage_file) as f:
                    datapackage = json.load(f)
            for root, dirs, files in os.walk(data_folder):
                for fname in files:
                    if fname == 'datapackage.json':
                        continue
                    resource_name, ext = os.path.splitext(fname)
                    # TODO support json format?
                    if ext != '.csv':
                        raise Exception(f'Non csv formats are not supported: {fname}')
                    data_file_path = os.path.join(root, fname)
                    if os.path.exists(data_file_path):
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
            'cache_id': cache_id,
            'datapackage': datapackage,
            'yaml': yaml,
            'resources': resources,
        }

    @staticmethod
    def get_pipeline_status(cache_id, name):
        status = status_mgr(f'{FILE_PATH}/tmp')
        status.initialize()
        pipeline_status = status.get(f'./{cache_id}/{name}')
        start_time = None
        finish_time = None
        pipeline_id = None
        status = None
        success = None
        error_log = None

        if pipeline_status and pipeline_status.last_execution:
            last_execution = pipeline_status.last_execution
            start_time = last_execution.start_time
            pipeline_id = last_execution.pipeline_id
            finish_time = last_execution.finish_time
            success = last_execution.success
            error_log = last_execution.error_log
            status = pipeline_status.state()

        return {
            'start_time': start_time,
            'finish_time': finish_time,
            'pipeline_id': pipeline_id,
            'status': status,
            'error_log': error_log,
            'success': success,
        }

    @staticmethod
    def log_slow_compute(start, cache_id, text):
        elapsed = time.time() - start
        if elapsed > 0.1:
            logging.error(f'Slow compute while {text}: {elapsed} - {cache_id}')

    def run_pipeline_thread(self, cache_id, verbose):
        cache_dir = f'{ROOT_DIR}/{cache_id}'
        pipeline_spec_path = f'{cache_dir}/pipeline-spec.yaml'
        pipeline_id = f'./{cache_id}/{self.name}'

        dpp_command_path, processor_path = self._get_version_paths(self.version)
        os.environ['DPP_PROCESSOR_PATH'] = processor_path
        try:
            # Activate the correct virtual environment
            start = time.time()
            self._activate_virtualenv(self.version)
            BcodmoPipeline.log_slow_compute(start, cache_id, 'activating the virtualenv')

            # Set the verbose string if necessary
            if verbose:
                command_list = [dpp_command_path, 'run', '--verbose', pipeline_id]
            else:
                command_list = [dpp_command_path, 'run', pipeline_id]

            # Start the dpp process
            start = time.time()
            p = subprocess.Popen(
                command_list,
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                cwd=ROOT_DIR,
            )
            BcodmoPipeline.log_slow_compute(start, cache_id, 'creating the process')

            sleep_timer = 1
            start = time.time()
            while p.poll() is None:
                BcodmoPipeline.log_slow_compute(start, cache_id, 'polling the process')
                time.sleep(1)
                if sleep_timer != 5:
                    sleep_timer += 1

                # The pipeline-spec.yaml was deleted, need to end the process now
                if not os.path.exists(pipeline_spec_path):
                    # Get the chilren of the dpp process (the dpp slave process)
                    children = [child.pid for child in psutil.Process(p.pid).children()]

                    # Terminate the parent process
                    p.terminate()
                    # Terminate all of the children processes
                    for child in children:
                        os.kill(child, signal.SIGTERM)

                    # Invalidate the pipeline in the dpp backend
                    status = status_mgr(ROOT_DIR)
                    status.initialize()
                    pipeline_status = status.get(pipeline_id)
                    if pipeline_status:
                        last_execution = pipeline_status.last_execution
                        if last_execution:
                            last_execution.finish_execution(
                                False,
                                {},
                                ['This pipeline was stopped by laminar'],
                            )

                    # One last try
                    if p.poll() is None:
                        p.kill()
                        break
                start = time.time()
        finally:
            # Deactivate the virtualenv - not sure if this is necessary since it is a thread
            start = time.time()
            self._deactivate_virtualenv()
            BcodmoPipeline.log_slow_compute(start, cache_id, 'deactivating the virtualenv')

        # If the pipeline-spec.yaml file has been deleted since this thread started, the
        # whole cache_id folder should be deleted
        if not os.path.exists(pipeline_spec_path) and os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)

    def run_pipeline(self, cache_id=None, verbose=False, num_rows=-1, background=False):
        '''
        Starts a thread that runs the datapackage pipelines for this pipeline

        - On fail, return error message
        - On success, return datapackage.json contents and the resulting csv

        - On both fail and success return a status code and a
        unique id that can be passed back in to this function
        to use the cache

        - if run in the background use the static functions get_pipeline_status and
          get_pipeline_data to access the results.
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
        cache_dir = f'{ROOT_DIR}/{cache_id}'
        results_folder = f'{cache_dir}/results'
        # Create the directory and file
        if not os.path.exists(cache_dir):
            start = time.time()
            os.makedirs(cache_dir)
            BcodmoPipeline.log_slow_compute(start, cache_id, 'creating the directories')
        try:
            start = time.time()
            self.save_to_file(f'{cache_dir}/pipeline-spec.yaml.original', steps=self._steps)
            BcodmoPipeline.log_slow_compute(start, cache_id, 'creating the pipeline-spec.original.yaml file')
            # Create a new save step so we can access the data here
            new_save_step = {
                'run': 'dump_to_path',
                'parameters': {
                    'out-path': results_folder,
                }
            }
            new_steps = self._steps + [new_save_step]
            start = time.time()
            self.save_to_file(f'{cache_dir}/pipeline-spec.yaml', steps=new_steps)
            BcodmoPipeline.log_slow_compute(start, cache_id, 'creating the pipeline-spec.yaml file')

            # Remove the results folder
            start = time.time()
            shutil.rmtree(results_folder, ignore_errors=True)
            BcodmoPipeline.log_slow_compute(start, cache_id, 'removing the results folder')

            start = time.time()
            pipeline_id = f'./{cache_id}/{self.name}'
            status = status_mgr(ROOT_DIR)
            status.initialize()
            pipeline_status = status.get(pipeline_id)
            last_execution = pipeline_status.last_execution
            BcodmoPipeline.log_slow_compute(start, cache_id, 'checking the status before creating a thread')
            old_start_time = None
            if last_execution:
                old_start_time = last_execution.start_time

            start = time.time()
            x = threading.Thread(target=self.run_pipeline_thread, args=(cache_id, verbose,), daemon=True)
            BcodmoPipeline.log_slow_compute(start, cache_id, 'creating the thread')
            start = time.time()
            x.start()
            BcodmoPipeline.log_slow_compute(start, cache_id, 'starting the thread')

            if background:
                while True:
                    # Loop until the next pipeline has started
                    start = time.time()
                    status = status_mgr(ROOT_DIR)
                    status.initialize()
                    pipeline_status = status.get(pipeline_id)
                    last_execution = pipeline_status.last_execution
                    BcodmoPipeline.log_slow_compute(start, cache_id, 'checking the status after creating the thread')
                    if last_execution and last_execution.start_time != old_start_time:
                        break
                    if x.is_alive():
                        time.sleep(0.1)
                    else:
                        return {
                            'status_code': 1,
                            'cache_id': cache_id,
                            'yaml': self.get_yaml(),
                            'error_text': 'There was an unknown error in starting the pipeline',
                        }

                return {
                    'status_code': 0,
                    'cache_id': cache_id,
                    'yaml': self.get_yaml(),
                }

            else:
                # Join the thread
                x.join()
                status_dict = BcodmoPipeline.get_pipeline_status(cache_id, self.name)
                if status_dict['success']:
                    pipeline_data = BcodmoPipeline.get_pipeline_data(cache_id, num_rows)
                    return {
                        'status_code': 0,
                        'cache_id': cache_id,
                        'yaml': self.get_yaml(),
                        'datapackage': pipeline_data['datapackage'],
                        'resources': pipeline_data['resources'],
                    }
                else:
                    return {
                        'status_code': 1,
                        'cache_id': cache_id,
                        'yaml': self.get_yaml(),
                        'error_text': status_dict.error_log,
                    }

        finally:
            try:
                start = time.time()
                # Clean up the directory, deleting old folders
                cur_time = time.time()
                dirs = [
                    folder_name for folder_name in os.listdir(f'{FILE_PATH}/tmp')
                    if not folder_name.startswith('.')
                ]
                for folder_name in dirs:
                    folder = f'{FILE_PATH}/tmp/{folder_name}'
                    st = os.stat(folder)
                    modified_time = st.st_mtime
                    age = cur_time - modified_time

                    if age > DAY * 30:
                        shutil.rmtree(folder)
                BcodmoPipeline.log_slow_compute(start, cache_id, 'checking age status of folders after complete')
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


