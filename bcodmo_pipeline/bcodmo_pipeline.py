import io
import logging
import yaml
import uuid

from .constants import VALID_OBJECTS

logging.basicConfig(
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

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

    def save_to_file(self, file_path):
        with open(file_path, 'w') as fd:
            num_chars = fd.write(self._get_yaml_format())

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

    def run_pipeline(self):
        unique_id = uuid.uuid1()
        self.save_to_file(f'./bcodmo_pipeline/tmp/{unique_id}/pipeline-spec.yaml')
        pass

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


    def _get_yaml_format(self):
        return yaml.dump({
            self.name: {
                'title': self.title,
                'description': self.description,
                'pipeline': self._steps,
            }
        })

    def _parse_pipeline_spec(self, pipeline_spec):
        stream = io.StringIO(pipeline_spec)
        try:
            res = yaml.load(stream)
            logger.info(res)

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

