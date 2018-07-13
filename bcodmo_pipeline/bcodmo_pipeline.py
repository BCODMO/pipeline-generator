import logging

logging.basicConfig(
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

class BcodmoPipeline:
    def __init__(self, name, title, description):
        name.replace(' ', '-')
        self.intro = f'''{name}:
  title: {title}
  description: {description}
  pipeline:'''
        self._steps = []
        self._resources = []

    def save_to_file(self, file_path):
        with open(file_path, 'w') as fd:
            num_chars = fd.write(self.intro + ''.join(self.get_steps()))

    def get_steps(self):
        string_steps = []
        for step in self._steps:
            string_step = self._obj_to_string([step])
            string_steps.append(string_step)
        return string_steps


    def add_resource(self, url, name='default', stream=True):
        dict_step = {
            'run': 'add_resource',
            'parameters': {
                'url': url,
                'name': name,
            },
        }
        self._steps.append(dict_step)
        self._resources.append(name)
        if stream:
            self._stream_remote_resources()

    def concatenate(self, fields, target_name='default', sources=None):
        '''
        Concatenate multiple resources together

        - fields is a dictionary of lists where the key is the target column and
        the list is a list of source columns. Leave list empty for the same column name
        - target_name is the name of the new resource
        - sources is the resources to concatenate. Defaults to all the resources
        '''
        dict_step = {
            'run': 'concatenate',
            'parameters': {
                'target': {
                    'name': target_name,
                    'path': f'data/{target_name}',
                },
                'fields': fields,
            }
        }
        if sources:
            dict_step['parameters']['sources'] = resources
            self._resources = list(filter(lambda resource: resource in resources, self._resources))
            self._resources.append(target_name)
        else:
            self._resources = [target_name]
        self._steps.append(dict_step)


    def delete_fields(self, fields, resources=None):
        '''
        Delete a list of fields
        '''
        dict_step = {
            'run': 'delete_fields',
            'parameters': {
                'fields': fields
            }
        }
        if resources:
            dict_step['parameters']['resources'] = resources
        self._steps.append(dict_step)

    def sort(self, field, resources=None):
        dict_step = {
            'run': 'sort',
            'parameters': {
                'resources': resources if resources else self._resources,
                'sort-by': f'"{{{field}}}"',
            }
        }
        self._steps.append(dict_step)

    def combine_fields(self, output_field, fields, resources=None):
        with_string = ' '.join([f'{{{field}}}' for field in fields])
        dict_step = {
            'run': 'add_computed_field',
            'parameters': {
                'fields': [{
                    'operation': 'format',
                    'target': output_field,
                    'with': f'"{with_string}"',
                }]
            }
        }
        if resources:
            dict_step['parameters']['resources'] = resources
        self._steps.append(dict_step)

    def round_field(
        self,
        field,
        digits,
        resources=None,
        custom_processor='bcodmo-pipeline-processors.round_fields'
    ):
        '''
        Round a field

        Takes in the field name and the number of digits to round to

        The pipeline will throw an error if the field cannot be converted to a float
        '''
        dict_step = {
            'run': custom_processor,
            'parameters': {
                'fields': [{
                    'name': field,
                    'digits': digits,
                }]
            }
        }
        if resources:
            dict_step['parameters']['resources'] = resources
        self._steps.append(dict_step)

    def convert_field_decimal_degrees(
        self,
        input_field,
        output_field,
        input_format,
        pattern,
        directional=None,
        resources=None,
        custom_processor='bcodmo-pipeline-processors.convert_to_decimal_degrees'
    ):
        '''
        Convert a field to decimal degrees and saves in a new field

        Takes multiple formats for the input string:
           degrees-minutes-seconds - degrees, minutes, seconds
           degrees-decimal_minutes - degress, decimal minutes

        The pattern is defined with %parameter% values, ex:
          "sometext%degrees% and%minutes% othertext%seconds%moretext"

        If %directional% is not defined in the pattern it must be defined as an input
        This is not checked in this function, rather in the pipeline
        '''
        dict_step = {
            'run': custom_processor,
            'parameters': {
                'fields': [{
                    'input_field': input_field,
                    'output_field': output_field,
                    'format': f'"{input_format}"',
                    'pattern': f'"{pattern}"',
                }]
            }
        }
        if resources:
            dict_step['parameters']['resources'] = resources
        if directional:
            dict_step['parameters']['fields']['directional'] = f'"{directional}"'
        self._steps.append(dict_step)

    def convert_date(
        self,
        input_field,
        output_field,
        input_format,
        input_timezone=None,
        output_format='%Y-%m-%dT%H:%M:%SZ',
        output_timezone='UTC',
        year=None,
        custom_processor='bcodmo-pipeline-processors.convert_date',
        resources=None,
    ):
        '''
        Convert a date with a given format to some output format

        If the date string does not contain timezone information
        input_timezone must be included (this is only enforced by the pipeline)
        '''
        dict_step = {
            'run': custom_processor,
            'parameters': {
                'fields': [{
                    'input_field': f'"{input_field}"',
                    'input_format': f'"{input_format}"',
                    'output_field': f'"{output_field}"',
                    'output_format': f'"{output_format}"',
                    'output_timezone': f'"{output_timezone}"',
                }],
            }
        }
        if input_timezone:
            dict_step['parameters']['fields'][0]['input_timezone'] = f'"{input_timezone}"'
        if year:
            dict_step['parameters']['fields'][0]['year'] = f'"{year}"'
        if resources:
            dict_step['parameters']['resources'] = resources
        self._steps.append(dict_step)



    def infer_types(
        self,
        custom_processor='bcodmo-pipeline-processors.infer_types',
        resources=None,
    ):
        dict_step = {'run': custom_processor}
        if resources:
            dict_step['parameters'] = {
                'resources': resources,
            }
        self._steps.append(dict_step)



    def save_datapackage(self, path):
        dict_step = {
            'run': 'dump.to_path',
            'parameters': {
                'out-path': path,
            }
        }
        self._steps.append(dict_step)

    def _stream_remote_resources(self):
        dict_step = {
            'run': 'stream_remote_resources',
            'cache': True,
        }
        self._steps.append(dict_step)

    def _obj_to_string(self, obj, depth=2):
        '''
        A recursive function that generates proper yaml format
        given an object
        '''

        # Handle list case
        if type(obj) == list:
            if len(obj) == 0:
                return ' []'
            return ''.join([
                f'''
{'  ' * depth}-{self._obj_to_string(step, depth + 1)}'''
                for step in obj
            ])

        # Handle dictionary case
        if type(obj) == dict:
            return ''.join([
                f'''
{'  ' * depth}{key}:{self._obj_to_string(value, depth + 1)}'''
                for key, value in obj.items()
            ])

        # Base case where the value is not a list or a dict
        return f' {obj}'
