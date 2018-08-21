from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
import logging
import re

logging.basicConfig(
    level=logging.WARNING,
)
logger = logging.getLogger(__name__)

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'))
fields = parameters.get('fields', [])

def modify_datapackage(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            new_fields = [{
                'name': f['output_field'],
                'type': 'number',
            } for f in fields]
            resource_['schema']['fields'] += new_fields
    return datapackage_


def process_resource(rows):
    for row in rows:
        for field in fields:
            input_field = field['input_field']
            if input_field not in row:
                raise Exception(f'Input field {input_field} not found in row')
            row_value = row[input_field]
            output_field = field['output_field']

            # If directional is user inputted, get it
            directional_inputted = 'directional' in field and field['directional']

            pattern = field['pattern']
            input_format = field['format']

            # Input is degrees, minutes, seconds
            if input_format == 'degrees-minutes-seconds':

                if '%degrees%' not in pattern:
                    raise Exception('%degrees% not in the inputted pattern for converting to decimal degrees')
                if '%minutes%' not in pattern:
                    raise Exception('%minutes% not in the inputted pattern for converting to decimal degrees')
                if '%seconds%' not in pattern:
                    raise Exception('%seconds% not in the inputted pattern for converting to decimal degrees')

                degrees = find_numeric_coordinate_parameter('degrees', pattern, row_value)
                minutes = find_numeric_coordinate_parameter('minutes', pattern, row_value)
                seconds = find_numeric_coordinate_parameter('seconds', pattern, row_value)
                if seconds >= 60:
                    raise Exception(f'Seconds are greater than 60: {seconds}')
                decimal_minutes = minutes + (seconds / 60)

                if directional_inputted:
                    directional = field['directional']
                elif '%directional%' in pattern:
                    directional = find_directional_parameter(pattern, row_value)
                else:
                    directional = None

#                logger.debug(f'Row: {row_value}')
#                logger.debug(f'Degrees: {degrees}')
#                logger.debug(f'Decimal minuets: {decimal_minutes}')
#                logger.debug(f'Directional: {directional}')

            # Input is degrees, decimal seconds
            elif input_format == 'degrees-decimal_minutes':
                if '%degrees%' not in pattern:
                    raise Exception('%degrees% not in the inputted pattern for converting to decimal degrees')
                if '%decimal_minutes%' not in pattern:
                    raise Exception('%decimal_minutes% not in the inputted pattern for converting to decimal degrees')

                degrees = find_numeric_coordinate_parameter('degrees', pattern, row_value)
                decimal_minutes = find_numeric_coordinate_parameter('decimal_minutes', pattern, row_value)
                if directional_inputted:
                    directional = field['directional']
                elif '%directional%' in pattern:
                    directional = find_directional_parameter(pattern, row_value)
                else:
                    directional = None

#                logger.debug(f'Row: {row_value}')
#                logger.debug(f'Degrees: {degrees}')
#                logger.debug(f'Decimal minuets: {decimal_minutes}')
#                logger.debug(f'Directional: {directional}')

            if decimal_minutes >= 60:
                raise Exception(f'Decimal minutes are greater than 60: {decimal_minutes}')

            if degrees < 0:
                decimal_degrees = degrees - (decimal_minutes / 60)
            else:
                # TODO: is it always true that decimal_minutes will be positive?
                decimal_degrees = degrees + (decimal_minutes / 60)

            # Handle  change in sign if directional requires
            logger.info(f'Decimal degrees is {decimal_degrees}')
            logger.info(f'Direcitonal is {directional}')
            logger.info(f'Direcitonal type is {type(directional)}')
            if (directional == 'W' or directional == 'S') and decimal_degrees >= 0:
                logger.info('Direcetional was correct, multiplying by -1')
                decimal_degrees *= -1

            logger.info(f'After, decimal degrees is {decimal_degrees}')
            row[output_field] = decimal_degrees

        yield row

def find_numeric_coordinate_parameter(name, pattern, row_value):
    '''
    Takes in a name, pattern and row value and returns the value corresponding to the named parameter

    name should be one of 'degrees', 'decimal_minutes', 'minutes', or 'seconds'
    '''
    # Create the regular expression
    regex = remove_all_coordinate_parameters(
        # Escape any regex characters that might have been present in the original string
        re.escape(pattern).replace(
            f'\\%{name}\\%', '(\d*\.?\d+)',
        )
    )
    match = re.match(regex, row_value)
    if not match:
        raise Exception(f'Match not found for {name}: expression \"{regex}\" and value \"{row_value}\"')
    return float(match.group(1))

def find_directional_parameter(pattern, row_value):
    '''
    Takes in a pattern and row value and returns the directional value
    '''
    # Create the regular expression
    regex = remove_all_coordinate_parameters(
        # Escape any regex characters that might have been present in the original string
        re.escape(pattern).replace(
            f'\\%directional\\%', '(\w+)',
        )
    )
    match = re.match(regex, row_value)
    if not match:
        raise Exception(f'Match not found for directional: expression \"{regex}\" and value \"{row_value}\"')
    directional = match.group(1)
    if directional in ['N', 'n', 'North', 'NORTH']:
        return 'N'
    elif directional in ['S', 's', 'South', 'SOUTH']:
        return 'S'
    elif directional in ['E', 'e', 'East', 'EAST']:
        return 'E'
    elif directional in ['W', 'w', 'West', 'WEST']:
        return 'W'
    raise Exception(f'Directional {directional} doesn\t match any known directional pattern')

def remove_all_coordinate_parameters(string):
    ''' Converts all coordinate parameters into .* wildcard matches '''
    return string.replace(
        '\\%degrees\\%', '.*',
    ).replace(
        '\\%decimal_minutes\\%', '.*',
    ).replace(
        '\\%minutes\\%', '.*',
    ).replace(
        '\\%seconds\\%', '.*',
    ).replace(
        '\\%directional\\%', '.*',
    )


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield process_resource(resource)


spew(modify_datapackage(datapackage), process_resources(resource_iterator))
