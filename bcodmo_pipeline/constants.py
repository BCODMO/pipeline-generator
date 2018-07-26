VALID_OBJECTS = {
    'add_resource': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'name',
            'url',
            'format',
        ],
    },
    'stream_remote_resources': {
        'valid_top_keys': [
            'run',
            'cache',
            'resources',
        ],
        'valid_parameter_keys': [
            'resources',
        ],
    },
    'concatenate': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'target',
            'sources',
        ],
    },
    'delete_fields': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'resources',
        ],

    },
    'sort': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'sort-by',
            'resources',
        ],
    },
    'add_computed_field': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'resources',
        ],
        'valid_fields_keys': [
            'operation',
            'target',
            'source',
            'with',
        ],
    },
    'bcodmo-pipeline-processors.round_fields': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'resources',
        ],
        'valid_fields_keys': [
            'digits',
            'name',
        ],
    },
    'bcodmo-pipeline-processors.convert_to_decimal_degrees': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'resources',
        ],
        'valid_fields_keys': [
            'format',
            'input_field',
            'output_field',
            'pattern',
        ],
    },
    'bcodmo-pipeline-processors.convert_date': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'resources',
        ],
        'valid_fields_keys': [
            'input_field',
            'input_format',
            'input_timezone',
            'output_field',
            'output_format',
            'output_timezone',
            'year',
        ],
    },
    'bcodmo-pipeline-processors.infer_types': {
        'valid_top_keys': [
            'run',
        ],
    },
    'dump.to_path': {
        'valid_top_keys': [
            'run',
            'parameters',
        ],
        'valid_parameter_keys': [
            'out-path',
        ],
    },
}

