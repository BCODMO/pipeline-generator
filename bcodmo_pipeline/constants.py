VALID_OBJECTS = {
    'add_resource': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
        'valid_parameter_keys': [
            'name',
            'url',
            'format',
            'sheet',
            'headers',
        ],
    },
    'add_metadata': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
    },
    'set_types': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
        'valid_parameter_keys': [
            'resources',
            'types',
        ],
    },
    'stream_remote_resources': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
        'valid_parameter_keys': [
            'resources',
            'limit-rows',
        ],
    },
    'concatenate': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'target',
            'sources',
        ],
    },
    'join': {
        'valid_top_keys': [
            'run',
            'parameters',
            'cache',
        ],
        'valid_parameter_keys': [
            'target',
            'source',
            'fields',
        ],
    },
    'delete_fields': {
        'valid_top_keys': [
            'run',
            'cache',
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
            'cache',
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
            'cache',
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
    'find_replace': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'resources',
        ],
        'valid_fields_keys': [
            'name',
            'patterns',
        ],
    },
    'bcodmo_pipeline_processors.round_fields': {
        'valid_top_keys': [
            'run',
            'cache',
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
    'bcodmo_pipeline_processors.convert_to_decimal_degrees': {
        'valid_top_keys': [
            'run',
            'cache',
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
            'directional',
        ],
    },
    'bcodmo_pipeline_processors.convert_date': {
        'valid_top_keys': [
            'run',
            'cache',
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
    'bcodmo_pipeline_processors.infer_types': {
        'valid_top_keys': [
            'cache',
            'run',
        ],
    },
    'dump.to_path': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
        'valid_parameter_keys': [
            'out-path',
        ],
    },
    'bcodmo_pipeline_processors.boolean_add_computed_field': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
        'valid_parameter_keys': [
            'fields',
            'resources',
        ],
        'valid_fields_keys': [
            'functions',
            'target',
        ],
    },
    'bcodmo_pipeline_processors.add_schema_metadata': {
        'valid_top_keys': [
            'run',
            'cache',
            'parameters',
        ],
    },
}
