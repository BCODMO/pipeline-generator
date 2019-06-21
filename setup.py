from setuptools import setup, find_packages

required = [
    'PyYAML==5.1',
    #'datapackage-pipelines==2.1.6',
    # Testing full outer join
    'datapackage-pipelines @ git+https://github.com/okfn/datapackage-pipelines.git@full-outer-join',
    'pyparsing==2.2.0',
    #'dataflows==0.0.55',
    # Testing full-outer-join
    'dataflows @ git+https://github.com/roll/dataflows.git@full-outer-join',
    'bcodmo_processors @ git+https://git@github.com/BCODMO/bcodmo_processors.git@master',
]


setup(
    name='pipeline-generator',
    version='v0.0.4e',
    description='BCODMO Pipelines Library',
    author='BCODMO',
    author_email='conrad.schloer@gmail.com',
    url='https://github.com/bcodmo/pipeline-generator',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)

