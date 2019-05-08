from setuptools import setup, find_packages

required = [
    'PyYAML==5.1',
    'datapackage-pipelines==2.1.5',
    'pyparsing==2.2.0',
    'dataflows==0.0.50',
]


setup(
    name='pipeline-generator',
    version='v0.0.4c',
    description='BCODMO Pipelines Library',
    author='BCODMO',
    author_email='conrad.schloer@gmail.com',
    url='https://github.com/bcodmo/pipeline-generator',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)

