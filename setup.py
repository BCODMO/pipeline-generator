from setuptools import setup, find_packages

required = [
    'PyYAML==4.2b1',
    'datapackage-pipelines==2.1.5',
    'pyparsing==2.2.0',
]


setup(
    name='pipeline-generator',
    version='v0.0.4a',
    description='BCODMO Pipelines Library',
    author='BCODMO',
    author_email='conrad.schloer@gmail.com',
    url='https://github.com/bcodmo/pipeline-generator',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)

