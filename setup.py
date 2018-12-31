from setuptools import setup, find_packages

required = [
    'PyYAML==3.13',
    'datapackage-pipelines==2.0.0',
    'dataflows=1.5.0'
]


setup(
    name='pipeline-generator',
    version='v0.0.1dev',
    description='BCODMO Pipelines Library',
    author='BCODMO',
    author_email='cschloer@whoi.edu',
    url='https://github.com/bcodmo/pipeline-generator',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)

