from setuptools import setup, find_packages

required = [
    'PyYAML==5.1',
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

