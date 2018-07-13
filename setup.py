from setuptools import setup, find_packages

required = []

setup(
    name='bpvalve',
    version='v0.0.1dev',
    description='BCODMO Pipelines Library',
    author='BCODMO',
    author_email='admin@blocpower.org',
    url='https://github.com/Blocp/bpvalve',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)

