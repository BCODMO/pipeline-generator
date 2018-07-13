import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(os.path.join(here, "bcodmo-pipeline", "__version__.py")) as f:
    exec(f.read(), about)

required = []

setup(
    name='bpvalve',
    version=about['__version__'],
    description='BCODMO Pipelines Library',
    author='BCODMO',
    author_email='admin@blocpower.org',
    url='https://github.com/Blocp/bpvalve',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)

