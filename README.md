# data-pipeline
Generates a pipeline .yml file

Run with python 3.6.5

# Testing

To test, create a data folder in `/tests/`


Create the ENV variables 
```
TEST_PATH=/Path/To/tests/
DPP_PROCESSOR_PATH=/Path/To/Processor/Submodule
```

Processor submodule should simply be in `./bcodmo_pipeline/processors` (but you should write out the whole path)

Run `pytest`

# Running DPP locally

To run the `dpp` command locally using the custom processors located in this repository, simply clone this reposistory and add the environment variable `DPP_PROCESSOR_PATH`.
If this repository is located at $GENERATOR_REPO, the environment variable will probably be `$GENERATOR_REPO/bcodmo_pipeline/processors`.
You can add environment variables manually using `export DPP_PROCESSOR_PATH=$PUT_PATH_HERE` or you can place all of your environment variables in a .env file and run the following commands:
```
set -a
source .env
```

Now when using `dpp` it will first look inside this repository when resolving processors.
