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

Processor submodule should simply be in `./bcodmo_pipeline` (but you should write out the whole path)

Run `pytest`
