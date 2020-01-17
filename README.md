DEPRECRATED WITH LAMINAR v1.0.8 (https://github.com/BCODMO/laminar-web/releases/tag/v1.0.8)



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

