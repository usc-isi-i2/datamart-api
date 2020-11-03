# Datamart Data Refresher
---
This folder contains the `Data Refresher`, a tool for appending one or more data files to Datamart. In order to run this tool, you are expected to have `papermill` and `IPython 7.0.0+` installed in your current environment.

Currently, `Data Refresher` supports the following:
1. Posting one or a folder of data files to Datamart.
2. Erase a dataset from Datamart.
3. Save the template for a dataset as a `.tsv` file.
4. Provides visualization for three datasets.

For template annotation requirements, please refer to: https://t2wml-annotation.readthedocs.io/en/latest/

---

Description of the module:
1. Directory `templates` includes all the template files.
2. Directory `utils` contains all the tools needed for the `refresher`.
3. Other directories: One folder includes the all the spreadsheets for one dataset.

---
## Usage
Users are encouraged to follow the examples in `Data Refresher Demo.ipynb`, where they can enter their arguments in the notebook and execute. The following would be taken care of by the module automatically. Or they can execute the notebook `DataLoader.ipynb` from commandline, or feed it with a `.yaml` file.

### Parameters
The following are `required` parameters:
1. `datamart_api_url`: url of the Datamart server
2. `template_path`: location of the template.tsv file

The following is `required` is you want to append data to Datamart:
`dataset_path`: A filename or path where the datafiles are stored. Currently only supports *.csv and *.xlsx file.

The following parameters are `optional`:
1. `dataset_id`: Can be inferred from template
2. `save_template_path`: Required if you want to save the template
3. `dataset_id_to_erase`: Required if you want to erase the dataset
4. `flag_combine_files`: Whether to combine data across different files before posting them to Datamart. Default value is `False`.
5. `DEBUG`: Debug mode, default is `False`.
