# Orchestrate Great Expectations with Airflow

Integrating Great Expectations (GX) with Apache Airflow for data validation and pipeline orchestration.

Ref: [https://www.astronomer.io/docs/learn/airflow-great-expectations](https://www.astronomer.io/docs/learn/airflow-great-expectations)

## Installation and Setup

1. Install Great Expectations

```
pip install airflow-provider-great-expectations
```

2. Initialize Great Expectations

```
great_expectations init
```

3. Create an **Expectation Suite** in JSON format for validating data in your ```gx/expectations/``` directory. For example:

```json
{
    "data_asset_type": null,
    "expectation_suite_name": "example_suite",
    "expectations": [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "person_id"
            }
        }
    ],
    "ge_cloud_id": null,
    "meta": {
        "great_expectations_version": "0.18.15"
    }
}
```

Additional ```expectation_type```: [https://greatexpectations.io/expectations/](https://greatexpectations.io/expectations/)

## Example Usage in Airflow DAG

To integrate the Great Expectations validation step into your Airflow DAG, you can use the ```GreatExpectationsOperator```. This operator allows you to validate data against defined expectations within your database. 

```python
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
```

```python
validate_data_task = GreatExpectationsOperator(
    task_id='gx_validate_TABLE_NAME',
    conn_id=CONN_ID,
    data_context_root_dir=DATA_CONTEXT_ROOT_DIR,
    schema=SCHEMA_NAME,
    data_asset_name=TABLE_NAME,
    expectation_suite_name='example_suite',
    return_json_dict=True,
    dag=dag,
)
```

### Operator parameters:

- **data_context_root_dir:** Path of the great_expectations directory
- **schema:** The schema within your database where the data asset is located.  Note that instead of using the schema parameter, you can also provide the schema name to the ```data_asset_name``` parameter in the form of ```SCHEMA_NAME.TABLE_NAME```
- **data_asset_name:** The name of the table or dataframe that the default Data Context will load and default Checkpoint will run over
- **expectation_suite_name:** Name of the expectation suite to run if using a default Checkpoint
- **return_json_dict:** If True, returns a json-serializable dictionary instead of a CheckpointResult object

More ```operators```: [https://registry.astronomer.io/providers/airflow-provider-great-expectations/versions/latest/modules/GreatExpectationsOperator](https://registry.astronomer.io/providers/airflow-provider-great-expectations/versions/latest/modules/GreatExpectationsOperator)

## Running the DAG

Once you have defined your DAG with the ```GreateExpectationsOperator```, the DAG will execute the defined validation expectations against the specified data asset as part of your workflow like this:

![Screenshot 2567-06-21 at 13 42 15](https://github.com/hamcheezee/eunomia-data-pipeline/assets/135502061/438a0e19-9030-49c2-b97e-a0283894b66a)

> By default, a Great Expectations task runs validations and raises an AirflowException if any of the tests fail. To override this behavior and continue running the pipeline even if tests fail, set the ```fail_task_on_validation_failure``` flag to **False**.