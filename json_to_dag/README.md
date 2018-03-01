# json_to_dag

We wanted to create a DAG in airflow in a more generic way.
So we started using a JSON-Template as an input for our DAG creation.

## Usage
The JsonToDag class needs to run in a DAG itself that runs periodically and checks the input folder for new zip-files.
If there is a new zip-file it will be processed and a new DAG will be created

You can find a sample zip in the zip directory
