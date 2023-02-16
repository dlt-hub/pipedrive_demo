
from airflow.operators import PythonOperator, ExternalTaskSensor
from airflow.models import DAG


# make a pipeline method
import dlt
import json
from pipedrive import pipedrive_source, pipedrive_mapping


def load_pipedrive():
    """Constructs a pipeline that will load all pipedrive data"""
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination='bigquery', dataset_name='pipedrive_dataset')
    load_info = pipeline.run(pipedrive_source(fix_custom_fields=True))
    print(load_info)
    print(json.dumps(pipedrive_mapping(), sort_keys=True, indent=4, separators=(',', ': ')))

# make dag and test tasks


args = {
    'owner': 'airflow',
    'start_date': "2023-01-01",
    'max_active_runs': 1,
}

dag = DAG(
    dag_id='DAG_Pipedrive', default_args=args,
    schedule_interval="30 4 * * *")

wait_for_DM_build = ExternalTaskSensor(
    task_id='wait_for_DM_build',
    external_dag_id='ETL_load_DM',
    external_task_id='add_indices',
    execution_delta=None,  # Same day as today
    dag=dag)

load_pipedrive_data = PythonOperator(
    task_id='load_company_pipedrive_data',
    provide_context=True,
    python_callable=load_pipedrive,
    email_on_failure=True,
    email='data@urbansportsclub.com',
    retries=3,
    dag=dag)


test_duplicate_attribution = PythonOperator(
    task_id='test_duplicate_attribution',
    provide_context=True,
    python_callable=test_duplicate_attribution,
    email_on_failure=True,
    email=['data@company.com', 'pipedrive_data_owner@company.com'],
    retries=0,
    dag=dag)

test_orphan_prod_companies = PythonOperator(
    task_id='test_orphan_prod_companies',
    provide_context=True,
    python_callable=test_check_for_orphan_prod_companies,
    email_on_failure=True,
    email=['data@company.com', 'pipedrive_data_owner@company.com'],
    retries=0,
    dag=dag)

test_orphan_pipedrive = PythonOperator(
    task_id='test_incorrect_company_sk_in_pipedrive',
    provide_context=True,
    python_callable=test_check_for_orphan_pipedrive_company_sk,
    email_on_failure=True,
    email=['data@company.com', 'pipedrive_data_owner@company.com'],
    retries=0,
    dag=dag)


wait_for_DM_build.set_downstream(load_pipedrive_data)
load_company_pipedrive_data.set_downstream(test_duplicate_attribution)
load_company_pipedrive_data.set_downstream(test_orphan_prod_companies)
load_company_pipedrive_data.set_downstream(test_orphan_pipedrive)
