from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount



default_args = {
    'owner': 'prem',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@prem.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    '3_Step_Model_flow', default_args=default_args, schedule_interval=timedelta(minutes=10))

input_volume_mount = VolumeMount('data-volume',
                           mount_path='/input-dataset',
                           sub_path=None,
                           read_only=False)

output_volume_mount = VolumeMount('data-volume',
                           mount_path='/output-dataset',
                           sub_path=None,
                           read_only=False)
volume_config= {
    'persistentVolumeClaim':
    {
        'claimName': 'airflow-dags' # uses the persistentVolumeClaim given in the Kube yaml
    }
}

in_volume = Volume(name='input-dataset', configs=volume_config)
out_volume = Volume(name='output-dataset', configs=volume_config)

step1 = KubernetesPodOperator(namespace='airflow',
                          image="cestum/airflow-demo:prep-input",
                          cmds=[],
                          arguments=["K8S-Airflow"],
                          labels={"foo": "bar"},
                          name="prep-input",
                          volumes=[in_volume],
                          volume_mounts=[input_volume_mount],
                          task_id="prep-input",
                          get_logs=True,
                          dag=dag,
                          in_cluster=True
                          )

step2 = KubernetesPodOperator(namespace='airflow',
                          image="cestum/airflow-demo:model-run",
                          cmds=[],
                          arguments=["3"],
                          labels={"foo": "bar"},
                          name="model-run",
                          volumes=[out_volume],
                          volume_mounts=[output_volume_mount],                         
                          task_id="model-run",
                          get_logs=True,
                          dag=dag,
                          in_cluster=True
                          )


step2.set_upstream(step1)

