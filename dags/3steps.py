from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

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
    '3 Step Model flow', default_args=default_args, schedule_interval=timedelta(minutes=10))

volume_mount = VolumeMount('data-volume',
                           mount_path='/',
                           sub_path=None,
                           read_only=False)
volume_config= {
    'persistentVolumeClaim':
    {
        'claimName': 'airflow-dag' # uses the persistentVolumeClaim given in the Kube yaml
    }
}

volume = Volume(name='data-volume', configs=volume_config)

step1 = KubernetesPodOperator(namespace='airflow',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="passing-test",
                          volumes=[volume],
                          volume_mounts=[volume_mount]
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag,
                          in_cluster=True
                          )

step2 = KubernetesPodOperator(namespace='airflow',
                          image="ubuntu:16.04",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="fail",
                          task_id="failing-task",
                          get_logs=True,
                          dag=dag,
                          in_cluster=True
                          )

step3 = KubernetesPodOperator(namespace='airflow',
                          image="ubuntu:16.04",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="fail",
                          task_id="failing-task",
                          get_logs=True,
                          dag=dag,
                          in_cluster=True
                          )

step2.set_upstream(step1)
step3.set_upstream(step2)

