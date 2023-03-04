# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example DAG demonstrating simple Apache Airflow operators."""

# [START composer_simple]
from __future__ import print_function

# [START composer_simple_define_dag]
import os
import datetime

from airflow import models
from airflow import configuration

# [END composer_simple_define_dag]
# [START composer_simple_operators]
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreatePythonJobOperator,
)
from airflow.providers.google.cloud.operators.mlengine import (
    MLEngineStartTrainingJobOperator,
)

# [END composer_simple_operators]


# [START composer_simple_define_dag]
default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime.datetime(2020, 1, 1),
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    "composer_twitter_sentiment_train",
    schedule_interval=None,
    default_args=default_dag_args,
) as dag:
    # [END composer_simple_define_dag]
    # [START composer_simple_operators]

    # An instance of an operator is called a task. In this case, the
    # hello_python task calls the "greeting" Python function.
    preprocessing = DataflowCreatePythonJobOperator(
        task_id="preprocessing_tweets",
        py_file="{{ dag_run.conf['work_dir'] }}/code/preprocess.py",
        job_name="twitter_train_preprocess_batch_{{ ds_nodash }}",
        options={
            "temp_location": "{{ dag_run.conf['work_dir'] }}/beam-temp/{{ ds_nodash }}",
            "setup_file": os.path.join(
                configuration.get("core", "dags_folder"), "twitter-sentiment/setup.py"
            ),
            "work-dir": "{{ dag_run.conf['work_dir'] }}",
            "input": "{{ dag_run.conf['work_dir'] }}/input_data/training*.csv",
            "output": "{{ dag_run.conf['work_dir'] }}/output_data/{{ ds_nodash }}/",
            "mode": "train",
        },
        # TODO: change for dynamic parse from requirements.txt
        py_requirements=[
            "apache-beam[gcp]==2.24.0",
            "tensorflow-transform==0.24.1",
            "tensorflow==2.3.0",
            "tfx==0.24.1",
            "nltk==3.5",
            "gensim==3.6.0",
            "fsspec==0.8.4",
            "gcsfs==0.7.1",
        ],
        project_id="ivory-enigma-295118",
        location="europe-west1",
    )

    # Likewise, the goodbye_bash task calls a Bash script.
    training = MLEngineStartTrainingJobOperator(
        task_id="training_model",
        project_id="ivory-enigma-295118",
        job_id="twitter_sentiment_{{ ds_nodash }}_{{ run_id }}",
        package_uris="{{ dag_run.conf['work_dir'] }}/code/trainer-0.0.1.tar.gz",
        training_python_module="trainer.task",
        training_args=[
            "--work-dir",
            "{{ dag_run.conf['work_dir'] }}/output_data/{{ ds_nodash }}/",
            "--epochs",
            "1",
        ],
        region="europe-west1",
        scale_tier="basic_gpu",
        runtime_version="2.1",
        python_version="3.7",
        job_dir="{{ dag_run.conf['work_dir'] }}/temp/trainer",
    )
    # [END composer_simple_operators]

    # [START composer_simple_relationships]
    # Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, hello_python executes before goodbye_bash.
    preprocessing >> training
    # [END composer_simple_relationships]
# [END composer_simple]
