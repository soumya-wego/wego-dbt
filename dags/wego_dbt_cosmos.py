"""The ONLY Airflow code anyone writes for the whole dbt project.

Cosmos reads the dbt project and auto-renders the DAG: one task group per
model (run + test), dependencies taken from ref()/source(). Add a model to
the dbt repo -> a new task appears here on the next parse. Nothing below
ever lists models by name.

One file, two environments, switched by env vars the environment provides:

  local Docker (docker-compose sets both):
    WEGO_DBT_ROOT=/opt/airflow/dbt/wego-dbt   (volume mount)
    WEGO_DBT_EXEC_MODE=local                  (dbt baked into the image venv)

  Composer (nothing set -> defaults):
    project read from the GCS-mounted DAGs folder, dbt installed per-task
    in a throwaway virtualenv so the shared environment needs no dbt
    packages. Once dbt-bigquery is added to Composer's PyPI packages,
    flip the default exec mode to "local".
"""

import os
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode

DBT_PROJECT = os.getenv("WEGO_DBT_ROOT", "/home/airflow/gcs/dags/wego-dbt")
EXEC_MODE = os.getenv("WEGO_DBT_EXEC_MODE", "virtualenv")

if EXEC_MODE == "local":
    DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
    # SUBPROCESS: shell out to the venv's dbt binary. The default DBT_RUNNER
    # imports dbt into Airflow's Python, which can't work here — dbt lives in
    # its own venv precisely because the dependency trees conflict.
    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_executable_path=DBT_BIN,
    )
    # Parse via `dbt ls` instead of Cosmos's file walker: the local project
    # mount contains .venv (~15k files) and walking it times out the DagBag
    # import. dbt only reads its own configured paths (models/, tests/).
    render_config = RenderConfig(
        load_method=LoadMode.DBT_LS,
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_executable_path=DBT_BIN,
    )
    operator_args = {}
else:
    # Composer: the bucket copy of the project is clean (no .venv), so the
    # default parser is fine, and dbt isn't installed on the scheduler anyway.
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.VIRTUALENV)
    render_config = RenderConfig()
    operator_args = {
        "py_requirements": ["dbt-bigquery==1.12.1"],
        "py_system_site_packages": False,
        "install_deps": True,
    }

wego_dbt = DbtDag(
    dag_id="wego_dbt",
    project_config=ProjectConfig(DBT_PROJECT),
    profile_config=ProfileConfig(
        profile_name="wego_dbt",
        target_name="dev",  # oauth -> your login locally, service account on Composer
        profiles_yml_filepath=f"{DBT_PROJECT}/profiles.yml",
    ),
    execution_config=execution_config,
    render_config=render_config,
    operator_args=operator_args,
    schedule=None,          # manual trigger while piloting (prod would be e.g. "0 2 * * *")
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"retries": 1},
)
