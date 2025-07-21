%pip install --upgrade databricks-sdk==0.49.0
%restart_pythons

from databricks.sdk.service.jobs import JobSettings as Job


Bronze_Ingestion = Job.from_dict(
    {
        "name": "Bronze_Ingestion",
        "tasks": [
            {
                "task_key": "Parameters",
                "notebook_task": {
                    "notebook_path": "/Workspace/Flights/Src_Parameters",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "Incremental_Ingestion",
                "depends_on": [
                    {
                        "task_key": "Parameters",
                    },
                ],
                "for_each_task": {
                    "inputs": "{{tasks.Parameters.values.output_key}}",
                    "task": {
                        "task_key": "Incremental_Ingestion_iteration",
                        "notebook_task": {
                            "notebook_path": "/Workspace/Flights/Bronze_Layer",
                            "base_parameters": {
                                "src": "{{input.src}}",
                            },
                            "source": "WORKSPACE",
                        },
                    },
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "performance_target": "STANDARD",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Bronze_Ingestion, job_id=1024011572176614)
# or create a new job using: w.jobs.create(**Bronze_Ingestion.as_shallow_dict())
