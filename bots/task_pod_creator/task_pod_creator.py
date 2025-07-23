import os
from typing import Dict

from kubernetes import client, config

# fmt: off

# For creating pods that are for a one-off task, similar to a celery task.
# For now it is used for meeting app sessions and bots.
# This class used to be only for creating pods for bots, that's why there's some bot-specific things in there.
class TaskPodCreator:
    def __init__(self, namespace: str = "attendee"):
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        
        self.v1 = client.CoreV1Api()
        self.namespace = namespace
        
        # Get configuration from environment variables
        self.app_name = os.getenv('CUBER_APP_NAME', 'attendee')
        self.app_version = os.getenv('CUBER_RELEASE_VERSION')
        
        if not self.app_version:
            raise ValueError("CUBER_RELEASE_VERSION environment variable is required")
            
        # Parse instance from version (matches your pattern of {hash}-{timestamp})
        self.app_instance = f"{self.app_name}-{self.app_version.split('-')[-1]}"
        default_pod_image = f"nduncan{self.app_name}/{self.app_name}"
        self.image = f"{os.getenv('BOT_POD_IMAGE', default_pod_image)}:{self.app_version}"

    def create_task_pod(
        self,
        name: str,
        cpu_request: int,
        run_command: str
    ) -> Dict:
        """
        Create a worker pod with configuration from environment.
        
        Args:
            name: Name for the pod
            cpu_request: CPU request for the pod
            run_command: Command to run in the pod
        """

        if cpu_request is None:
            cpu_request = os.getenv("CPU_REQUEST", "4")

        # Run entrypoint script first, then the run command
        command = ["/bin/bash", "-c", f"/opt/bin/entrypoint.sh && {run_command}"]

        # Metadata labels matching the deployment
        labels = {
            "app.kubernetes.io/name": self.app_name,
            "app.kubernetes.io/instance": self.app_instance,
            "app.kubernetes.io/version": self.app_version,
            "app.kubernetes.io/managed-by": "cuber",
            "app": "bot-proc"
        }

        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=self.namespace,
                labels=labels
            ),
            spec=client.V1PodSpec(
                containers=[
                    client.V1Container(
                        name="bot-proc",
                        image=self.image,
                        image_pull_policy="Always",
                        command=command,
                        resources=client.V1ResourceRequirements(
                            requests={
                                "cpu": cpu_request,
                                "memory": os.getenv("BOT_MEMORY_REQUEST", "4Gi"),
                                "ephemeral-storage": os.getenv("BOT_EPHEMERAL_STORAGE_REQUEST", "10Gi")
                            },
                            limits={
                                "memory": os.getenv("BOT_MEMORY_LIMIT", "4Gi"),
                                "ephemeral-storage": os.getenv("BOT_EPHEMERAL_STORAGE_LIMIT", "10Gi")
                            }
                        ),
                        env_from=[
                            # environment variables for the bot
                            client.V1EnvFromSource(
                                config_map_ref=client.V1ConfigMapEnvSource(
                                    name="env"
                                )
                            ),
                            client.V1EnvFromSource(
                                secret_ref=client.V1SecretEnvSource(
                                    name="app-secrets"
                                )
                            )
                        ],
                        env=[]
                    )
                ],
                restart_policy="Never",
                image_pull_secrets=[
                    client.V1LocalObjectReference(
                        name="regcred"
                    )
                ],
                termination_grace_period_seconds=60,
                # Add tolerations to allow pods to be scheduled on nodes with specific taints
                # This can help with scheduling during autoscaling events
                tolerations=[
                    client.V1Toleration(
                        key="node.kubernetes.io/not-ready",
                        operator="Exists",
                        effect="NoExecute",
                        toleration_seconds=900  # Tolerate not-ready nodes for 15 minutes
                    ),
                    client.V1Toleration(
                        key="node.kubernetes.io/unreachable",
                        operator="Exists",
                        effect="NoExecute",
                        toleration_seconds=900  # Tolerate unreachable nodes for 15 minutes
                    )
                ]
            )
        )

        try:
            api_response = self.v1.create_namespaced_pod(
                namespace=self.namespace,
                body=pod
            )
            
            return {
                "name": api_response.metadata.name,
                "status": api_response.status.phase,
                "created": True,
                "image": self.image,
                "app_instance": self.app_instance,
                "app_version": self.app_version
            }
            
        except client.ApiException as e:
            return {
                "name": name,
                "status": "Error",
                "created": False,
                "error": str(e)
            }
