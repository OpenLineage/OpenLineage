import logging
from typing import Optional, List
import json
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.run import Dataset, RunEvent, RunState, Run, Job
from openlineage.client.client import OpenLineageClient
import uuid
from openlineage.client.facet import DocumentationJobFacet, SourceCodeJobFacet

from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook

log = logging.getLogger(__name__)


def get_s3_bucket(path):
    return path.replace("s3://", "").split('/')[0]


class SageMakerProcessingExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SageMakerProcessingOperator']

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=self._get_s3_input_datasets(task_instance),
            outputs=self._get_s3_output_datasets(task_instance),
        )

    def extract(self) -> Optional[TaskMetadata]:
        return None

    def _get_s3_input_datasets(self, task_instance):
        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)

        inputs = []
        for processing_input in xcom_values['Processing']['ProcessingInputs']:
            inputs.append(
                Dataset(
                    namespace="s3://{}".format(
                        get_s3_bucket(processing_input['S3Input']['S3Uri'])
                    ),
                    name=processing_input['S3Input']['S3Uri'],
                    facets={}
                ),
            )

        return inputs

    def _get_s3_output_datasets(self, task_instance):
        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)

        outputs = []
        for processing_output in xcom_values['Processing']['ProcessingOutputConfig']['Outputs']:
            outputs.append(
                Dataset(
                    namespace="s3://{}".format(
                        get_s3_bucket(processing_output['S3Output']['S3Uri'])
                    ),
                    name=processing_output['S3Output']['S3Uri'],
                    facets={}
                ),
            )

        return outputs


class SageMakerTransformExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SageMakerTransformOperator']

    def extract(self) -> Optional[TaskMetadata]:
        return None

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        log.debug(f"extract_on_complete({task_instance})")
        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)
        model_package_arn = xcom_values['Model']['PrimaryContainer']['ModelPackageName']
        transform_input = xcom_values['Transform']['TransformInput']['DataSource']['S3DataSource']['S3Uri']
        transform_output = xcom_values['Transform']['TransformOutput']['S3OutputPath']

        inputs = [
            Dataset(
                namespace="s3://{}".format(
                    get_s3_bucket(transform_input)
                ),
                name=transform_input,
                facets={}
            )
        ]

        model_data_urls = self._get_model_data_urls(model_package_arn)
        for model_data_url in model_data_urls:
            inputs.append(
                Dataset(
                    namespace="s3://{}".format(
                        get_s3_bucket(model_data_url)
                    ),
                    name=model_data_url,
                    facets={}
                )
            )

        output = Dataset(
            namespace="s3://{}".format(get_s3_bucket(transform_output)),
            name=transform_output,
            facets={}
        )

        self._get_sagemaker_lineage(model_package_arn)

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=inputs,
            outputs=[output]
        )

    def _get_model_data_urls(self, model_package_arn):
        sagemaker_hook = SageMakerHook()
        sagemaker_client = sagemaker_hook.get_conn()

        model_data_urls = []
        try:
            model_containers = sagemaker_client.describe_model_package(
                ModelPackageName=model_package_arn
            )['InferenceSpecification']['Containers']

            for container in model_containers:
                model_data_urls.append(container['ModelDataUrl'])
        except:
            pass

        return model_data_urls

    def _get_sagemaker_lineage(self, model_package_arn):
        sagemaker_hook = SageMakerHook()
        sagemaker_client = sagemaker_hook.get_conn()

        model_building_datasets = []
        # try:
        model_artifact_arns = []
        artifact_summaries = sagemaker_client.list_artifacts(SourceUri=model_package_arn)['ArtifactSummaries']

        for summary in artifact_summaries:
            if summary['ArtifactType'] == 'Model':
                model_artifact_arns.append(summary['ArtifactArn'])

        for model_artifact_arn in model_artifact_arns:
            response = sagemaker_client.query_lineage(
                StartArns=[
                    model_artifact_arn,
                ],
                Direction='Ascendants',
                IncludeEdges=False,
                Filters={
                    'Types': [
                        'DataSet',
                        'TrainingJob'
                    ],
                    'LineageTypes': [
                        'TrialComponent',
                        'Artifact'
                    ],
                },
                MaxDepth=10,
                MaxResults=10,
            )

            for vertex in response['Vertices']:
                if vertex['Type'] == 'DataSet':
                    model_building_datasets.append(
                        sagemaker_client.describe_artifact(ArtifactArn=vertex['Arn'])['Source']['SourceUri']
                    )

                if vertex['Type'] == 'TrainingJob':
                    response = sagemaker_client.describe_trial_component(
                        TrialComponentName=vertex['Arn'].split('/')[-1]
                    )
                    training_job_name = response['Source']['SourceArn'].split('/')[-1]

                    training_job = sagemaker_client.describe_training_job(TrainingJobName=training_job_name)

                    start_time = training_job['TrainingStartTime']
                    end_time = training_job['TrainingEndTime']
                    training_job_arn = training_job['TrainingJobArn']
                    hyper_parameters = training_job['HyperParameters']
                    inputs = [i['DataSource']['S3DataSource']['S3Uri'] for i in training_job['InputDataConfig']]
                    output = training_job['ModelArtifacts']['S3ModelArtifacts']

                    self._create_sagemaker_training_job(training_job_name, inputs, output, hyper_parameters,
                                                        start_time, end_time, training_job_arn)

    def _create_sagemaker_training_job(self, training_job_name, data_inputs, model_output, hyper_parameters, start_time,
                                       end_time, job_arn):
        client = OpenLineageClient.from_environment()
        producer = 'https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client'

        # Create a Job object
        job = Job(
            namespace="sagemaker",
            name=training_job_name,
            facets={
                "sourceCode": SourceCodeJobFacet(language='python', source=json.dumps(hyper_parameters)),
                "description": DocumentationJobFacet(
                    description="This job is generated from Sagemaker Lineage & Training Job")
            }
        )

        # Create a Run object with a unique ID
        run = Run(str(uuid.uuid5(uuid.NAMESPACE_DNS, job_arn)))

        inputs = [
            Dataset(
                namespace="s3://{}".format(get_s3_bucket(input_data)),
                name=input_data,
                facets={}
            ) for input_data in data_inputs
        ]

        # Emit a START run event
        client.emit(
            RunEvent(
                eventType=RunState.START,
                eventTime=start_time.isoformat(),
                run=run,
                job=job,
                producer=producer
            )
        )

        # Emit a COMPLETE run event
        client.emit(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=end_time.isoformat(),
                run=run,
                job=job,
                producer=producer,
                inputs=inputs,
                outputs=[Dataset(namespace="s3://{}".format(get_s3_bucket(model_output)), name=model_output, facets={})]
            )
        )
