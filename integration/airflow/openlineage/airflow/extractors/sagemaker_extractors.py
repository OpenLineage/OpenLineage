# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.run import Dataset

log = logging.getLogger(__name__)


def generate_s3_dataset(path) -> Dataset:
    return Dataset(
        namespace="s3://{}".format(path.replace("s3://", "").split('/')[0]),
        name=path,
        facets={}
    )


class SageMakerProcessingExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SageMakerProcessingOperator', 'SageMakerProcessingOperatorAsync']

    def extract(self) -> Optional[TaskMetadata]:
        pass

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:

        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)

        inputs = []
        outputs = []

        try:
            inputs, outputs = self._get_s3_datasets(
                processing_inputs=xcom_values['Processing']['ProcessingInputs'],
                processing_outputs=xcom_values['Processing']['ProcessingOutputConfig']['Outputs']
            )
        except KeyError as e:
            log.error(f"Could not find input/output information in Xcom. {e}")

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=inputs,
            outputs=outputs,
        )

    @staticmethod
    def _get_s3_datasets(processing_inputs, processing_outputs):

        inputs = []
        outputs = []

        try:
            for processing_input in processing_inputs:
                inputs.append(generate_s3_dataset(processing_input['S3Input']['S3Uri']))
        except Exception as e:
            log.error(f"Cannot find S3 input details. {e}", exc_info=True)

        try:
            for processing_output in processing_outputs:
                outputs.append(generate_s3_dataset(processing_output['S3Output']['S3Uri']))
        except Exception as e:
            log.error(f"Cannot find S3 output details. {e}", exc_info=True)

        return inputs, outputs


class SageMakerTransformExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SageMakerTransformOperator', 'SageMakerTransformOperatorAsync']

    def extract(self) -> Optional[TaskMetadata]:
        pass

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        log.debug(f"extract_on_complete({task_instance})")

        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)

        model_package_arn = None
        transform_input = None
        transform_output = None

        try:
            model_package_arn = xcom_values['Model']['PrimaryContainer']['ModelPackageName']
        except KeyError as e:
            log.error(f"Cannot find Model Package Name in Xcom values. {e}", exc_info=True)

        try:
            transform = xcom_values['Transform']
            transform_input = transform['TransformInput']['DataSource']['S3DataSource']['S3Uri']
            transform_output = transform['TransformOutput']['S3OutputPath']
        except KeyError as e:
            log.error(
                f"Cannot find some required input/output details in XCom. {e}", exc_info=True
            )

        inputs = []

        if transform_input is not None:
            inputs.append(generate_s3_dataset(transform_input))

        if model_package_arn is not None:
            model_data_urls = self._get_model_data_urls(model_package_arn)
            for model_data_url in model_data_urls:
                inputs.append(generate_s3_dataset(model_data_url))

        output = []
        if transform_output is not None:
            output.append(generate_s3_dataset(transform_output))

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=inputs,
            outputs=output
        )

    def _get_model_data_urls(self, model_package_arn):
        model_data_urls = []
        try:
            model_containers = self.operator.hook.get_conn().describe_model_package(
                ModelPackageName=model_package_arn
            )['InferenceSpecification']['Containers']

            for container in model_containers:
                model_data_urls.append(container['ModelDataUrl'])
        except Exception as e:
            log.error(f"Cannot retrieve model details. {e}", exc_info=True)

        return model_data_urls


class SageMakerTrainingExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SageMakerTrainingOperator', 'SageMakerTrainingOperatorAsync']

    def extract(self) -> Optional[TaskMetadata]:
        pass

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        log.debug(f"extract_on_complete({task_instance})")

        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)

        inputs = []
        output = []

        try:
            for input_data in xcom_values['Training']['InputDataConfig']:
                inputs.append(
                    generate_s3_dataset(input_data['DataSource']['S3DataSource']['S3Uri'])
                )
        except KeyError as e:
            log.error(f"Issues extracting inputs. {e}")

        try:
            output.append(
                generate_s3_dataset(xcom_values['Training']['ModelArtifacts']['S3ModelArtifacts'])
            )
        except KeyError as e:
            log.error(f"Issues extracting inputs. {e}")

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=inputs,
            outputs=output
        )
