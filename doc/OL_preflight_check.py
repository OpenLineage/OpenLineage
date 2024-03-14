import os
import re
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow import __version__ as airflow_version
from airflow.utils.trigger_rule import TriggerRule
from distutils.version import StrictVersion

LINEAGE_BACKEND = "MARQUEZ"  # update this if using any other backend for OpenLineage events ingestion and implement custom checks in other_server_check function
BYPASS_LATEST_VERSION_CHECK = False  # set this to True to bypass the latest version check for openlineage-airflow library and OpenLineage provider package
MARQUEZ_PATH_PREFIX = "/api/v1/"

log = logging.getLogger(__name__)

def parse_prefix(version_string):
    """
    Parse the version string and return the prefix.
    This function is used to extract the base version (major.minor.patch) from a
    version string possibly containing additional metadata.
    :param version_string: str, version string (e.g., '2.7.0+astro1.2')
    :return: str or None, base version (e.g., '2.7.0') if match found, otherwise None
    """
    log.debug(f"Airflow Version string: {version_string} ")
    pattern = r'^(\d+\.\d+\.\d+)'
    match = re.match(pattern, version_string)
    return match.group(1) if match else None


def check_ol_enabled():
    """
    This function is used to check if OpenLineage is disabled using environment variables.
    """
    if os.environ.get('AIRFLOW__OPENLINEAGE__DISABLED', 'false').lower() == 'true':
        raise Exception("OpenLineage is disabled due to the environment variable AIRFLOW__OPENLINEAGE__DISABLED")
    elif os.environ.get('OPENLINEAGE_DISABLED', 'false').lower() == 'true':
        raise Exception("OpenLineage is disabled due to the environment variable OPENLINEAGE_DISABLED")
    return


def check_airflow_version():
    """
    This function is used to check the airflow version and return the branch based on the version.
    <2.1: airflow_version_unsupported
    >=2.1 and <2.7: uses openlineage-airflow library
    >=2.7: uses provider package
    returns the branch based on the version.
    """
    try:
        version = StrictVersion(parse_prefix(airflow_version))
    except ValueError:
        raise ValueError("Failed to parse airflow version")
    if version < StrictVersion('2.1'):
        raise Exception('OpenLineage is not supported in Airflow versions <2.1')
    elif version < StrictVersion('2.7'):
        return 'OL_library'
    else:
        return 'provider_package'

def OL_library_config(**kwargs):
    """
    This function is used to check the OL configuration when using openlineage-airflow library.
    This function also ensures that the openlineage-airflow library is installed and up-to-date.
    pushes the OL_type to xcom for other tasks to refer.
    returns the branch based on the config type.
    :param kwargs:
    """
    log.info("Airflow version is >=2.1 and <2.7, checking openlineage-airflow library configuration")
    library_name = "openlineage-airflow"
    check_library_version(library_name, get_latest_version_pypi(library_name))
    from openlineage.airflow.adapter import OpenLineageAdapter
    from openlineage.client.transport.http import HttpConfig
    from openlineage.client.transport.kafka import KafkaConfig
    from openlineage.client.transport.file import FileConfig
    from openlineage.client.transport.console import ConsoleConfig
    try:
        config = OpenLineageAdapter().client.transport.config
        config_type = type(config)
        log.debug(f"OpenLineage config: {config}")
        kwargs['ti'].xcom_push(key='OL_type', value='OL_library')
    except Exception as e:
        raise Exception(
            "OpenLineage config is empty, please setup OpenLineage using https://openlineage.io/docs/client/python - ",
            e)

    if config_type is HttpConfig:
        return 'http_config'
    elif config_type is KafkaConfig:
        return 'kafka_config'
    elif config_type is FileConfig:
        return 'file_config'
    elif config_type is ConsoleConfig:
        return 'console_config'


def provider_package_config(**kwargs):
    """
    This function is used to check the OpenLineage configuration when using provider package.
    This function also ensures that the OpenLineage provider package is installed and up-to-date.
    pushes the OL_type to xcom for other tasks to refer.
    returns the branch based on the config type.
    :param kwargs:
    """
    log.info("Airflow version is >=2.7.0, checking OpenLineage provider package configuration")
    library_name = "apache-airflow-providers-openlineage"
    check_library_version(library_name, get_latest_version_pypi(library_name))

    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
    config = get_openlineage_listener().adapter.get_openlineage_config()
    log.debug(f"Config: {config}")
    if config is None:
        raise Exception(
            "OpenLineage provider package config is empty, please setup OpenLineage using https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html")

    config_type = config['type']
    kwargs['ti'].xcom_push(key='OL_type', value='provider_package')
    if config_type == 'http':
        return 'http_config'
    elif config_type == 'kafka':
        return 'kafka_config'
    elif config_type == 'file':
        return 'file_config'
    elif config_type == 'console':
        return 'console_config'


def http_config(**kwargs):
    """
    This function is used to check the HttpConfig for OpenLineage events ingestion.
    extracts and pushes the url to xcom for other tasks to refer.
    returns the branch based on the backend type. (marquez or other)
    :param kwargs:
    """
    log.info("Config type is HttpConfig")
    OL_type = kwargs['ti'].xcom_pull(key='OL_type', task_ids='OL_library')
    if OL_type is None:
        OL_type = kwargs['ti'].xcom_pull(key='OL_type', task_ids='provider_package')
    log.debug(f"OL_type: {OL_type}")

    if OL_type == 'OL_library':
        from openlineage.airflow.adapter import OpenLineageAdapter
        config = OpenLineageAdapter().client.transport.config
        url = config.url
    elif OL_type == 'provider_package':
        from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
        config = get_openlineage_listener().adapter.get_openlineage_config()
        url = config['url']
    log.info(f"OL_url: {url}")
    kwargs['ti'].xcom_push(key='url', value=url)
    if LINEAGE_BACKEND.lower() == "marquez":
        return 'marquez_check'
    else:
        return 'other_server_check'


def kafka_config():
    log.info("Config type is KafkaConfig - TO BE IMPLEMENTED")
    raise NotImplementedError(
        "This feature is not implemented yet - Requesting Community to contribute to this feature.")


def file_config():
    log.info("Config type is FileConfig - TO BE IMPLEMENTED")
    raise NotImplementedError(
        "This feature is not implemented yet - Requesting Community to contribute to this feature.")


def console_config():
    log.info("Config type is ConsoleConfig - TO BE IMPLEMENTED")
    raise NotImplementedError(
        "This feature is not implemented yet - Requesting Community to contribute to this feature.")


def marquez_check(**kwargs):
    """
    This function is used to check the Marquez setup.
    It checks if the namespace exists in Marquez.
    Also checks if Airflow is able to make API calls to the specified lineage backend server.
    :param kwargs:
    """
    log.info("Checking Marquez setup")
    url = kwargs['ti'].xcom_pull(key='url', task_ids='http_config')
    list_namespace_url = url + MARQUEZ_PATH_PREFIX + "namespaces"
    import requests
    try:
        response = requests.get(list_namespace_url)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to connect to Marquez at {url}", e)
    log.info("Airflow is able to access the URL")
    # extract list of all names from list of namespaces
    namespaces = [namespace['name'] for namespace in response.json()['namespaces']]
    log.debug(f"All namespaces: {namespaces}")

    OL_type = kwargs['ti'].xcom_pull(key='OL_type', task_ids='OL_library')
    if OL_type is None:
        OL_type = kwargs['ti'].xcom_pull(key='OL_type', task_ids='provider_package')
    log.debug(f"OL_type: {OL_type}")

    if OL_type == 'OL_library':
        _DAG_DEFAULT_NAMESPACE = "default"
        _DAG_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", os.getenv("MARQUEZ_NAMESPACE", _DAG_DEFAULT_NAMESPACE))
    elif OL_type == 'provider_package':
        from airflow.configuration import conf
        _DAG_DEFAULT_NAMESPACE = "default"
        _DAG_NAMESPACE = conf.get("openlineage", "namespace",fallback=os.getenv("OPENLINEAGE_NAMESPACE", _DAG_DEFAULT_NAMESPACE))
    if _DAG_NAMESPACE in namespaces:
        log.info(f"Namespace {_DAG_NAMESPACE} exists in Marquez")
    else:
        raise Exception(f"Namespace {_DAG_NAMESPACE} does not exist, please create it or use existing namespace")


def other_server_check():
    log.info("Checking other server setup - TO BE IMPLEMENTED")
    raise NotImplementedError("This feature is not implemented yet")
    # Please implement this if you are using any other server for OpenLineage events ingestion. Default is Marquez.
    # points to check
    # 1. extract API key(if applicable)
    # 2. check if namespace exists on server (if applicable)
    # 3. check if Airflow is able to make API calls to the specified lineage backend server.


def get_latest_version_pypi(library_name):
    """Function to fetch the latest version of a library from PyPI."""
    try:
        import requests
        response = requests.get(f"https://pypi.org/pypi/{library_name}/json")
        response.raise_for_status()
        return response.json()['info']['version']
    except requests.RequestException as e:
        log.error(f"Failed to fetch latest version for {library_name} from PyPI: {e}")
        return None


def check_library_version(library_name, latest_version):
    """
    Checks if the given library is installed with the specified version.
    Raises an exception if the library is not installed or not up-to-date.
    """
    import subprocess
    import sys
    try:
        installed_libs = subprocess.run([sys.executable, '-m', 'pip', 'freeze'], capture_output=True, text=True)
        installed_libs = installed_libs.stdout
        lib_version_match = re.search(f"{library_name}==(.*)", installed_libs)

        if lib_version_match:
            if BYPASS_LATEST_VERSION_CHECK:
                return True
            installed_version = lib_version_match.group(1)
            if installed_version == latest_version:
                return True
            else:
                raise Exception(
                    f"{library_name} is out of date. Installed version: {installed_version}, Required version: {latest_version}")
        else:
            raise Exception(f"{library_name} is not installed.")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to check installed libraries. Error: {e.output}")


default_args = {
    'owner': 'community',
    'start_date': days_ago(1),
    'retries': 0,
}

dag = DAG(
    dag_id='OpenLineage_setup_check',
    default_args=default_args,
    description='A DAG to check OpenLineage setup and configurations',
    schedule_interval=None,
)

check_ol_enabled_task = PythonOperator(
    task_id='check_ol_enabled',
    python_callable=check_ol_enabled,
    dag=dag,
)

check_airflow_version_task = BranchPythonOperator(
    task_id='check_airflow_version',
    python_callable=check_airflow_version,
    dag=dag,
)

OL_library = BranchPythonOperator(
    task_id='OL_library',
    python_callable=OL_library_config,
    provide_context=True,
    dag=dag,
)

provider_package = BranchPythonOperator(
    task_id='provider_package',
    python_callable=provider_package_config,
    provide_context=True,
    dag=dag,
)

http_config = BranchPythonOperator(
    task_id='http_config',
    python_callable=http_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    provide_context=True,
    dag=dag,
)

kafka_config = PythonOperator(
    task_id='kafka_config',
    python_callable=kafka_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

file_config = PythonOperator(
    task_id='file_config',
    python_callable=file_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

console_config = PythonOperator(
    task_id='console_config',
    python_callable=console_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

marquez_check = PythonOperator(
    task_id='marquez_check',
    python_callable=marquez_check,
    provide_context=True,
    dag=dag,
)

other_server_check = PythonOperator(
    task_id='other_server_check',
    python_callable=other_server_check,
    dag=dag,
)

all_checks_passed = PythonOperator(
    task_id='all_checks_passed',
    python_callable=lambda: print("All checks passed!"),
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

check_ol_enabled_task >> check_airflow_version_task
check_airflow_version_task >> [OL_library, provider_package]
OL_library >> [http_config, kafka_config, file_config, console_config]
provider_package >> [http_config, kafka_config, file_config, console_config]
http_config >> [marquez_check, other_server_check]
marquez_check >> all_checks_passed
other_server_check >> all_checks_passed

