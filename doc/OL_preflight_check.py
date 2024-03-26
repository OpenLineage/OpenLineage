import os
import re
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow import __version__ as airflow_version
from airflow.utils.trigger_rule import TriggerRule
from distutils.version import StrictVersion
from importlib.metadata import version, PackageNotFoundError

BYPASS_LATEST_VERSION_CHECK = False  # set this to True to bypass the latest version check for openlineage-airflow library and OpenLineage provider package. Version check will be skipped if unable to access PyPI URL

# Dictionary mapping backend names to their corresponding verification functions
BACKENDS = {
    "MARQUEZ": "verify_marquez_http_backend",
    "ATLAN": "verify_atlan_http_backend",
    "CUSTOM": "verify_custom_http_backend"
}
LINEAGE_BACKEND = "MARQUEZ"  # update this if using any other backend for OpenLineage events ingestion and implement custom checks in verify_custom_http_backend function

log = logging.getLogger(__name__)

def get_backend_verification_function(backend_name):
    """
    Returns the appropriate backend verification function based on the backend_name.
    If the backend_name is not recognized, returns None.
    """
    return BACKENDS.get(backend_name)

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


def is_ol_enabled(ol_type):
    """
    This function is used to check if OpenLineage is disabled for provider package or library.
    :param ol_type:
    """
    if ol_type == 'ol_library':
        from openlineage.airflow.plugin import _is_disabled
        log.info("is_disabled: " + str(_is_disabled()))
        if _is_disabled():
            if os.getenv("OPENLINEAGE_DISABLED", "false").lower() == "true":
                raise Exception("OpenLineage is disabled due to the environment variable OPENLINEAGE_DISABLED")
            else:
                raise Exception(
                    "OpenLineage is disabled because required config/env variables are not set. Please refer to https://openlineage.io/docs/client/python")
    elif ol_type == 'provider_package':
        from airflow.providers.openlineage.plugins.openlineage import _is_disabled
        from airflow.configuration import conf
        if _is_disabled():
            if conf.getboolean("openlineage", "disabled", fallback=False):
                raise Exception("OpenLineage is disabled in airflow.cfg: openlineage.disabled")
            elif os.getenv("OPENLINEAGE_DISABLED", "false").lower() == "true":
                raise Exception("OpenLineage is disabled due to the environment variable OPENLINEAGE_DISABLED")
            else:
                raise Exception(
                    "OpenLineage is disabled because required config/env variables are not set. Please refer to https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html")

def is_provider_or_library():
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
        return 'check_ol_library_transport'
    else:
        return 'check_provider_package_transport'


def get_config_type_branch(transport_kind):
    """
    This function is used to return the branch based on the transport kind.
    :param transport_kind:
    """
    if transport_kind is 'http':
        return 'extract_http_config'
    elif transport_kind is 'kafka':
        return 'extract_kafka_config'
    elif transport_kind is 'file':
        return 'extract_file_config'
    elif transport_kind is 'console':
        return 'extract_console_config'
    elif transport_kind is 'some_custom_transport': #this is a placeholder for any custom transport that user wants to add
        return 'extract_custom_config'
    else:
        raise Exception("Unrecognized transport type: ", transport_kind)


def check_ol_library_transport(**kwargs):
    """
    This function is used to check the OL configuration when using openlineage-airflow library.
    This function also ensures that the openlineage-airflow library is installed and up-to-date.
    pushes the ol_type to xcom for other tasks to refer.
    returns the branch based on the config type.
    :param kwargs:
    """
    log.info("Airflow version is >=2.1 and <2.7, checking openlineage-airflow library configuration")
    library_name = "openlineage-airflow"
    check_library_version(library_name, get_latest_version_pypi(library_name))
    is_ol_enabled(ol_type='ol_library')
    from openlineage.airflow.listener import adapter
    try:
        transport = adapter.client.transport
        transport_kind = transport.kind
        log.debug(f"OpenLineage transport kind: {transport_kind}")
    except Exception as e:
        raise Exception("Unable to parse OL transport kind due to an exception - ", e)

    kwargs['ti'].xcom_push(key='ol_type', value='ol_library')
    return get_config_type_branch(transport_kind)


def check_provider_package_transport(**kwargs):
    """
    This function is used to check the OpenLineage configuration when using provider package.
    This function also ensures that the OpenLineage provider package is installed and up-to-date.
    pushes the ol_type to xcom for other tasks to refer.
    returns the branch based on the config type.
    :param kwargs:
    """
    log.info("Airflow version is >=2.7.0, checking OpenLineage provider package configuration")
    library_name = "apache-airflow-providers-openlineage"
    check_library_version(library_name, get_latest_version_pypi(library_name))
    is_ol_enabled(ol_type='provider_package')

    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
    try:
        transport = get_openlineage_listener().adapter.get_or_create_openlineage_client().transport
        transport_kind = transport.kind
        log.info(f"OpenLineage transport kind: {transport_kind}")
    except Exception as e:
        raise Exception(
            "Unable to parse OL transport kind due to an exception - ", e)
    kwargs['ti'].xcom_push(key='ol_type', value='provider_package')
    return get_config_type_branch(transport_kind)


def get_ol_type(**kwargs):
    """
    This function is used to get the ol_type from xcom.
    :param kwargs:
    """
    ol_type = kwargs['ti'].xcom_pull(key='ol_type', task_ids='check_ol_library_transport')
    if ol_type is None:
        ol_type = kwargs['ti'].xcom_pull(key='ol_type', task_ids='check_provider_package_transport')
    log.debug(f"ol_type: {ol_type}")
    return ol_type


def serialize_http_config(http_config):
    """
    This function is used to extract the config from the HttpConfig object.
    :param http_config:
    """
    extracted_config = {"url": http_config.url, "endpoint": http_config.endpoint, "timeout": http_config.timeout, "verify": http_config.verify, "auth_exists": (http_config.auth is not None), "auth": "extract when required, it is not safe to pass in xcom", "session": http_config.session, "adapter": http_config.adapter}
    return extracted_config


def extract_http_config(**kwargs):
    """
    This function is used to check the HttpConfig for OpenLineage events ingestion.
    extracts and pushes the config to xcom for other tasks to refer.
    returns the branch based on the backend type. (marquez or other)
    :param kwargs:
    """
    log.info("Config type is HttpConfig")
    ol_type = get_ol_type(**kwargs)
    if ol_type == 'ol_library':
        from openlineage.airflow.listener import adapter
        http_config = adapter.client.transport.config
    elif ol_type == 'provider_package':
        from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
        http_config = get_openlineage_listener().adapter.get_or_create_openlineage_client().transport.config
        log.debug(f"ol transport config: {http_config}")
    extracted_config = serialize_http_config(http_config)
    kwargs['ti'].xcom_push(key='config', value=extracted_config)
    return get_backend_verification_function(LINEAGE_BACKEND)


def extract_kafka_config():
    log.info("Config type is KafkaConfig - TO BE IMPLEMENTED")
    raise NotImplementedError(
        "This feature is not implemented yet - Requesting Community to contribute to this feature.")


def extract_file_config():
    log.info("Config type is FileConfig - TO BE IMPLEMENTED")
    raise NotImplementedError(
        "This feature is not implemented yet - Requesting Community to contribute to this feature.")


def extract_console_config():
    log.info("Config type is ConsoleConfig - TO BE IMPLEMENTED")
    raise NotImplementedError(
        "This feature is not implemented yet - Requesting Community to contribute to this feature.")


def extract_custom_config():
    """This is a placeholder for any custom transport that user wants to add."""
    log.info("This is a placeholder for any custom transport that user wants to add - TO BE IMPLEMENTED")
    raise NotImplementedError(
        "This feature is not implemented yet - Requesting Community to contribute to this feature.")


def verify_marquez_http_backend(**kwargs):
    """
    This function is used to check the Marquez setup.
    Also checks if Airflow is able to make API calls to the specified lineage backend server.
    :param kwargs:
    """
    log.info("Checking Marquez setup")
    config = kwargs['ti'].xcom_pull(key='config', task_ids='extract_http_config')
    ol_url = config['url']
    ol_endpoint = config['endpoint'] # "api/v1/lineage"
    marquez_prefix_path = ol_endpoint[:ol_endpoint.rfind('/')+1] # "api/v1/"
    list_namespace_url = ol_url + "/" + marquez_prefix_path + "namespaces"
    import requests
    try:
        response = requests.get(list_namespace_url)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to connect to Marquez at {ol_url}/{ol_endpoint}/namespaces", e)
    log.info("Airflow is able to access the URL")


def verify_atlan_http_backend():
    log.info("Checking Atlan server connection - TO BE IMPLEMENTED")
    raise NotImplementedError("This feature is not implemented yet")


def verify_custom_http_backend():
    """This is a placeholder for any custom http backend that user wants to test."""
    log.info("Checking custom server connection - TO BE IMPLEMENTED")
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


def get_library_version(library_name):
    """Function to get the installed version of a library."""
    try:
        return StrictVersion(version(library_name))
    except PackageNotFoundError:
        return None


def check_library_version(library_name, latest_version):
    """
    Checks if the given library is installed with the specified version.
    Raises an exception if the library is not installed or not up-to-date.
    """
    if BYPASS_LATEST_VERSION_CHECK:
        log.info(f"Bypassing the latest version check for {library_name}")
        return
    if latest_version is None:
        log.warn(f"Failed to fetch the latest version for {library_name}. Skipping version check.")
        return
    library_version = get_library_version(library_name)
    if library_version is None:
        raise Exception(f"{library_name} is not installed.")
    elif library_version < StrictVersion(latest_version):
        raise Exception(f"{library_name} is out of date. Installed version: {library_version}, Required version: {latest_version}")


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

is_provider_or_library = BranchPythonOperator(
    task_id='is_provider_or_library',
    python_callable=is_provider_or_library,
    dag=dag,
)

check_ol_library_transport = BranchPythonOperator(
    task_id='check_ol_library_transport',
    python_callable=check_ol_library_transport,
    provide_context=True,
    dag=dag,
)

check_provider_package_transport = BranchPythonOperator(
    task_id='check_provider_package_transport',
    python_callable=check_provider_package_transport,
    provide_context=True,
    dag=dag,
)

extract_http_config = BranchPythonOperator(
    task_id='extract_http_config',
    python_callable=extract_http_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    provide_context=True,
    dag=dag,
)

extract_kafka_config = PythonOperator(
    task_id='extract_kafka_config',
    python_callable=extract_kafka_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

extract_file_config = PythonOperator(
    task_id='extract_file_config',
    python_callable=extract_file_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

extract_console_config = PythonOperator(
    task_id='extract_console_config',
    python_callable=extract_console_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

extract_custom_config = PythonOperator( # this is a placeholder for any custom transport that user wants to add
    task_id='extract_custom_config',
    python_callable=extract_custom_config,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

verify_marquez_http_backend = PythonOperator(
    task_id='verify_marquez_http_backend',
    python_callable=verify_marquez_http_backend,
    provide_context=True,
    dag=dag,
)

verify_custom_http_backend = PythonOperator(
    task_id='verify_custom_http_backend',
    python_callable=verify_custom_http_backend,
    dag=dag,
)

verify_atlan_http_backend = PythonOperator(
    task_id='verify_atlan_http_backend',
    python_callable=verify_atlan_http_backend,
    dag=dag,
)

all_checks_passed = PythonOperator(
    task_id='all_checks_passed',
    python_callable=lambda: print("All checks passed!"),
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

is_provider_or_library >> [check_ol_library_transport, check_provider_package_transport]
check_ol_library_transport >> [extract_http_config, extract_kafka_config, extract_file_config, extract_console_config, extract_custom_config]
check_provider_package_transport >> [extract_http_config, extract_kafka_config, extract_file_config, extract_console_config, extract_custom_config]
extract_http_config >> [verify_marquez_http_backend, verify_custom_http_backend, verify_atlan_http_backend]
verify_marquez_http_backend >> all_checks_passed
verify_custom_http_backend >> all_checks_passed
verify_atlan_http_backend >> all_checks_passed
