import inspect
import os
import logging
from typing import Type, Union, Optional

from openlineage.client.transport.transport import Config, Transport, TransportFactory
from openlineage.client.utils import try_import_from_string


log = logging.getLogger(__name__)


try:
    import yaml
except ImportError:
    yaml = False


class DefaultTransportFactory(TransportFactory):
    def __init__(self):
        self.transports = {}

    def register_transport(self, type: str, clazz: Union[Type[Transport], str]):
        self.transports[type] = clazz

    def create(self) -> Optional[Transport]:
        if yaml:
            yml_config = self._try_config_from_yaml()
            if yml_config:
                return self._create_transport(yml_config)
        # Fallback to setting HTTP transport from env variables
        return self._try_http_from_env_config()

    def _create_transport(self, config: dict):
        transport_type = config['type']

        if transport_type in self.transports:
            transport_class = self.transports[transport_type]
        else:
            transport_class = transport_type

        if isinstance(transport_class, str):
            transport_class = try_import_from_string(transport_class)
        if not inspect.isclass(transport_class) or not issubclass(transport_class, Transport):
            raise TypeError(
                f"Transport {transport_class} has to be class, and subclass of Transport"
            )

        config_class = transport_class.config

        if isinstance(config_class, str):
            config_class = try_import_from_string(config_class)
        if not inspect.isclass(config_class) or not issubclass(config_class, Config):
            raise TypeError(f"Config {config_class} has to be class, and subclass of Config")

        return transport_class(config_class.from_dict(config))

    def _try_config_from_yaml(self) -> Optional[dict]:
        file = self._find_yaml()
        if file:
            try:
                with open(file, 'r') as f:
                    config = yaml.safe_load(f)
                    return config['transport']
            except Exception:
                # Just move to read env vars
                pass
        return

    @staticmethod
    def _find_yaml() -> Optional[str]:
        # Check current working directory:
        try:
            cwd = os.getcwd()
            if 'openlineage.yml' in os.listdir(cwd):
                return os.path.join(cwd, 'openlineage.yml')
        except Exception:
            # We can get different errors depending on system
            pass

        # Check $HOME/.openlineage dir
        try:
            path = os.path.expanduser("~/.openlineage")
            if 'openlineage.yml' in os.listdir(path):
                return os.path.join(path, 'openlineage.yml')
        except Exception:
            # We can get different errors depending on system
            pass

    @staticmethod
    def _try_http_from_env_config() -> Optional[Transport]:
        from openlineage.client.transport.http import HttpTransport, HttpConfig
        # backwards compatibility: create Transport from
        # OPENLINEAGE_URL and OPENLINEAGE_API_KEY
        if 'OPENLINEAGE_URL' not in os.environ:
            log.error("Did not find openlineage.yml and OPENLINEAGE_URL is not set")
            return
        config = HttpConfig(
            url=os.environ['OPENLINEAGE_URL'],
            api_key=os.environ.get('OPENLINEAGE_API_KEY', None)
        )
        return HttpTransport(config)
