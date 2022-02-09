from openlineage.client.run import RunEvent


class Config:
    @classmethod
    def from_dict(cls, params: dict):
        return cls()


class Transport:
    kind = None
    config = Config

    def emit(self, event: RunEvent):
        raise NotImplementedError()


class TransportFactory:
    def create(self) -> Transport:
        pass
