

class SourceNotFoundError(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__("data source not found", *args)
