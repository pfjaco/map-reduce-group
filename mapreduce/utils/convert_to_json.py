"""Convert Pathlib objects to json."""
import json
import pathlib


class PathJSONEncoder(json.JSONEncoder):
    """Class to convert Pathlib objects to json."""

    def default(self, o):
        """Override base class method to include Path object serialization."""
        if isinstance(o, pathlib.Path):
            return str(o)
        return super().default(o)
