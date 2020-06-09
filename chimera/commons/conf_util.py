from __future__ import absolute_import
from builtins import object
import re
import yaml
import os
import json

from collections import OrderedDict

# have yaml parse regular expressions
yaml.SafeLoader.add_constructor(
    u"tag:yaml.org,2002:python/regexp", lambda l, n: re.compile(l.construct_scalar(n))
)


class YamlConfEncoder(json.JSONEncoder):
    """Custom encoder for YamlConf."""

    def default(self, obj):
        if isinstance(obj, type(re.compile(r""))):
            return obj.pattern
        return super(YamlConfEncoder, self).default(obj)


class YamlConfError(Exception):
    """Exception class for YamlConf class."""

    pass


class YamlConf(object):
    """YAML configuration class."""

    def __init__(self, file):
        """Construct YamlConf instance."""

        self._file = file
        with open(self._file) as f:
            self._cfg = yaml.safe_load(f)

    @property
    def file(self):
        return self._file

    @property
    def cfg(self):
        return self._cfg

    def get(self, key):
        try:
            return self._cfg[key]
        except KeyError:
            raise YamlConfError

    def __repr__(self):
        return json.dumps(self._cfg, cls=YamlConfEncoder, indent=2)


class JobContext(object):
    """Job context class."""

    def __init__(self, file):
        """Construct JobContext instance."""
        self._file = file
        with open(self._file) as f:
            self._ctx = json.load(f)

    @property
    def file(self):
        return self._file

    @property
    def ctx(self):
        return self._ctx

    def get(self, key):
        try:
            return self._ctx[key]
        except KeyError:
            raise (
                Exception(
                    "Context '{}' doesn't exist in {}.".format(key, self._file)
                )
            )

    def set(self, key, val):
        self._ctx[key] = val

    def save(self):
        with open(self._file, "w") as f:
            json.dump(self._ctx, f, indent=2, sort_keys=True)



class DockerParams(object):
    """Job context class."""

    def __init__(self, file):
        """Construct DockerParams instance."""
        self._file = file
        with open(self._file) as f:
            self._params = json.load(f)

    @property
    def file(self):
        return self._file

    @property
    def params(self):
        return self._params

    def get(self, key):
        try:
            return self._params[key]
        except KeyError:
            raise (
                Exception(
                    "Docker params '{}' doesn't exist in {}.".format(key, self._file)
                )
            )


def load_config(config_filepath):
    # load config file
    config_ext = os.path.splitext(config_filepath)[1]
    if config_ext == ".json":
        try:
            config = json.load(open(config_filepath, 'r'), object_pairs_hook=OrderedDict)
        except Exception as e:
            raise RuntimeError("Could not load Config : {}".format(e))
    elif config_ext == ".yaml":
        try:
            config = YamlConf(config_filepath).cfg
        except Exception as e:
            raise RuntimeError("Could not load Config : {}".format(e))
    else:
        raise RuntimeError("Config file must end in .yaml or .json: {}".format(config_filepath))

    return config