# coding utf-8
#   Copyright 2018 The Batfish Open Source Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
"""Convert YAML file into validation tasks."""
import logging
from typing import Dict, List, Text

import six
from yaml import safe_load

from pybatfish.validation.commands import (
    Command, InitSnapshot, SetNetwork, ShowFacts,
)

_COMMANDS = 'commands'
# Supported commands
_CMD_SET_NETWORK = 'set_network'
_CMD_INIT_SNAPSHOT = 'init_snapshot'
_CMD_SHOW_FACTS = 'show_facts'

# Modifiers for commands
_MOD_OUTPUT_DIRECTORY = '_output_directory'


def convert_yaml(filename):
    # type: (Text) -> List[Command]
    """Convert specified file into validation commands."""
    logger = logging.getLogger(__name__)

    logger.info('Parsing YAML file: {}'.format(filename))
    with open(filename, 'r') as f:
        yaml_dict = safe_load(f)

    cmds_in = yaml_dict.get(_COMMANDS)
    if not cmds_in:
        raise ValueError(
            'Commands must be specified under top-level key {}'.format(
                _COMMANDS))

    cmds_out = []  # type: List[Command]
    for cmd_dict in cmds_in:
        logger.debug('Command: {}'.format(cmd_dict))
        if len(cmd_dict) != 1:
            raise ValueError(
                'Got malformed command. Expecting single key-value pair '
                'but got: {}'.format(
                    cmd_dict))
        cmd = next(iter(cmd_dict))
        cmd_params = cmd_dict[cmd]

        if cmd == _CMD_SET_NETWORK:
            cmds_out.append(_extract_network(cmd_params))
        elif cmd == _CMD_INIT_SNAPSHOT:
            cmds_out.append(_extract_snapshot(cmd_params))
        elif cmd == _CMD_SHOW_FACTS:
            cmds_out.append(_extract_show_facts(cmd_params))
        else:
            raise ValueError('Got unexpected command: {}'.format(cmd))

    return cmds_out


def _extract_network(name):
    # type: (str) -> SetNetwork
    """Create set network command."""
    if not isinstance(name, six.string_types):
        raise TypeError('{} value must be a string'.format(_CMD_SET_NETWORK))
    return SetNetwork(name)


def _extract_show_facts(dict_):
    # type: (Dict) -> ShowFacts
    """Extract fact-extractions from input dict."""
    if not type(dict_) is dict:
        raise TypeError(
            '{} value must be key-value pairs'.format(_CMD_SHOW_FACTS))
    nodes = dict_.get('nodes', None)
    dir_out = dict_.get(_MOD_OUTPUT_DIRECTORY, None)
    return ShowFacts(nodes, dir_out)


def _extract_snapshot(dict_):
    # type: (Dict) -> InitSnapshot
    """Extract snapshot init from input dict."""
    if not type(dict_) is dict:
        raise TypeError(
            '{} value must be key-value pairs'.format(_CMD_INIT_SNAPSHOT))

    if 'path' not in dict_:
        raise ValueError('Snapshot path must be set via \'path\'')
    path = dict_.get('path')
    overwrite = dict_.get('overwrite', False)
    name = dict_.get('name', None)
    return InitSnapshot(name, path, overwrite)
