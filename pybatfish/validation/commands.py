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
"""Internal representation for validation commands."""
import json
import logging
import os
from abc import ABCMeta, abstractmethod
from enum import IntEnum
from typing import Any, Dict, List, Optional

import yaml
from six import add_metaclass

from pybatfish.client.session import Session


class CommandExecutionStatus(IntEnum):
    """Enum for command execution status."""
    SUCCESS = 0
    FAILURE = 1
    ERROR = 2


class CommandResult(object):
    """Result from execution of a command."""

    def __init__(self, status, result=None, error=None):
        # type: (CommandExecutionStatus, Optional[Any], Optional[str]) -> None
        self.status = status
        self.result = result
        self.error = error

    def dict(self):
        # type: () -> Dict[str, Any]
        d = {
            'status': self.status
        }
        if self.result is not None:
            d['result'] = self.result
        if self.error is not None:
            d['error'] = self.error
        return d


@add_metaclass(ABCMeta)
class Command(object):
    """Command abstract class."""

    @abstractmethod
    def run(self, session):
        raise NotImplementedError()


class InitSnapshot(Command):
    """Command to initialize a new snapshot."""

    def __init__(self, name, upload, overwrite):
        # type: (str, Optional[str], bool) -> None
        self.name = name
        self.upload = upload
        self.overwrite = overwrite

    def run(self, session):
        # type: (Session) -> CommandResult
        result = session.init_snapshot(self.upload, self.name, self.overwrite)
        return CommandResult(CommandExecutionStatus.SUCCESS, result)


class SetNetwork(Command):
    """Command to set current network."""

    def __init__(self, name):
        # type: (str) -> None
        self.name = name

    def run(self, session):
        # type: (Session) -> CommandResult
        result = session.set_network(self.name)
        return CommandResult(CommandExecutionStatus.SUCCESS, result)


class ShowFacts(Command):
    """Command to show facts about a network."""

    def __init__(self, nodes=None, dir_out=None):
        # type: (str) -> None
        self.nodes = nodes
        self.dir_out = dir_out

    def run(self, session):
        # type: (Session) -> CommandResult
        logger = logging.getLogger(__name__)
        # TODO merge these using something like
        # if nodes:
        #     args['nodes'] = nodes
        # func(**args)
        if self.nodes:
            node_properties = session.q.nodeProperties(
                nodes=self.nodes).answer()
            interface_properties = session.q.interfaceProperties(
                nodes=self.nodes).answer()
        else:
            node_properties = session.q.nodeProperties().answer()
            interface_properties = session.q.interfaceProperties().answer()

        facts = self._process_facts(node_properties, interface_properties)

        if self.dir_out:
            for node in facts:
                # TODO do this the right way
                # Should dump to YAML directly instead of going thru JSON first
                from pybatfish.util import BfJsonEncoder
                # y_json = json.loads(json.dumps(facts[node]))
                y_json = json.loads(BfJsonEncoder().encode(facts[node]))
                y = yaml.safe_dump(y_json, default_flow_style=False)

                filepath = os.path.join(self.dir_out, '{}.yml'.format(node))
                logger.debug('Writing node: {} at: {}'.format(node, filepath))
                with open(filepath, 'w') as f:
                    f.write(y)
            logger.info('Wrote facts to directory: {}'.format(self.dir_out))

        return CommandResult(CommandExecutionStatus.SUCCESS, facts)

    @classmethod
    def _process_facts(cls, node_props, iface_props):
        """Process node and interface properties."""
        out = {}
        iface_dict = iface_props.frame().to_dict(orient='records')
        for record in node_props.frame().to_dict(orient='records'):
            # node = record.pop('Node')
            node = record.get('Node')

            # TODO Do better job of matching instead of O(m*n)
            ifaces = []
            for i in iface_dict:
                if i['Interface'].hostname == node:
                    # if i.pop('Interface').hostname == node:
                    ifaces.append(i)
            record['Interfaces'] = ifaces

            # Add properties as children of node
            out[node] = {'facts': record}
        return out


class CommandList(object):
    """Internal representation of a collection of commands."""

    def __init__(self, name, cmds):
        # type: (str, List[Command]) -> None
        self.name = name
        self.cmds = cmds

    def run(self, session):
        """Run the commands in this CommandList."""
        return [self._run_command(cmd, session) for cmd in self.cmds]

    @classmethod
    def _run_command(cls, cmd, session):
        """Run the specified command."""
        try:
            return cmd.run(session)
        except Exception as e:
            # TODO stop re-raising error
            raise e
            err = '{}: {}'.format(e.__class__.__name__, e)
            logger = logging.getLogger(__name__)
            logger.error(err)
            return CommandResult(
                CommandExecutionStatus.ERROR,
                error=err)
