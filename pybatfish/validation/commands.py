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
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

from six import add_metaclass

from pybatfish.client.session import Session


class CommandExecutionStatus(Enum):
    """Enum for command execution status."""
    SUCCESS = 1
    FAILURE = 2
    ERROR = 3


class CommandResult(object):
    """Result from execution of a command."""

    def __init__(self, status, data):
        # type: (CommandExecutionStatus, Dict[str, Any]) -> None
        self.status = status
        self.data = data

    def to_dict(self):
        # type: () -> Dict[str, Any]
        return {
            'status': self.status,
            'data': self.data,
        }


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
        return CommandResult(
            CommandExecutionStatus.SUCCESS, {
                'result': result
            })


class SetNetwork(Command):
    """Command to set current network."""

    def __init__(self, name):
        # type: (str) -> None
        self.name = name

    def run(self, session):
        # type: (Session) -> CommandResult
        result = session.set_network(self.name)
        return CommandResult(
            CommandExecutionStatus.SUCCESS, {
                'result': result
            })


class ShowFacts(Command):
    """Command to show facts about a network."""

    def __init__(self, nodes=None):
        # type: (str) -> None
        self.nodes = nodes

    def run(self, session):
        # type: (Session) -> CommandResult
        raise NotImplementedError()


class CommandList(object):
    """Internal representation of a collection of commands."""

    def __init__(self, name, cmds):
        # type: (str, List[Command]) -> None
        self.name = name
        self.cmds = cmds

    def run(self, session):
        """Run the commands in this CommandList."""
        return [self.run_command(cmd, session) for cmd in self.cmds]

    def run_command(self, cmd, session):
        """Run the specified command."""
        try:
            return cmd.run(session)
        except Exception as e:
            return CommandResult(
                CommandExecutionStatus.ERROR,
                {
                    'result': '{}: {}'.format(e.__class__.__name__, e)
                }
            )
