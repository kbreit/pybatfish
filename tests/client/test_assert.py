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
import pytest
import six
from pandas import DataFrame

from pybatfish.client.asserts import (_get_question_object,
                                      _raise_common,
                                      assert_filter_denies,
                                      assert_filter_permits,
                                      assert_flows_fail,
                                      assert_flows_succeed)
from pybatfish.client.session import Session
from pybatfish.datamodel import HeaderConstraints, PathConstraints
from pybatfish.datamodel.answer import TableAnswer
from pybatfish.exception import (BatfishAssertException,
                                 BatfishAssertWarning, BatfishException)
from pybatfish.question import bfq
from pybatfish.question.question import QuestionBase

if six.PY3:
    from unittest.mock import patch
else:
    from mock import patch


def test_raise_common_default():
    with pytest.raises(BatfishAssertException) as e:
        _raise_common("foobar")
    assert "foobar" in str(e.value)

    with pytest.raises(BatfishAssertException) as e:
        _raise_common("foobaragain", False)
    assert "foobaragain" in str(e.value)


def test_raise_common_warn():
    with pytest.warns(BatfishAssertWarning):
        result = _raise_common("foobar", True)
    assert not result


class MockTableAnswer(TableAnswer):
    def __init__(self, frame_to_use=DataFrame()):
        self._frame = frame_to_use

    def frame(self):
        return self._frame


class MockQuestion(QuestionBase):
    def __init__(self, answer=None):
        self._answer = answer if answer is not None else MockTableAnswer()

    def answer(self, *args, **kwargs):
        return self._answer


def test_filter_permits():
    """Confirm filter-permits assert passes and fails as expected when specifying a session."""
    headers = HeaderConstraints(srcIps='1.1.1.1')
    bf = Session(load_questions=False)
    with patch.object(bf.q, 'searchFilters',
                      create=True) as mock_search_filters:
        # Test success
        mock_search_filters.return_value = MockQuestion()
        assert_filter_permits('filter', headers, session=bf)
        mock_search_filters.assert_called_with(filters='filter',
                                               headers=headers,
                                               action='deny')
        # Test failure; also test that startLocation is passed through
        mock_df = DataFrame.from_records([{'Flow': 'found', 'More': 'data'}])
        mock_search_filters.return_value = MockQuestion(
            MockTableAnswer(mock_df))
        with pytest.raises(BatfishAssertException) as excinfo:
            assert_filter_permits('filter', headers, startLocation='Ethernet1',
                                  session=bf)
        # Ensure found answer is printed
        assert mock_df.to_string() in str(excinfo.value)
        mock_search_filters.assert_called_with(filters='filter',
                                               headers=headers,
                                               startLocation='Ethernet1',
                                               action='deny')


def test_filter_permits_no_session():
    """
    Confirm filter-permits assert passes and fails as expected when not specifying a session.

    For reverse compatibility.
    """
    headers = HeaderConstraints(srcIps='1.1.1.1')
    # Confirm assert works when not specifying a session
    # for reverse compatibility
    with patch.object(bfq, 'searchFilters', create=True) as mock_search_filters:
        # Test success
        mock_search_filters.return_value = MockQuestion()
        assert_filter_permits('filter', headers)
        mock_search_filters.assert_called_with(filters='filter',
                                               headers=headers,
                                               action='deny')
        # Test failure; also test that startLocation is passed through
        mock_df = DataFrame.from_records([{'Flow': 'found', 'More': 'data'}])
        mock_search_filters.return_value = MockQuestion(
            MockTableAnswer(mock_df))
        with pytest.raises(BatfishAssertException) as excinfo:
            assert_filter_permits('filter', headers, startLocation='Ethernet1')
        # Ensure found answer is printed
        assert mock_df.to_string() in str(excinfo.value)
        mock_search_filters.assert_called_with(filters='filter',
                                               headers=headers,
                                               startLocation='Ethernet1',
                                               action='deny')


def test_filter_denies():
    """Confirm filter-denies assert passes and fails as expected when specifying a session."""
    headers = HeaderConstraints(srcIps='1.1.1.1')
    bf = Session(load_questions=False)
    with patch.object(bf.q, 'searchFilters',
                      create=True) as mock_search_filters:
        # Test success
        mock_search_filters.return_value = MockQuestion()
        assert_filter_denies('filter', headers, session=bf)
        mock_search_filters.assert_called_with(filters='filter',
                                               headers=headers,
                                               action='permit')
        # Test failure; also test that startLocation is passed through
        mock_df = DataFrame.from_records([{'Flow': 'found', 'More': 'data'}])
        mock_search_filters.return_value = MockQuestion(
            MockTableAnswer(mock_df))
        with pytest.raises(BatfishAssertException) as excinfo:
            assert_filter_denies('filter', headers, startLocation='Ethernet1',
                                 session=bf)
        # Ensure found answer is printed
        assert mock_df.to_string() in str(excinfo.value)
        mock_search_filters.assert_called_with(filters='filter',
                                               headers=headers,
                                               startLocation='Ethernet1',
                                               action='permit')


def test_filter_denies_no_session():
    """
    Confirm filter-denies assert passes and fails as expected when not specifying a session.

    For reverse compatibility.
    """
    headers = HeaderConstraints(srcIps='1.1.1.1')
    with patch.object(bfq, 'searchFilters', create=True) as mock_search_filters:
        # Test success
        mock_search_filters.return_value = MockQuestion()
        assert_filter_denies('filter', headers)
        mock_search_filters.assert_called_with(filters='filter',
                                               headers=headers,
                                               action='permit')
        # Test failure; also test that startLocation is passed through
        mock_df = DataFrame.from_records([{'Flow': 'found', 'More': 'data'}])
        mock_search_filters.return_value = MockQuestion(
            MockTableAnswer(mock_df))
        with pytest.raises(BatfishAssertException) as excinfo:
            assert_filter_denies('filter', headers, startLocation='Ethernet1')
        # Ensure found answer is printed
        assert mock_df.to_string() in str(excinfo.value)
        mock_search_filters.assert_called_with(filters='filter',
                                               headers=headers,
                                               startLocation='Ethernet1',
                                               action='permit')


def test_flows_fail():
    """Confirm flows-fail assert passes and fails as expected when specifying a session."""
    startLocation = "node1"
    headers = HeaderConstraints(srcIps='1.1.1.1')
    bf = Session(load_questions=False)
    with patch.object(bf.q, 'reachability', create=True) as reachability:
        # Test success
        reachability.return_value = MockQuestion()
        assert_flows_fail(startLocation, headers, session=bf)
        reachability.assert_called_with(
            pathConstraints=PathConstraints(startLocation=startLocation),
            headers=headers,
            actions='success')
        # Test failure
        mock_df = DataFrame.from_records([{'Flow': 'found', 'More': 'data'}])
        reachability.return_value = MockQuestion(
            MockTableAnswer(mock_df))
        with pytest.raises(BatfishAssertException) as excinfo:
            assert_flows_fail(startLocation, headers, session=bf)
        # Ensure found answer is printed
        assert mock_df.to_string() in str(excinfo.value)
        reachability.assert_called_with(
            pathConstraints=PathConstraints(startLocation=startLocation),
            headers=headers,
            actions='success')


def test_flows_fail_no_session():
    """
    Confirm flows-fail assert passes and fails as expected when not specifying a session.

    For reverse compatibility.
    """
    startLocation = "node1"
    headers = HeaderConstraints(srcIps='1.1.1.1')
    with patch.object(bfq, 'reachability', create=True) as reachability:
        # Test success
        reachability.return_value = MockQuestion()
        assert_flows_fail(startLocation, headers)
        reachability.assert_called_with(
            pathConstraints=PathConstraints(startLocation=startLocation),
            headers=headers,
            actions='success')
        # Test failure
        mock_df = DataFrame.from_records([{'Flow': 'found', 'More': 'data'}])
        reachability.return_value = MockQuestion(
            MockTableAnswer(mock_df))
        with pytest.raises(BatfishAssertException) as excinfo:
            assert_flows_fail(startLocation, headers)
        # Ensure found answer is printed
        assert mock_df.to_string() in str(excinfo.value)
        reachability.assert_called_with(
            pathConstraints=PathConstraints(startLocation=startLocation),
            headers=headers,
            actions='success')


def test_flows_succeed():
    """Confirm flows-succeed assert passes and fails as expected when specifying a session."""
    startLocation = "node1"
    headers = HeaderConstraints(srcIps='1.1.1.1')
    bf = Session(load_questions=False)
    with patch.object(bf.q, 'reachability', create=True) as reachability:
        # Test success
        reachability.return_value = MockQuestion()
        assert_flows_succeed(startLocation, headers, session=bf)
        reachability.assert_called_with(
            pathConstraints=PathConstraints(startLocation=startLocation),
            headers=headers,
            actions='failure')
        # Test failure
        mock_df = DataFrame.from_records([{'Flow': 'found', 'More': 'data'}])
        reachability.return_value = MockQuestion(
            MockTableAnswer(mock_df))
        with pytest.raises(BatfishAssertException) as excinfo:
            assert_flows_succeed(startLocation, headers, session=bf)
        # Ensure found answer is printed
        assert mock_df.to_string() in str(excinfo.value)
        reachability.assert_called_with(
            pathConstraints=PathConstraints(startLocation=startLocation),
            headers=headers,
            actions='failure')


def test_flows_succeed_no_session():
    """
    Confirm flows-succeed assert passes and fails as expected when not specifying a session.

    For reverse compatibility.
    """
    startLocation = "node1"
    headers = HeaderConstraints(srcIps='1.1.1.1')
    with patch.object(bfq, 'reachability', create=True) as reachability:
        # Test success
        reachability.return_value = MockQuestion()
        assert_flows_succeed(startLocation, headers)
        reachability.assert_called_with(
            pathConstraints=PathConstraints(startLocation=startLocation),
            headers=headers,
            actions='failure')
        # Test failure
        mock_df = DataFrame.from_records([{'Flow': 'found', 'More': 'data'}])
        reachability.return_value = MockQuestion(
            MockTableAnswer(mock_df))
        with pytest.raises(BatfishAssertException) as excinfo:
            assert_flows_succeed(startLocation, headers)
        # Ensure found answer is printed
        assert mock_df.to_string() in str(excinfo.value)
        reachability.assert_called_with(
            pathConstraints=PathConstraints(startLocation=startLocation),
            headers=headers,
            actions='failure')


def test_get_question_object():
    """Confirm _get_question_object identifies the correct question object based on the specified session and the questions it contains."""
    # Session contains the question we're searching for
    bf = Session(load_questions=False)
    with patch.object(bf.q, 'qName', create=True):
        assert bf.q == _get_question_object(bf, 'qName')

    # Session does not contain the question we're searching for, but bfq does
    with patch.object(bfq, 'qName', create=True):
        assert bfq == _get_question_object(bf, 'qName')

    # No Session specified, but bfq contains the question we're searching for
    with patch.object(bfq, 'qName', create=True):
        assert bfq == _get_question_object(None, 'qName')

    # Cannot find the question we're searching for
    with patch.object(bf.q, 'otherName', create=True):
        with patch.object(bfq, 'otherOtherName', create=True):
            with pytest.raises(BatfishException) as err:
                _get_question_object(bf, 'qName')
            assert 'qName question was not found' in str(err.value)


if __name__ == "__main__":
    pytest.main()
