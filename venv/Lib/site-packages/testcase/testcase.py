# Copyright 2018, Alan Zaffetti
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ========================================================================
"""Run tabular unit tests."""

import collections


TestCase = collections.namedtuple('TestCase',
                                  ['name',
                                   'args',
                                   'kwargs',
                                   'expect',
                                   'raises'])


def new(name=None, args=None, kwargs=None, expect=None, raises=None):
    """create a new `TestCase`.

    Args:
        name: (string) test case name or description.
        args: (list) list of positional test arguments.
        kwargs: (dict) keyword test arguments.
        expect: expected test result or `None` if no result.
        raises: expected exception class or `None` if no exception.
    
    Returns:
        a new `TestCase`.
    """
    return TestCase(name, args, kwargs or {}, expect, raises)


def runall(tc, func, testcases):
    """run all test cases
    
    Args:
        tc: (`unittest.TestCase`) testing instance having `assertEqual`
            and `assertRaises`.
        func: (callable) the callable testing function.
        testcases: (iterable) `TestCase` table.
    """
    for t in testcases:
        if t.raises is None:
            tc.assertEqual(
                    func(*t.args, **t.kwargs),
                    t.expect,
                    msg=t.name)
        else:
            with tc.assertRaises(t.raises, msg=t.name):
                func(*t.args, **t.kwargs)
