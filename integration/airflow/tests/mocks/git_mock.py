# SPDX-License-Identifier: Apache-2.0.

import logging
import subprocess

log = logging.getLogger(__name__)


def execute_git_mock(cwd, params):
    # just mock the git revision
    log.debug("execute_git_mock()")
    if len(cwd) > 0 and params[0] == 'rev-list':
        return 'abcd1234'

    p = subprocess.Popen(['git'] + params,
                         cwd=cwd, stdout=subprocess.PIPE, stderr=None)
    p.wait(timeout=0.5)
    out, err = p.communicate()
    return out.decode('utf8').strip()
