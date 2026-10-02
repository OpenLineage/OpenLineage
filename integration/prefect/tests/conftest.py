# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import sys

# Find the absolute path of the directory containing 'src' 
# by stepping one folder up from 'tests/'
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..src/openlineage/"))

# Inject it into the front of Python's search path
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)
