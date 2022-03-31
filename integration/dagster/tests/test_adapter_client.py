# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

from openlineage.dagster.adapter import OpenLineageAdapter


def test_custom_client_config():
    os.environ["OPENLINEAGE_URL"] = "http://ol-api:5000"
    os.environ["OPENLINEAGE_API_KEY"] = "api-key"
    adapter = OpenLineageAdapter()
    assert adapter._client.transport.url == "http://ol-api:5000"
    assert adapter._client.transport.session.headers['Authorization'] == "Bearer api-key"
    del os.environ["OPENLINEAGE_URL"]
    del os.environ["OPENLINEAGE_API_KEY"]
