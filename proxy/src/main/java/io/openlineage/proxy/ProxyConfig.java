/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.proxy;

import io.dropwizard.Configuration;

import java.util.Map;
import java.util.Properties;
import io.openlineage.proxy.api.models.ConsoleLineageStream;
import io.openlineage.proxy.api.models.LineageStream;
import javax.validation.constraints.NotEmpty;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ProxyConfig defines the structure of the configuration file proxy.yml
 */
public final class ProxyConfig extends Configuration
{
  @NotEmpty
  private String     lineageSourceName;

  private boolean    consoleLog = false;

  @NotEmpty
  private String     kafkaTopicName;
  @NotEmpty
  private String     kafkaBootstrapServerURL;
  @NotEmpty
  private Properties kafkaProperties = null;

  @JsonProperty
  public String getLineageSourceName()
  {
    return lineageSourceName;
  }

  @JsonProperty
  public void setLineageSourceName(String lineageSourceName)
  {
    this.lineageSourceName = lineageSourceName;
  }

  @JsonProperty
  public boolean getConsoleLog()
  {
    return consoleLog;
  }

  @JsonProperty
  public void setConsoleLog(boolean consoleLog)
  {
    this.consoleLog = consoleLog;
  }


  @JsonProperty
  public String getKafkaTopicName()
  {
    return kafkaTopicName;
  }

  @JsonProperty
  public void setKafkaTopicName(String kafkaTopicName)
  {
    this.kafkaTopicName = kafkaTopicName;
  }

  @JsonProperty
  public String getKafkaBootstrapServerURL()
  {
    return kafkaBootstrapServerURL;
  }

  @JsonProperty
  public void setKafkaBootstrapServerURL(String kafkaBootstrapServerURL)
  {
    this.kafkaBootstrapServerURL = kafkaBootstrapServerURL;
  }

  @JsonProperty
  public Properties getKafkaProperties()
  {
    return kafkaProperties;
  }

  @JsonProperty
  public void setKafkaProperties(Properties kafkaProperties)
  {
    this.kafkaProperties = kafkaProperties;
  }
}
