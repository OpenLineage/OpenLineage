/*
 * SPDX-License-Identifier: Apache-2.0.
 */

package io.openlineage.proxy;

import com.fasterxml.jackson.databind.SerializationFeature;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.openlineage.proxy.api.ProxyResource;
import io.openlineage.proxy.service.ProxyService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** ProxyApp is the main class of the Proxy Backend. */
@Slf4j
public final class ProxyApp extends Application<ProxyConfig> {
  private static final String APP_NAME = "OpenLineageProxyBackend";
  private static final boolean ERROR_ON_UNDEFINED = false;

  /**
   * The main function receives the config file which is used in the initialization of the proxy
   * backend.
   *
   * @param args commandline arguments
   * @throws Exception issues with initialization
   */
  public static void main(final String[] args) throws Exception {
    new ProxyApp().run(args);
  }

  /**
   * Standard dropwizard function to return fixed name of the application at the endpoint.
   *
   * @return name of this application
   */
  @Override
  public String getName() {
    return APP_NAME;
  }

  /**
   * Initialize the application.
   *
   * @param bootstrap combination of the yml file and environment variables
   */
  @Override
  public void initialize(@NonNull Bootstrap<ProxyConfig> bootstrap) {
    // Enable variable substitution with environment variables.
    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(),
            new EnvironmentVariableSubstitutor(ERROR_ON_UNDEFINED)));

    bootstrap.getObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  /**
   * Called from main.
   *
   * @param config yml file
   * @param env runtime platform environment
   */
  @Override
  public void run(@NonNull ProxyConfig config, @NonNull Environment env) {
    log.debug("Registering resources...");
    env.jersey().register(new ProxyResource(new ProxyService(config)));
  }
}
