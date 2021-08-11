package openlineage.spark.agent.client;

import static org.apache.hc.core5.http.HttpHeaders.AUTHORIZATION;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ok2c.hc5.json.http.JsonRequestProducers;
import com.ok2c.hc5.json.http.JsonResponseConsumers;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.async.methods.BasicHttpRequests;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.Message;

@Slf4j
public class OpenLineageClient {

  public static final String OPEN_LINEAGE_CLIENT_URI = getUri();
  public static final String OPEN_LINEAGE_PARENT_FACET_URI =
      "https://openlineage.io/spec/1-0-0/OpenLineage.json#/definitions/ParentRunFacet";
  public static final String OPEN_LINEAGE_DATASOURCE_FACET =
      "https://openlineage.io/spec/1-0-0/OpenLineage.json#/definitions/DatasourceDatasetFacet";
  public static final String OPEN_LINEAGE_SCHEMA_FACET_URI =
      "https://openlineage.io/spec/1-0-0/OpenLineage.json#/definitions/SchemaDatasetFacet";

  private final CloseableHttpAsyncClient http;
  private final ExecutorService executorService;
  private final Optional<String> apiKey;
  @Getter protected static final ObjectMapper objectMapper = createMapper();

  public OpenLineageClient(
      CloseableHttpAsyncClient http, Optional<String> apiKey, ExecutorService executorService) {
    this.http = http;
    this.executorService = executorService;
    this.http.start();
    this.apiKey = apiKey;
  }

  public static OpenLineageClient create(
      final Optional<String> apiKey, ExecutorService executorService) {
    final CloseableHttpAsyncClient http = HttpAsyncClients.createDefault();
    return new OpenLineageClient(http, apiKey, executorService);
  }

  public <T> ResponseMessage post(URI uri, Object obj) throws OpenLineageHttpException {
    return post(uri, obj, Void.class);
  }

  public <T> ResponseMessage<T> post(URI uri, Object obj, Class<T> clazz)
      throws OpenLineageHttpException {
    return post(uri, obj, getTypeReference(clazz));
  }

  public <T> ResponseMessage<T> post(URI uri, Object obj, TypeReference<T> ref)
      throws OpenLineageHttpException {
    return executeSync(BasicHttpRequests.post(uri), obj, ref);
  }

  public <T> ResponseMessage<T> executeSync(HttpRequest request, Object obj, TypeReference<T> ref)
      throws OpenLineageHttpException {
    CompletableFuture<ResponseMessage<T>> future = executeAsync(request, obj, ref);
    try {
      ResponseMessage<T> message =
          (ResponseMessage<T>)
              future
                  .exceptionally(
                      (resp) -> {
                        return new ResponseMessage(
                            0, null, new HttpError(0, resp.getMessage(), resp.toString()));
                      })
                  .get();
      if (message == null) {
        return new ResponseMessage(0, null, new HttpError(0, "unknown error", "unknown error"));
      }
      return message;
    } catch (ExecutionException | InterruptedException e) {
      throw new OpenLineageHttpException(e);
    }
  }

  public CompletableFuture<ResponseMessage<Void>> postAsync(URI uri, Object obj) {
    return postAsync(uri, obj, Void.class);
  }

  public <T> CompletableFuture<ResponseMessage<T>> postAsync(URI uri, Object obj, Class<T> clazz) {
    return postAsync(uri, obj, getTypeReference(clazz));
  }

  public <T> CompletableFuture<ResponseMessage<T>> postAsync(
      URI uri, Object obj, TypeReference<T> ref) {
    return executeAsync(BasicHttpRequests.post(uri), obj, ref);
  }

  protected <T> CompletableFuture<ResponseMessage<T>> executeAsync(
      HttpRequest request, Object obj, TypeReference<T> ref) {
    addAuthToReqIfKeyPresent(request);

    Future<Message<HttpResponse, JsonNode>> future =
        http.execute(
            JsonRequestProducers.create(request, obj, objectMapper),
            JsonResponseConsumers.create(objectMapper.getFactory()),
            null);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            Message<HttpResponse, JsonNode> message = future.get();
            return createMessage(message, ref);
          } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
          }
        },
        executorService);
  }

  private <T> ResponseMessage<T> createMessage(
      Message<HttpResponse, JsonNode> message, TypeReference<T> ref) {
    if (!completedSuccessfully(message)) {
      return new ResponseMessage<>(
          message.getHead().getCode(),
          null,
          objectMapper.convertValue(message.getBody(), HttpError.class));
    }

    return new ResponseMessage<>(
        message.getHead().getCode(), objectMapper.convertValue(message.getBody(), ref), null);
  }

  private boolean completedSuccessfully(Message<HttpResponse, JsonNode> message) {
    final int code = message.getHead().getCode();
    if (code >= 400 && code < 600) { // non-2xx
      return false;
    }
    return true;
  }

  public static ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
    return mapper;
  }

  private void addAuthToReqIfKeyPresent(final HttpRequest request) {
    if (apiKey.isPresent()) {
      request.addHeader(AUTHORIZATION, "Bearer " + apiKey.get());
    }
  }

  protected static String getUserAgent() {
    return "openlineage-java" + "/1.0";
  }

  private static String getUri() {
    return String.format(
        "https://github.com/OpenLineage/OpenLineage/tree/%s/integration/spark", getVersion());
  }

  private static String getVersion() {
    try {
      Properties properties = new Properties();
      InputStream is = OpenLineageClient.class.getResourceAsStream("version.properties");
      properties.load(is);
      return properties.getProperty("version");
    } catch (IOException exception) {
      return "main";
    }
  }

  private <T> TypeReference<T> getTypeReference(Class<T> clazz) {
    return new TypeReference<T>() {
      @Override
      public Type getType() {
        return clazz;
      }
    };
  }

  public void close() {
    try {
      http.close();
    } catch (IOException e) {
      e.printStackTrace(System.out);
    }
  }
}
