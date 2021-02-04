package marquez.spark.agent.client;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Exclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

/**
 * Requires jackson serialization features:
 *
 * <p>mapper.registerModule(new JavaTimeModule());
 * mapper.setSerializationInclusion(Include.NON_NULL);
 * mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
 * mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class LineageEvent {

  private String eventType;
  @NonNull private ZonedDateTime eventTime;
  @NonNull private LineageEvent.Run run;
  @NonNull private LineageEvent.Job job;
  private List<Dataset> inputs;
  private List<Dataset> outputs;
  @NonNull private String producer;

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @Getter
  @ToString
  @NonNull
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Run {

    @NonNull private String runId;
    private RunFacet facets;
  }

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"nominalTime", "parent"})
  public static class RunFacet {

    private NominalTimeRunFacet nominalTime;
    private ParentRunFacet parent;

    @Builder.Default private Map<String, Object> additional = new LinkedHashMap<>();

    @JsonAnySetter
    public void setFacet(String key, Object value) {
      additional.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalFacets() {
      return additional;
    }

    public NominalTimeRunFacet getNominalTime() {
      return nominalTime;
    }

    public ParentRunFacet getParent() {
      return parent;
    }
  }

  @Getter
  @Setter
  public abstract static class BaseFacet {

    @NonNull private URI _producer;
    @NonNull private URI _schemaURL;

    protected BaseFacet() {}

    public BaseFacet(@NonNull URI _producer, @NonNull URI _schemaURL) {
      this._producer = _producer;
      this._schemaURL = _schemaURL;
    }
  }

  @Getter
  @Setter
  @ToString
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class NominalTimeRunFacet extends BaseFacet {

    @NonNull private ZonedDateTime nominalStartTime;
    private ZonedDateTime nominalEndTime;

    @Builder
    public NominalTimeRunFacet(
        @NonNull URI _producer,
        @NonNull URI _schemaURL,
        @NonNull ZonedDateTime nominalStartTime,
        ZonedDateTime nominalEndTime) {
      super(_producer, _schemaURL);
      this.nominalStartTime = nominalStartTime;
      this.nominalEndTime = nominalEndTime;
    }
  }

  @NoArgsConstructor
  @Getter
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ParentRunFacet extends BaseFacet {

    @NonNull private RunLink run;
    @NonNull private JobLink job;

    @Builder
    public ParentRunFacet(
        @NonNull URI _producer,
        @NonNull URI _schemaURL,
        @NonNull RunLink run,
        @NonNull JobLink job) {
      super(_producer, _schemaURL);
      this.run = run;
      this.job = job;
    }
  }

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @Getter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RunLink {

    @NonNull private String runId;
  }

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @Getter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class JobLink {

    @NonNull private String namespace;
    @NonNull private String name;
  }

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @Getter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Job {

    @NonNull private String namespace;
    @NonNull private String name;
    private JobFacet facets;
  }

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"documentation", "sourceCodeLocation", "sql", "description"})
  public static class JobFacet {

    private DocumentationJobFacet documentation;
    private SourceCodeLocationJobFacet sourceCodeLocation;
    private SQLJobFacet sql;
    @Builder.Default private Map<String, Object> additional = new LinkedHashMap<>();

    @JsonAnySetter
    public void setFacet(String key, Object value) {
      additional.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalFacets() {
      return additional;
    }

    public DocumentationJobFacet getDocumentation() {
      return documentation;
    }

    public SourceCodeLocationJobFacet getSourceCodeLocation() {
      return sourceCodeLocation;
    }

    public SQLJobFacet getSql() {
      return sql;
    }
  }

  @NoArgsConstructor
  @Getter
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DocumentationJobFacet extends BaseFacet {

    @NonNull private String description;

    @Builder
    public DocumentationJobFacet(
        @NonNull URI _producer, @NonNull URI _schemaURL, @NonNull String description) {
      super(_producer, _schemaURL);
      this.description = description;
    }
  }

  @NoArgsConstructor
  @Getter
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SourceCodeLocationJobFacet extends BaseFacet {

    private String type;
    private String url;

    @Builder
    public SourceCodeLocationJobFacet(
        @NonNull URI _producer, @NonNull URI _schemaURL, String type, String url) {
      super(_producer, _schemaURL);
      this.type = type;
      this.url = url;
    }
  }

  @NoArgsConstructor
  @Getter
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SQLJobFacet extends BaseFacet {

    @NonNull private String query;

    @Builder
    public SQLJobFacet(@NonNull URI _producer, @NonNull URI _schemaURL, @NonNull String query) {
      super(_producer, _schemaURL);
      this.query = query;
    }
  }

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @Getter
  @ToString
  @NonNull
  @JsonIgnoreProperties(ignoreUnknown = true)
  @EqualsAndHashCode
  public static class Dataset {

    @NonNull private String namespace;
    @NonNull private String name;
    @Exclude private DatasetFacet facets;
  }

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"documentation", "schema", "dataSource", "description"})
  public static class DatasetFacet {

    private DocumentationDatasetFacet documentation;
    private SchemaDatasetFacet schema;
    private DatasourceDatasetFacet dataSource;
    private String description;
    @Builder.Default private Map<String, Object> additional = new LinkedHashMap<>();

    @JsonAnySetter
    public void setFacet(String key, Object value) {
      additional.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalFacets() {
      return additional;
    }

    public DocumentationDatasetFacet getDocumentation() {
      return documentation;
    }

    public SchemaDatasetFacet getSchema() {
      return schema;
    }

    public DatasourceDatasetFacet getDataSource() {
      return dataSource;
    }

    public String getDescription() {
      return description;
    }
  }

  @NoArgsConstructor
  @Getter
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DocumentationDatasetFacet extends BaseFacet {

    @NonNull private String description;

    @Builder
    public DocumentationDatasetFacet(
        @NonNull URI _producer, @NonNull URI _schemaURL, @NonNull String description) {
      super(_producer, _schemaURL);
      this.description = description;
    }
  }

  @NoArgsConstructor
  @Getter
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SchemaDatasetFacet extends BaseFacet {

    private List<SchemaField> fields;

    @Builder
    public SchemaDatasetFacet(
        @NonNull URI _producer, @NonNull URI _schemaURL, List<SchemaField> fields) {
      super(_producer, _schemaURL);
      this.fields = fields;
    }
  }

  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  @Setter
  @Getter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SchemaField {

    @NonNull private String name;
    @NonNull private String type;
    private String description;
  }

  @NoArgsConstructor
  @Getter
  @Setter
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DatasourceDatasetFacet extends BaseFacet {

    private String name;
    private String uri;

    @Builder
    public DatasourceDatasetFacet(
        @NonNull URI _producer, @NonNull URI _schemaURL, String name, String uri) {
      super(_producer, _schemaURL);
      this.name = name;
      this.uri = uri;
    }
  }
}
