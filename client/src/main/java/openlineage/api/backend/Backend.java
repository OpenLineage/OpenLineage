package openlineage.api.backend;

public interface Backend {

  public void call(String method, String path, Parameters pathParams, Parameters params, Object body);

}
