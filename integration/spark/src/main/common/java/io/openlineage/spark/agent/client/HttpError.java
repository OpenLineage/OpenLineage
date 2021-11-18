package io.openlineage.spark.agent.client;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@RequiredArgsConstructor
public class HttpError {
  protected Integer code;
  @NonNull protected String message;
  protected String details;
}
