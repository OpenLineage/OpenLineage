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

package io.openlineage.client;

import javax.annotation.Nullable;

import io.openlineage.client.OpenLineageClientException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

/** An exception thrown to indicate an HTTP error. */
@NoArgsConstructor
@ToString
public final class OpenLineageHttpException extends OpenLineageClientException {
    private static final long serialVersionUID = 1L;

    @Getter @Nullable private Integer code;
    @Getter @Nullable private String message;
    @Getter @Nullable private String details;

    /** Constructs a {@code OpenLineageHttpException} with the HTTP error {@code error}. */
    OpenLineageHttpException(@NonNull final OpenLineageHttp.HttpError error) {
        this.code = error.getCode();
        this.message = error.getMessage();
        this.details = error.getDetails();
    }
}