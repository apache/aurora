/**
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
package org.apache.aurora.scheduler.thrift;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.twitter.common.base.MorePreconditions;

import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ResponseDetail;

/**
 * Utility class for constructing responses to API calls.
 */
public final class Util {

  private Util() {
    // Utility class.
  }

  /**
   * Creates a new empty response.
   *
   * @return An empty response message.
   */
  public static Response emptyResponse() {
    return new Response().setDetails(Lists.<ResponseDetail>newArrayList());
  }

  /**
   * Adds a human-friendly message to a response, usually to indicate a problem or deprecation
   * encountered while handling the request.
   *
   * @param response Response to augment.
   * @param message Message to include in the response.
   * @return {@code response} with {@code message} included.
   */
  public static Response addMessage(Response response, String message) {
    return appendMessage(response, MorePreconditions.checkNotBlank(message));
  }

  /**
   * Identical to {@link #addMessage(Response, String)} that also applies a response code.
   *
   * @param response Response to augment.
   * @param code Response code to include.
   * @param message Message to include in the response.
   * @return {@code response} with {@code message} included.
   * @see {@link #addMessage(Response, String)}
   */
  public static Response addMessage(Response response, ResponseCode code, String message) {
    return addMessage(response.setResponseCode(code), message);
  }

  /**
   * Identical to {@link #addMessage(Response, String)} that also applies a response code and
   * extracts a message from the provided {@link Throwable}.
   *
   * @param response Response to augment.
   * @param code Response code to include.
   * @param throwable {@link Throwable} to extract message from.
   * @return {@link #addMessage(Response, String)}
   */
  public static Response addMessage(Response response, ResponseCode code, Throwable throwable) {
    return appendMessage(response.setResponseCode(code), throwable.getMessage());
  }

  private static Response appendMessage(Response response, String message) {
    String existingMessage = response.getMessageDEPRECATED();
    String prefix = Strings.isNullOrEmpty(existingMessage) ? "" : existingMessage + ", ";
    response.setMessageDEPRECATED(prefix + message);

    response.addToDetails(new ResponseDetail(message));
    return response;
  }
}
