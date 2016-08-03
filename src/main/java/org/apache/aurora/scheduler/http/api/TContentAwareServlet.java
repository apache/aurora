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
package org.apache.aurora.scheduler.http.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import static java.util.Objects.requireNonNull;

import static javax.ws.rs.core.HttpHeaders.ACCEPT;

/**
 * An implementation of {@link org.apache.thrift.server.TServlet} that can handle multiple thrift
 * protocols. The protocols are dispatched on HTTP headers.
 */
public class TContentAwareServlet extends HttpServlet {
  private final TProcessor processor;
  private final InputConfig inputConfig;
  private final OutputConfig outputConfig;

  /**
   * Class which contains the mapping of the factory and the content type of the output.
   */
  static class ContentFactoryPair implements TProtocolFactory {
    private final TProtocolFactory factory;

    private final MediaType outputType;

    ContentFactoryPair(TProtocolFactory factory, MediaType outputType) {
      this.factory = requireNonNull(factory);
      this.outputType = requireNonNull(outputType);
    }

    MediaType getOutputType() {
      return outputType;
    }

    @Override
    public TProtocol getProtocol(TTransport tTransport) {
      return factory.getProtocol(tTransport);
    }
  }

  /**
   * Configures how to interpret the Content-Type of the request.
   */
  static class InputConfig {
    // Type to use when there is no Content-Type
    private final MediaType defaultType;
    // Mapping of values in Content-Type to protocol to use to deserialize
    private final Map<MediaType, ContentFactoryPair> inputMapping;

    InputConfig(MediaType defaultType, Map<MediaType, ContentFactoryPair> inputMapping) {
      this.defaultType = requireNonNull(defaultType);
      this.inputMapping = requireNonNull(inputMapping);
    }

    Optional<ContentFactoryPair> getFactory(Optional<MediaType> mediaType) {
      return Optional.ofNullable(inputMapping.get(mediaType.orElse(defaultType)));
    }
  }

  /**
   * Configures how to interpret the Accept header of the request. The defaultType's factory is
   * returned for almost all values to maintain backwards compatibility.
   */
  static class OutputConfig {
    // Type to use when there is no Accept header
    private final MediaType defaultType;
    // Mapping of MediaTypes in the Accept header to protocol used to serialize the response
    private final Map<MediaType, ContentFactoryPair> outputMapping;
    private final ContentFactoryPair defaultFactory;

    OutputConfig(MediaType defaultType, Map<MediaType, ContentFactoryPair> outputMapping) {
      this.defaultType = requireNonNull(defaultType);
      this.outputMapping = requireNonNull(outputMapping);
      this.defaultFactory = requireNonNull(outputMapping.get(defaultType));
    }

    ContentFactoryPair getFactory(Optional<MediaType> type) {
      return Optional.ofNullable(outputMapping.get(type.orElse(defaultType)))
          .orElse(defaultFactory);
    }
  }

  TContentAwareServlet(TProcessor processor, InputConfig inputConfig, OutputConfig outputConfig) {
    this.processor = requireNonNull(processor);
    this.inputConfig = requireNonNull(inputConfig);
    this.outputConfig = requireNonNull(outputConfig);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    Optional<ContentFactoryPair> factoryOptional =
        inputConfig.getFactory(Optional.of(request.getContentType()).map(MediaType::valueOf));

    if (!factoryOptional.isPresent()) {
      response.setStatus(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
      String msg = "Unsupported Content-Type: " + request.getContentType();
      response.getOutputStream().write(msg.getBytes(StandardCharsets.UTF_8));
      return;
    }

    TTransport transport =
        new TIOStreamTransport(request.getInputStream(), response.getOutputStream());

    TProtocol inputProtocol = factoryOptional.get().getProtocol(transport);

    Optional<String> acceptHeader = Optional.ofNullable(request.getHeader(ACCEPT));
    Optional<MediaType> acceptType = Optional.empty();
    if (acceptHeader.isPresent()) {
      try {
        acceptType = acceptHeader.map(MediaType::valueOf);
      } catch (IllegalArgumentException e) {
        // Thrown if the Accept header contains more than one type or something else we can't
        // parse, we just treat is as no header (which will pick up the default value).
        acceptType = Optional.empty();
      }
    }

    ContentFactoryPair outputProtocolFactory = outputConfig.getFactory(acceptType);

    response.setContentType(outputProtocolFactory.getOutputType().toString());
    TProtocol outputProtocol = outputProtocolFactory.getProtocol(transport);
    try {
      processor.process(inputProtocol, outputProtocol);
      response.getOutputStream().flush();
    } catch (TException e) {
      throw new ServletException(e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    doPost(request, response);
  }
}
