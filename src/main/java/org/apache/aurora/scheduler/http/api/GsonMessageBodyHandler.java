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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializer;

import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TUnion;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

/**
 * A message body reader/writer that uses gson to translate JSON to and from java objects produced
 * by the thrift compiler.
 * <p>
 * This is used since jackson doesn't provide target type information to custom deserializer
 * implementations, so it is apparently not possible to implement a generic deserializer for
 * sublasses of {@link TUnion}.
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class GsonMessageBodyHandler
    implements MessageBodyReader<Object>, MessageBodyWriter<Object> {

  @Override
  public Object readFrom(
      Class<Object> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException {

    // For some reason try-with-resources syntax trips a findbugs error here.
    InputStreamReader streamReader = null;
    try {
      streamReader = new InputStreamReader(entityStream, StandardCharsets.UTF_8);
      Type jsonType;
      if (type.equals(genericType)) {
        jsonType = type;
      } else {
        jsonType = genericType;
      }
      return GSON.fromJson(streamReader, jsonType);
    } finally {
      if (streamReader != null) {
        streamReader.close();
      }
    }
  }

  @Override
  public void writeTo(
      Object o,
      Class<?> type,
      Type genericType, Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, Object> httpHeaders,
      OutputStream entityStream) throws IOException, WebApplicationException {

    try (OutputStreamWriter writer = new OutputStreamWriter(entityStream, StandardCharsets.UTF_8)) {
      Type jsonType;
      if (type.equals(genericType)) {
        jsonType = type;
      } else {
        jsonType = genericType;
      }
      GSON.toJson(o, jsonType, writer);
    }
  }

  @Override
  public boolean isReadable(
      Class<?> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType) {

    return true;
  }

  @Override
  public boolean isWriteable(
      Class<?> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType) {

    return true;
  }

  @Override
  public long getSize(
      Object o,
      Class<?> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType) {

    return -1;
  }

  private static final Set<String> THRIFT_CONTROL_FIELDS = ImmutableSet.of(
      "__isset_bitfield",
      "optionals");

  private static final ExclusionStrategy EXCLUDE_THRIFT_FIELDS = new ExclusionStrategy() {
    @Override
    public boolean shouldSkipField(FieldAttributes f) {
      return THRIFT_CONTROL_FIELDS.contains(f.getName());
    }

    @Override
    public boolean shouldSkipClass(Class<?> clazz) {
      return false;
    }
  };

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static TUnion<?, ?> createUnion(
      Class<?> unionType,
      TFieldIdEnum setField,
      Object fieldValue) throws IllegalAccessException, InstantiationException {

    TUnion union = (TUnion) unionType.newInstance();
    union.setFieldValue(setField, fieldValue);
    return union;
  }

  public static final Gson GSON = new GsonBuilder()
      .addSerializationExclusionStrategy(EXCLUDE_THRIFT_FIELDS)
      .registerTypeHierarchyAdapter(
          TUnion.class,
          (JsonSerializer<TUnion<?, ?>>) (src, typeOfSrc, context) -> context.serialize(
              ImmutableMap.of(src.getSetField().getFieldName(), src.getFieldValue())))
      .registerTypeHierarchyAdapter(
          TUnion.class,
          (JsonDeserializer<TUnion<?, ?>>) (json, typeOfT, context) -> {
            JsonObject jsonObject = json.getAsJsonObject();
            if (jsonObject.entrySet().size() != 1) {
              throw new JsonParseException(
                  typeOfT.getClass().getName() + " must have exactly one element");
            }

            if (typeOfT instanceof Class) {
              Class<?> clazz = (Class<?>) typeOfT;
              Entry<String, JsonElement> item = Iterables.getOnlyElement(jsonObject.entrySet());

              try {
                Field metaDataMapField = clazz.getField("metaDataMap");
                @SuppressWarnings("unchecked")
                Map<TFieldIdEnum, FieldMetaData> metaDataMap =
                    (Map<TFieldIdEnum, FieldMetaData>) metaDataMapField.get(null);

                for (Entry<TFieldIdEnum, FieldMetaData> entry : metaDataMap.entrySet()) {
                  if (entry.getKey().getFieldName().equals(item.getKey())) {
                    Object result;
                    if (entry.getValue().valueMetaData.isStruct()) {
                      StructMetaData valueMeta = (StructMetaData) entry.getValue().valueMetaData;
                      result = context.deserialize(item.getValue(), valueMeta.structClass);
                    } else {
                      FieldValueMetaData valueMeta = entry.getValue().valueMetaData;
                      Type type;
                      switch (valueMeta.type) {
                        case TType.DOUBLE:
                          type = Double.TYPE;
                          break;
                        case TType.I64:
                          type = Long.TYPE;
                          break;
                        case TType.STRING:
                          type = String.class;
                          break;
                        default:
                          throw new RuntimeException("Unmapped type: " + valueMeta.type);
                      }
                      result = context.deserialize(item.getValue(), type);
                    }
                    return createUnion(clazz, entry.getKey(), result);
                  }
                }

                throw new RuntimeException("Failed to deserialize " + typeOfT);
              } catch (NoSuchFieldException | IllegalAccessException | InstantiationException e) {
                throw Throwables.propagate(e);
              }
            } else {
              throw new RuntimeException("Unable to deserialize " + typeOfT);
            }
          })
      .create();
}
