package com.github.jcustenborder.kafka.streams.regex.tagger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

class JsonMapSerde implements Serde<Map<String, Object>> {
  static final ObjectMapper mapper = new ObjectMapper();

  public static JsonMapSerde of() {
    return new JsonMapSerde();
  }

  static class JsonMapSerializer implements Serializer<Map<String, Object>> {
    @Override
    public byte[] serialize(String s, Map<String, Object> input) {
      byte[] result;
      if (null == input) {
        result = null;
      } else {
        try {
          result = mapper.writeValueAsBytes(input);
        } catch (JsonProcessingException e) {
          throw new SerializationException(e);
        }
      }
      return result;
    }
  }

  static class JsonMapDeserializer implements Deserializer<Map<String, Object>> {

    @Override
    public Map<String, Object> deserialize(String s, byte[] bytes) {
      Map<String, Object> result;
      if (null == bytes) {
        result = null;
      } else {
        try {
          result = mapper.readValue(bytes, Map.class);
        } catch (IOException e) {
          throw new SerializationException(e);
        }
      }


      return result;
    }
  }

  @Override
  public Serializer<Map<String, Object>> serializer() {
    return new JsonMapSerializer();
  }

  @Override
  public Deserializer<Map<String, Object>> deserializer() {
    return new JsonMapDeserializer();
  }
}
