package com.github.jcustenborder.kafka.streams.regex.tagger;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
@JsonDeserialize(as = ImmutableRegexConfig.class)
@JsonSerialize(as = ImmutableRegexConfig.class)
public interface RegexConfig {
  String name();

  String pattern();

  List<String> groups();

  String inputField();
}
