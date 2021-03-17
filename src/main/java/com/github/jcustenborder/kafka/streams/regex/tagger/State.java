package com.github.jcustenborder.kafka.streams.regex.tagger;

import org.immutables.value.Value;

import java.util.regex.Matcher;

@Value.Immutable
public interface State {
  RegexConfig config();
  Matcher matcher();
}
