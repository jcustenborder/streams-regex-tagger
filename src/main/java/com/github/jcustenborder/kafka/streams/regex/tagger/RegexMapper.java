package com.github.jcustenborder.kafka.streams.regex.tagger;

import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexMapper implements ValueMapper<Map<String, Object>, List<Map<String, Object>>> {
  final List<State> states;

  public RegexMapper(List<RegexConfig> configs) {
    List<State> states = new ArrayList<>();

    for (RegexConfig config : configs) {
      Pattern pattern = Pattern.compile(config.pattern());
      Matcher matcher = pattern.matcher("");
      states.add(
          ImmutableState.builder()
              .matcher(matcher)
              .config(config)
              .build()
      );
    }

    this.states = states;
  }


  @Override
  public List<Map<String, Object>> apply(Map<String, Object> input) {
    List<Map<String, Object>> result = new ArrayList<>(this.states.size());

    for (State state : this.states) {
      String inputField = input.getOrDefault(state.config().inputField(), "").toString();
      state.matcher().reset(inputField);

      if (state.matcher().find()) {
        Map<String, Object> copy = new LinkedHashMap<>(input.size() + state.config().groups().size());
        copy.putAll(input);
        for (String group : state.config().groups()) {
          copy.put(group, state.matcher().group(group));
        }
        result.add(copy);
      }
    }

    return result;
  }
}
