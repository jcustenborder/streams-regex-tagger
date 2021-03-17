package com.github.jcustenborder.kafka.streams.regex.tagger;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RegexMapperTests {
  private static final Logger log = LoggerFactory.getLogger(RegexMapperTests.class);

  @Test
  public void showRule() throws JsonProcessingException {
    RegexConfig config = ImmutableRegexConfig.builder()
        .name("test")
        .pattern("^(?<host>[^\\s]+)\\s+(?<timestamp>[^\\s]+)")
        .addGroups("host")
        .inputField("text")
        .build();
    ObjectMapper mapper = new YAMLMapper();
    log.info(mapper.writeValueAsString(config));
  }

  @Test
  public void apply() {


    RegexMapper mapper = new RegexMapper(
        Arrays.asList(
            ImmutableRegexConfig.builder()
                .name("test")
                .pattern("^(?<host>[^\\s]+)\\s+(?<timestamp>[^\\s]+)")
                .addGroups("host")
                .inputField("text")
                .build()
        )
    );
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("text", "215.121.68.3 - - [01/Oct/2016 00:02:57:715302] \"GET /cart.do?action=view&item_id=DFS-2&product_id=DFS-2&JSESSIONID=SD1SBL1FF10ADFF4 HTTP 1.1\" 404 1959 \"http://shop.acme.com/cart.do?action=view&item_id=DFS-2&product_id=DFS-2\" \"mozilla/5.0 (iPad; CPU OS 5_0_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) 1Password/3.6.1/361009 (like Mobile/8C148 Safari/6533.18.5)\" 101");


    List<Map<String, Object>> result = mapper.apply(input);
    assertNotNull(result);
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.putAll(input);
    expected.put("host", "215.121.68.3");

    assertEquals(Arrays.asList(expected), result);

  }
}
