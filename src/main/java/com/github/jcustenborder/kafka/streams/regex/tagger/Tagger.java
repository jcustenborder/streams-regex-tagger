package com.github.jcustenborder.kafka.streams.regex.tagger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class Tagger implements Callable<Integer> {
  private static final Logger log = LoggerFactory.getLogger(Tagger.class);

  @CommandLine.Option(names = {"-s", "--streams-config"}, required = true, paramLabel = "STREAMS_CONFIG", description = "Configuration file for Kafka Streams.")
  File streamsConfig;

  @CommandLine.Option(names = {"-c", "--config-directory"}, required = true, paramLabel = "CONFIG_DIRECTORY", description = "Directory containing yml mapping files.")
  File configDirectory;

  @CommandLine.Option(names = {"-i", "--input"}, required = true, paramLabel = "TOPIC", description = "Input Topic to read from")
  String inputTopic;
  @CommandLine.Option(names = {"-o", "--output"}, required = true, paramLabel = "TOPIC", description = "Output topic to write to")
  String outputTopic;

  @Override
  public Integer call() throws Exception {
    List<RegexConfig> regexConfigs = loadRegexConfigs();
    Properties streamsConfig = loadStreamsConfig();

    StreamsBuilder builder = new StreamsBuilder();
    RegexMapper mapper = new RegexMapper(regexConfigs);
    builder.stream(this.inputTopic, Consumed.with(Serdes.ByteArray(), JsonMapSerde.of()))
        .flatMapValues(mapper)
        .to(this.outputTopic, Produced.with(Serdes.ByteArray(), JsonMapSerde.of()));
    Topology topology = builder.build();

    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);

    //Workaround because we're in a daemon thread
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      kafkaStreams.close();
      countDownLatch.countDown();
    }));
    kafkaStreams.start();
    countDownLatch.await();

    return 0;
  }

  private Properties loadStreamsConfig() throws IOException {
    if (!this.streamsConfig.exists()) {
      throw new FileNotFoundException(
          String.format("Streams config %s was not found", this.streamsConfig)
      );
    }

    log.info("Loading Streams Config from {}", this.streamsConfig);
    Properties properties = new Properties();
    try (InputStream inputStream = new FileInputStream(this.streamsConfig)) {
      properties.load(inputStream);
    }
    return properties;
  }

  private List<RegexConfig> loadRegexConfigs() throws IOException {

    if (!this.configDirectory.exists()) {
      throw new FileNotFoundException(
          String.format("Config Directory %s was not found", this.configDirectory)
      );
    }

    log.info("Searching for yml or yaml files in {}", this.configDirectory);
    File[] configFiles = this.configDirectory.listFiles((dir, p) -> p.endsWith(".yml") || p.endsWith(".yaml"));
    log.info("Found {} file(s).", configFiles.length);

    if (configFiles.length == 0) {
      throw new IllegalStateException(
          String.format("No yaml files found in %s.", this.configDirectory)
      );
    }
    List<RegexConfig> result = new ArrayList<>();
    ObjectMapper mapper = new YAMLMapper();
    for (File configFile : configFiles) {
      log.info("Loading {}", configFile);
      RegexConfig config = mapper.readValue(configFile, RegexConfig.class);
      result.add(config);
    }
    return result;
  }

  public static void main(String... args) throws Exception {
    int result = new CommandLine(new Tagger()).execute(args);
    System.exit(result);
  }
}
