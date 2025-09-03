package com.mt.apache.beam.demo.services;

import com.mt.apache.beam.demo.doFns.OutPutLog;
import com.mt.apache.beam.demo.doFns.WordCountDoFn;
import com.mt.apache.beam.demo.pipelines.PipelineCreation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class StreamingWordCountService {

    @Value("${apache.beam.kafka.servers}")
    private String bootstrapServers;

    @Value("${apache.beam.kafka.topic}")
    private String topic;

    public void streamingWordCount() {

        Pipeline pipeline = PipelineCreation.createPipeline("WordCount");
        PCollection<KV<String, String>> lines = pipeline.apply(
                "ReadFromKafka",
                KafkaIO.<String, String>read()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(topic)
                        .withConsumerConfigUpdates(
                                Map.of(
                                        "auto.offset.reset", "latest",
                                        "isolation.level", "read_committed",
                                        "group.id", "apache-beam-group"
                                )
                        )
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withReadCommitted()
                        .commitOffsetsInFinalize()
                        .withoutMetadata()
        );

        PCollection<String> words = lines
                .apply("ExtractValues", Values.create())
                .apply("ExtractWords", ParDo.of(new WordCountDoFn()));

        PCollection<String> log = words.apply("FormatAsText",ParDo.of(new OutPutLog("formatted-words")));

        pipeline.run().waitUntilFinish();
    }
}
