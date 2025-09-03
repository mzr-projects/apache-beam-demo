package com.mt.apache.beam.demo.pipelines;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.protocol.types.Field;

@Slf4j
public class PipelineCreation {

    private PipelineCreation(){

    }

    public static Pipeline createPipeline(String jobName   ) {
        FlinkPipelineOptions pipelineOptions = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
        pipelineOptions.setRunner(FlinkRunner.class);
        pipelineOptions.setFlinkMaster("localhost:8081"); // or "localhost:8081" for cluster
        pipelineOptions.setParallelism(4);
        pipelineOptions.setCheckpointingInterval(10000L); ;
        pipelineOptions.setJobName(jobName);
        pipelineOptions.setCheckpointingInterval(60000L); // 1 minute
        pipelineOptions.setCheckpointingMode("EXACTLY_ONCE");
        pipelineOptions.setCheckpointTimeoutMillis(600000L);
        pipelineOptions.setStreaming(true);
        return Pipeline.create(pipelineOptions);
    }
}
