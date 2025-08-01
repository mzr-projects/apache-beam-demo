package com.mt.apache.beam.demo.pipelines;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@Slf4j
public class PipelineCreation {

    private PipelineCreation(){

    }

    public static Pipeline createPipeline() {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        return Pipeline.create(pipelineOptions);
    }
}
