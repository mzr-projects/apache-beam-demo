package com.mt.apache.beam.demo.services;

import com.mt.apache.beam.demo.doFns.OutPutLog;
import com.mt.apache.beam.demo.doFns.ToUppercaseDoFn;
import com.mt.apache.beam.demo.pipelines.PipelineCreation;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ToUppercaseService {

    public void toUppercase(){

        Pipeline pipeline = PipelineCreation.createPipeline();

        PCollection<String> input = pipeline.apply(
                "demo-input-string",
                Create.of("Hello", "World", "My", "Name", "Is", "MT")
        );
        input.apply("Log", ParDo.of(new OutPutLog<>("Before-ToUppercase")));

        PCollection<String> toUpperCased = input.apply(ParDo.of(new ToUppercaseDoFn()));
        toUpperCased.apply("After-ToUppercase-Log", ParDo.of(new OutPutLog<>("After-ToUppercase")));

        pipeline.run().waitUntilFinish();
    }
}
