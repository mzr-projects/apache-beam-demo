package com.mt.apache.beam.demo.doFns;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class ToUppercaseDoFn extends DoFn<String,String> {

    @ProcessElement
    public void processElement(@DoFn.Element String element, OutputReceiver<String> out) {
        out.output(element.toUpperCase());
    }
}
