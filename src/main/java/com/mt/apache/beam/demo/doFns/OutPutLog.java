package com.mt.apache.beam.demo.doFns;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class OutPutLog<T> extends DoFn<T, T> {

    private final String prefix;

    public OutPutLog(String prefix) {
        this.prefix = prefix;
    }

    public OutPutLog() {
        this.prefix = "processedElement";
    }

    @ProcessElement
    public void processElement(@DoFn.Element T element, OutputReceiver<T> out) {
        log.info("{} element: {}",prefix, element);
        out.output(element);
    }
}
