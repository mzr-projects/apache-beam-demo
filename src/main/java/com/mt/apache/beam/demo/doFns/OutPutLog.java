package com.mt.apache.beam.demo.doFns;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.support.ScopeNotActiveException;

@Slf4j
public class OutPutLog extends DoFn<String, String> {

    private final String prefix;

    public OutPutLog(String prefix) {
        this.prefix = prefix;
    }

    public OutPutLog() {
        this.prefix = "processedElement";
    }

    /*@ProcessElement
    public void processElement(@DoFn.Element T element, OutputReceiver<T> out) {
        log.info("{} element: {}",prefix, element);
        out.output(element);
    }*/

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        log.info("{} element: {}",prefix, ctx.element());
    }
}
