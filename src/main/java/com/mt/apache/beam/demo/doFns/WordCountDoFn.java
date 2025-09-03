package com.mt.apache.beam.demo.doFns;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.Objects;

public class WordCountDoFn extends DoFn<String,String> {

    @ProcessElement
    public void processElement(ProcessContext context) {

        if(context.element() != null ) {
            String[] words = Objects.requireNonNull(context.element()).split(" ");
            for (String word : words) {
                if (!word.isEmpty()) {
                    context.output(word);
                }
            }
        }
    }
}
