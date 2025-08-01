package com.mt.apache.beam.demo.doFns;

import org.apache.beam.sdk.transforms.DoFn;

public class WordCountDoFn extends DoFn<String,String> {

    @ProcessElement
    public void processElement(@DoFn.Element String element, OutputReceiver<String> out) {

        String[] words = element.split(" ");
        for (String word : words) {
            if (!word.isEmpty()) {
                out.output(word);
            }
        }
    }
}
