package com.mt.apache.beam.demo;

import com.mt.apache.beam.demo.services.StreamingWordCountService;
import com.mt.apache.beam.demo.services.ToUppercaseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.CompletableFuture;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class DemoApplication implements CommandLineRunner {

    private final ToUppercaseService toUppercaseService;
    private final StreamingWordCountService streamingWordCountService;

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Override
    public void run(String... args) {
        CompletableFuture.runAsync(toUppercaseService::toUppercase);
        CompletableFuture.runAsync(streamingWordCountService::streamingWordCount);
    }
}
