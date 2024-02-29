package com.example.springbatchsimple;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.sql.Timestamp;
import java.time.Instant;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchSimpleApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchSimpleApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner() {

        return new CommandLineRunner() {
            @Autowired
            Job job;
            @Autowired
            JobLauncher jobLauncher;

            @Override
            public void run(String... args) throws Exception {
                var jobParameters = new JobParametersBuilder()
                        .addLong("Times", Instant.now().toEpochMilli())
                        .toJobParameters();
                jobLauncher.run(job,jobParameters);
            }
        };
    }

}


