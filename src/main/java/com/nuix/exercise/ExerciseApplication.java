package com.nuix.exercise;

import com.nuix.exercise.service.ETLService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.*;

@SpringBootApplication
public class ExerciseApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ExerciseApplication.class);
    @Autowired
    private ETLService etlService;

    public static void main(String[] args) {
        SpringApplication.run(ExerciseApplication.class, args);
    }

    @Override
    public void run(String... args) {
        logger.info("Starting application with arguments " + Arrays.toString(args));
        if (args.length > 0) {
            etlService.process(Arrays.asList(args));
        } else {
            etlService.process(new ArrayList<>());
        }
        logger.info("Terminating application...");
    }
}
