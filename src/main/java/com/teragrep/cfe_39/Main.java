package com.teragrep.cfe_39;

import com.teragrep.cfe_39.consumers.kafka.KafkaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config config = null;
        try {
            config = new Config();
        } catch (IOException e){
            System.out.println("Can't load config: " + e);
            System.exit(1);
        } catch (IllegalArgumentException e) {
            System.out.println("Got invalid config: " + e);
            System.exit(1);
        }
        LOGGER.info("Running main program");
        KafkaController kafkaController = new KafkaController(config);
        kafkaController.run();
    }
}
