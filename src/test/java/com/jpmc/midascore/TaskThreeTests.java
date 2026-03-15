package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {"trader-updates"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092",
        "auto.create.topics.enable=true"
    }
)
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void task_three_verifier() throws InterruptedException {
        logger.info("=========================================");
        logger.info("Starting TaskThreeTests...");
        logger.info("=========================================");
        
        // Print Kafka broker details
        logger.info("Embedded Kafka brokers: {}", embeddedKafkaBroker.getBrokersAsString());
        
        // Populate users first
        userPopulator.populate();
        logger.info("Users populated");
        
        // Print initial users
        logger.info("=========================================");
        logger.info("Initial users in database:");
        logger.info("=========================================");
        for (UserRecord user : userRepository.findAll()) {
            logger.info("  - {}: Balance = {}", user.getName(), user.getBalance());
        }
        
        // Load and send transactions
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        logger.info("=========================================");
        logger.info("Loaded {} transactions", transactionLines.length);
        logger.info("=========================================");
        
        // Wait a bit for Kafka to be ready
        Thread.sleep(2000);
        
        for (int i = 0; i < transactionLines.length; i++) {
            String transactionLine = transactionLines[i];
            logger.info("Sending transaction {}: {}", i+1, transactionLine);
            kafkaProducer.send(transactionLine);
            Thread.sleep(100);
        }
        
        logger.info("=========================================");
        logger.info("All transactions sent, waiting 10 seconds for processing...");
        logger.info("=========================================");
        
        Thread.sleep(10000);
        
        // Find waldorf's final balance
        logger.info("=========================================");
        logger.info("FINAL WALDORF BALANCE");
        logger.info("=========================================");
        
        for (UserRecord user : userRepository.findAll()) {
            if ("waldorf".equalsIgnoreCase(user.getName())) {
                float balance = user.getBalance();
                int roundedDown = (int)Math.floor(balance);
                
                logger.info("*** WALDORF FINAL BALANCE: {} ***", balance);
                logger.info("*** ROUNDED DOWN: {} ***", roundedDown);
                logger.info("=========================================");
                logger.info("SUBMIT THIS NUMBER: {}", roundedDown);
                logger.info("=========================================");
                break;
            }
        }
        
        logger.info("=========================================");
        logger.info("Test completed");
        logger.info("=========================================");
    }
}