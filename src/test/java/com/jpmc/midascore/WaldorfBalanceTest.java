package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Testcontainers
@SpringBootTest
public class WaldorfBalanceTest {
    static final Logger logger = LoggerFactory.getLogger(WaldorfBalanceTest.class);

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));

    private static String bootstrapServers;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        bootstrapServers = kafka.getBootstrapServers();
        logger.info("=========================================");
        logger.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        logger.info("=========================================");
    }

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private KafkaTemplate<String, Transaction> kafkaTemplate;

    @Test
    void findWaldorfBalance() throws Exception {
        logger.info("=========================================");
        logger.info("FINDING WALDORF'S BALANCE");
        logger.info("=========================================");
        
        // Create topic manually
        createTopic("trader-updates");
        
        // Wait for topic to be created
        Thread.sleep(5000);
        
        // Populate users
        userPopulator.populate();
        logger.info("Users populated");
        
        // Show initial users
        for (UserRecord user : userRepository.findAll()) {
            logger.info("User: {} = {}", user.getName(), user.getBalance());
        }
        
        // Load transactions
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        logger.info("Loaded {} transactions", transactionLines.length);
        
        // Send each transaction
        for (int i = 0; i < transactionLines.length; i++) {
            String line = transactionLines[i];
            String[] parts = line.split(",");
            
            long senderId = Long.parseLong(parts[0].trim());
            long recipientId = Long.parseLong(parts[1].trim());
            float amount = Float.parseFloat(parts[2].trim());
            
            Transaction transaction = new Transaction(senderId, recipientId, amount);
            logger.info("Sending transaction {}: {}", i+1, line);
            
            try {
                kafkaTemplate.send("trader-updates", transaction).get(10, TimeUnit.SECONDS);
                logger.info("Transaction {} sent successfully", i+1);
            } catch (Exception e) {
                logger.error("Failed to send transaction: {}", e.getMessage());
                throw e;
            }
            
            Thread.sleep(200);
        }
        
        logger.info("All transactions sent, waiting for processing...");
        Thread.sleep(15000);
        
        // Find final waldorf balance
        logger.info("=========================================");
        logger.info("FINAL RESULT");
        logger.info("=========================================");
        
        boolean found = false;
        for (UserRecord user : userRepository.findAll()) {
            if ("waldorf".equalsIgnoreCase(user.getName())) {
                float balance = user.getBalance();
                int roundedDown = (int)Math.floor(balance);
                
                logger.info("*** WALDORF'S FINAL BALANCE: {} ***", balance);
                logger.info("*** ROUNDED DOWN: {} ***", roundedDown);
                logger.info("=========================================");
                logger.info("SUBMIT THIS NUMBER: {}", roundedDown);
                logger.info("=========================================");
                found = true;
                break;
            }
        }
        
        if (!found) {
            logger.error("WALDORF NOT FOUND IN DATABASE!");
        }
    }
    
    private void createTopic(String topicName) {
        try {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            
            try (AdminClient admin = AdminClient.create(config)) {
                NewTopic topic = new NewTopic(topicName, 1, (short) 1);
                admin.createTopics(java.util.Collections.singleton(topic)).all().get();
                logger.info("Created topic: {}", topicName);
            }
        } catch (Exception e) {
            logger.error("Failed to create topic: {}", e.getMessage());
        }
    }
}