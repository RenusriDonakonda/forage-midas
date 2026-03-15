package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Testcontainers
@SpringBootTest
public class DirectKafkaTest {
    static final Logger logger = LoggerFactory.getLogger(DirectKafkaTest.class);

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

    @Test
    void findWaldorfBalance() throws Exception {
        logger.info("=========================================");
        logger.info("DIRECT KAFKA TEST - FINDING WALDORF'S BALANCE");
        logger.info("=========================================");
        
        // Create topic
        createTopic("trader-updates");
        Thread.sleep(5000);
        
        // Populate users
        userPopulator.populate();
        logger.info("Users populated");
        
        // Show initial users
        logger.info("Initial users:");
        for (UserRecord user : userRepository.findAll()) {
            logger.info("  {}: {}", user.getName(), user.getBalance());
        }
        
        // Create Kafka producer directly
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(producerProps);
        
        // Load transactions
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        logger.info("Loaded {} transactions", transactionLines.length);
        
        // Send each transaction - without using lambda that needs final variables
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < transactionLines.length; i++) {
            final int transactionNumber = i + 1; // Make it effectively final
            String line = transactionLines[i];
            String[] parts = line.split(",");
            
            long senderId = Long.parseLong(parts[0].trim());
            long recipientId = Long.parseLong(parts[1].trim());
            float amount = Float.parseFloat(parts[2].trim());
            
            Transaction transaction = new Transaction(senderId, recipientId, amount);
            logger.info("Sending transaction {}: {}", transactionNumber, line);
            
            ProducerRecord<String, Transaction> record = new ProducerRecord<>("trader-updates", transaction);
            
            // Send synchronously to avoid lambda issues
            try {
                producer.send(record).get();
                successCount.incrementAndGet();
                logger.info("Transaction {} sent successfully", transactionNumber);
            } catch (Exception e) {
                logger.error("Failed to send transaction {}: {}", transactionNumber, e.getMessage());
            }
            
            Thread.sleep(200);
        }
        
        producer.close();
        
        logger.info("All transactions sent. Successful: {}/{}", successCount.get(), transactionLines.length);
        
        // Wait for processing
        logger.info("Waiting for transactions to be processed...");
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
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            
            try (AdminClient admin = AdminClient.create(props)) {
                NewTopic topic = new NewTopic(topicName, 1, (short) 1);
                admin.createTopics(Collections.singleton(topic)).all().get();
                logger.info("Created topic: {}", topicName);
            }
        } catch (Exception e) {
            logger.error("Failed to create topic: {}", e.getMessage());
        }
    }
}