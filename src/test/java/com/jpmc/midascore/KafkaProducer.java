package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
    private final String topic;
    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public KafkaProducer(@Value("${general.kafka-topic}") String topic, KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        try {
            // Handle both comma-separated and comma+space separated formats
            String[] transactionData = transactionLine.split("\\s*,\\s*");
            if (transactionData.length == 3) {
                long senderId = Long.parseLong(transactionData[0]);
                long recipientId = Long.parseLong(transactionData[1]);
                float amount = Float.parseFloat(transactionData[2]);
                
                Transaction transaction = new Transaction(senderId, recipientId, amount);
                kafkaTemplate.send(topic, transaction);
                System.out.println("Sent transaction: " + transaction);
            } else {
                System.err.println("Invalid transaction format: " + transactionLine);
            }
        } catch (Exception e) {
            System.err.println("Error sending transaction: " + transactionLine);
            e.printStackTrace();
        }
    }
}