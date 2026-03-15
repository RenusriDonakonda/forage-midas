package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private TransactionRecordRepository transactionRecordRepository;
    
    @Autowired
    private RestTemplate restTemplate;
    
    @Value("${general.incentive-api-url:http://localhost:8080/incentive}")
    private String incentiveApiUrl;

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {
        System.out.println("=========================================");
        System.out.println("Received transaction: " + transaction);
        System.out.println("=========================================");
        
        // Find sender and recipient
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        
        // Validate transaction
        if (sender == null || recipient == null) {
            System.out.println("Invalid sender or recipient");
            return;
        }
        
        if (sender.getBalance() < transaction.getAmount()) {
            System.out.println("Insufficient balance");
            return;
        }
        
        // Call Incentive API
        float incentiveAmount = 0;
        try {
            Incentive incentive = restTemplate.postForObject(incentiveApiUrl, transaction, Incentive.class);
            if (incentive != null) {
                incentiveAmount = incentive.getAmount();
                System.out.println("Incentive received: " + incentiveAmount);
            }
        } catch (Exception e) {
            System.out.println("Failed to get incentive: " + e.getMessage());
            // Continue processing even if incentive API fails
        }
        
        // Process valid transaction
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
        
        // Save updated users
        userRepository.save(sender);
        userRepository.save(recipient);
        
        // Save transaction record with incentive
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRecordRepository.save(transactionRecord);
        
        System.out.println("Transaction processed successfully with incentive: " + incentiveAmount);
        
        // Check specific users if needed
        checkUserBalance("wilbur");
    }
    
    private void checkUserBalance(String userName) {
        Iterable<UserRecord> allUsers = userRepository.findAll();
        for (UserRecord user : allUsers) {
            if (userName.equalsIgnoreCase(user.getName())) {
                System.out.println("\n\n");
                System.out.println("*****************************************************");
                System.out.println("*** CURRENT " + userName.toUpperCase() + " BALANCE: " + user.getBalance() + " ***");
                System.out.println("*** ROUNDED DOWN: " + (int)Math.floor(user.getBalance()) + " ***");
                System.out.println("*****************************************************");
                System.out.println("\n\n");
                break;
            }
        }
    }
}