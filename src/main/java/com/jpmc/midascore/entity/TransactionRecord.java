package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
public class TransactionRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private float amount;
    
    @ManyToOne
    private UserRecord sender;
    
    @ManyToOne
    private UserRecord recipient;
    
    // Constructors
    public TransactionRecord() {}
    
    public TransactionRecord(UserRecord sender, UserRecord recipient, float amount) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
    }
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public float getAmount() { return amount; }
    public void setAmount(float amount) { this.amount = amount; }
    
    public UserRecord getSender() { return sender; }
    public void setSender(UserRecord sender) { this.sender = sender; }
    
    public UserRecord getRecipient() { return recipient; }
    public void setRecipient(UserRecord recipient) { this.recipient = recipient; }
    private float incentive;

// Add getter and setter
public float getIncentive() {
    return incentive;
}

public void setIncentive(float incentive) {
    this.incentive = incentive;
}

// Update constructor
public TransactionRecord(UserRecord sender, UserRecord recipient, float amount, float incentive) {
    this.sender = sender;
    this.recipient = recipient;
    this.amount = amount;
    this.incentive = incentive;
}
}
