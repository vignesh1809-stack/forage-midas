package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

@Component
public class KafkaListenerComponent {
    private List<Float> transactionAmounts = new ArrayList<>();

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction){
        System.out.println("Received transaction: " + transaction);
        transactionAmounts.add(transaction.getAmount());
        if (transactionAmounts.size() <= 4) {
            System.out.println("Transaction #" + transactionAmounts.size() + " amount: " + transaction.getAmount());
        }
    }
    
    public List<Float> getTransactionAmounts() {
        return transactionAmounts;
    }
}
