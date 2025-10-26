package com.jpmc.midascore.listener;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MsgKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(MsgKafkaListener.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics="${general.kafka-topic}")
    @Transactional
    public void listening(Transaction transaction){
        log.info("Received Transaction: {}", transaction);

        log.debug("Transaction details - SenderId: {}, RecipientId: {}, Amount: {}",
                transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount());
        if(isValidTransaction(transaction)){
            processTransaction(transaction);
            log.info("Transaction processed successfully");
        }else{
            log.warn("Transaction validation failed", transaction);
        }
    }

    private boolean isValidTransaction(Transaction transaction){
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if(sender == null){
            log.debug("Sender not found. Invalid Id: {}", transaction.getSenderId());
            return false;
        }

        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if(recipient == null){
            log.debug("Recipient not found. Invalid Id: {}", transaction.getRecipientId());
            return false;
        }

        if(sender.getBalance() < transaction.getAmount()){
            log.debug("Sender's balance is less than transaction's amount");
            return false;
        }
        return true;
    }

    private void processTransaction(Transaction transaction){
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord transactionRecord = new TransactionRecord(sender,recipient,transaction.getAmount());
        transactionRepository.save(transactionRecord);

        log.debug("Balance updated successfully for sender: " + sender.getName() + " : " + sender.getBalance());
        log.debug("Balance updated successfully for recipient: " + recipient.getName() + " : " + recipient.getBalance());
    }
}
