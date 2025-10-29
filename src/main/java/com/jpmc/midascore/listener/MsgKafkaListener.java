package com.jpmc.midascore.listener;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class MsgKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(MsgKafkaListener.class);

    @Autowired
    private RestTemplate restTemplate;

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

        Incentive incentive = getIncentive(transaction);
        double incentiveAmount = incentive != null && incentive.getAmount() != null ? incentive.getAmount().doubleValue() : 0;
        log.debug("Incentive amount calculated: ",incentiveAmount);

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());
        recipient.setBalance((float)(recipient.getBalance() + transaction.getAmount() + incentiveAmount));

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord transactionRecord = new TransactionRecord(sender,recipient,transaction.getAmount());
        transactionRepository.save(transactionRecord);

        log.debug("Balance updated successfully for sender: " + sender.getName() + " : " + sender.getBalance());
        log.debug("Balance updated successfully for recipient: " + recipient.getName() + " : " + recipient.getBalance());
        log.debug("Balance updated - Sender: {} ({}), Recipient: {} ({}) [Incentive: {}]",
                sender.getName(),sender.getBalance(),recipient.getName(),recipient.getBalance(),
                recipient.getBalance(), recipient.getBalance(), incentiveAmount);
    }

    private Incentive getIncentive(Transaction transaction){
        try{
            String url = "http://localhost:8081/incentive";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Transaction> request = new HttpEntity<>(transaction, headers);

            Incentive incentive = restTemplate.postForObject(url, request, Incentive.class);

            log.debug("Incentive created successfully for sender: ", incentive != null ? incentive.getAmount() : "null");
            return incentive;
        } catch (Exception e){
            log.error("Incentive not found ", transaction, e);
            return null;
        }
    }
}
