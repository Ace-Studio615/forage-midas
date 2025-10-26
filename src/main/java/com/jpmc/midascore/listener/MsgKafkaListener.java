package com.jpmc.midascore.listener;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MsgKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(MsgKafkaListener.class);

    @KafkaListener(topics="${general.kafka-topic}")
    public void listening(Transaction transaction){
        log.info("Received Transaction: {}", transaction);

        log.debug("Transaction details - SenderId: {}, RecipientId: {}, Amount: {}",
                transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount());
    }
}
