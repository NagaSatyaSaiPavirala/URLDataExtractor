package com.satya.app.HTMLFileWorker.listener;

import com.satya.app.HTMLFileWorker.model.URL;
import com.satya.app.HTMLFileWorker.service.URLProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class HTMLWorker {
    private static final Logger LOG=LoggerFactory.getLogger(HTMLWorker.class);
    @Autowired
    private URLProcessor urlProcessor;
    /*
    @KafkaListener(topics="html_file", groupId = "group_id")
    public void consume(String message)
    {
        //System.out.println("Received message:"+message);
        LOG.info("Received message:{}",message);
        urlProcessor.process(message);

    }

     */
    @KafkaListener(topics = "html_topic", groupId = "group_id", containerFactory = "kafkaListenerContainerFactory")
    public void consume(@Payload URL message, @Headers MessageHeaders headers) {
        LOG.info("Received message: {}, headers: {}", message, headers);
        urlProcessor.process(message.getUrl(), message.getId());
    }
}
