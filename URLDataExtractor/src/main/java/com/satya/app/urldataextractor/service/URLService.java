package com.satya.app.urldataextractor.service;

import com.satya.app.urldataextractor.dao.URLRepository;
import com.satya.app.urldataextractor.model.URL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class URLService {
    private static Logger LOG=LoggerFactory.getLogger(URLService.class);
    @Value("#{${com.satya.app.urldataextractor.topics}}")
    Map<String,String> kafkaTopics;
    @Value("${com.satya.app.urldataextractor.service.cooldown}")
    private Integer cooldown;

    @Autowired
    private KafkaService kafkaService;
    @Autowired
    private URLRepository urlRepository;
    /*
    public void save(URL url){

        try {
            Optional<String> opt=getTopicByPath(url.getUrl());
            if(opt.isEmpty())
            {
                LOG.warn("Content type not found for URL:{}",url.getUrl());
                return;
            }
            String topic=opt.get();
            LOG.info("URL:{},sending to topic:{}",url.getUrl(),topic);
            //urlRepository.save(url);
            //kafkaService.send("html_file", url.getUrl());
            //urlRepository.save(url);
        }
        catch(IOException ex)
        {
            LOG.error("Exception:{}",ex);
        }
    }

    private Optional<String> getTopicByPath(String path) throws IOException{
        java.net.URL url=new java.net.URL(path);
        HttpURLConnection connection=(HttpURLConnection) url.openConnection();
        connection.setRequestMethod("HEAD");
        connection.connect();
        String rawContentType=connection.getContentType();
        LOG.info("URL:{} has content Type:{}",path,rawContentType);
        String contentType=rawContentType.split(";")[0];
        LOG.info("Key:{}",contentType);
        if(kafkaTopics.containsKey(contentType))
        {
            return Optional.of(kafkaTopics.get(contentType));
        }
        LOG.warn("Content type not configured for URL:{},Content Type:{}",path,rawContentType);
        return Optional.empty();
    }
     */
    /*
    @Autowired
    private CacheService cacheService;
   @Async
    public void save(Set<URL> urls) {

       for (URL url : urls) {
           try {
               LOG.info("--------- {}", Thread.currentThread().getName());
               if(cacheService.get(url.getUrl())!=null)
               {
                   return;
               }
               //URL existingURL = urlRepository.findByURL(url.getUrl());//for sql
               Optional<URL> existingURLOpt = urlRepository.findByUrl(url.getUrl());//for cassandra
               Optional<String> optContentType = Optional.empty();
               //if (existingURL != null) //for sql
                   if(!existingURLOpt.isEmpty())//for cassandra
               {URL existingURL=existingURLOpt.get();//for cassandra
                   // we are going to allow processing if the URL has been processed more than 7 days ago
                   if (existingURL.getLastProcessed().getTime() + TimeUnit.DAYS.toMillis(cooldown) > System.currentTimeMillis()) {
                       LOG.info("URL {} already processed on {}", existingURL.getUrl(), existingURL.getLastProcessed());
                       cacheService.set(existingURL);
                       return;
                   }
                   url = existingURL;
                   optContentType = Optional.of(existingURL.getContentType());
               }
               url.setLastProcessed(new Timestamp(System.currentTimeMillis()));
               url.setTimesProcessed(url.getTimesProcessed() + 1);
               if (optContentType.isEmpty()) {
                   optContentType = getContentType(url.getUrl());
               }
               if (optContentType.isEmpty()) {
                   LOG.warn("Content type not found for URL: {}", url.getUrl());
                   return;
               }
               Optional<String> optTopic = getTopicByContentType(optContentType.get());
               if (optTopic.isEmpty()) {
                   LOG.warn("Content type {} not mapped", optContentType.get());
                   return;
               }
               String topic = optTopic.get();
               if (url.getContentType() == null || url.getContentType().isEmpty()) {
                   url.setContentType(optContentType.get());
               }
               LOG.info("URL: {}, sending to topic: {}", url.getUrl(), topic);
               //kafkaService.send(topic, url.getUrl());
               cacheService.set(url);
               LOG.info("Saving URL: {}", url);
               urlRepository.save(url);
           } catch (IOException ex) {
               LOG.error("Exception: ", ex);
           }
       }
   }
    private Optional<String> getContentType(String path) throws IOException {
        java.net.URL url = new java.net.URL(path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("HEAD");
        connection.connect();
        return Optional.of(connection.getContentType());
    }

    private Optional<String> getTopicByContentType(String rawContentType) {
        String contentType = rawContentType.split(";")[0];
        LOG.info("Key: {}", contentType);
        if (kafkaTopics.containsKey(contentType)) {
            return Optional.of(kafkaTopics.get(contentType));
        }
        return Optional.empty();
    }
}
     */
    @Autowired
    private CacheService cacheService;
    public Optional<URL> get(String id) {
        return urlRepository.findById(id);
    }
    @Async
    public void save(Set<URL> urls) {
        for(URL url : urls) {
            try {
                LOG.info("--------- {}", Thread.currentThread().getName());
                if (cacheService.get(url.getUrl()) != null) {
                    return;
                }
                Optional<URL> existingURLOpt = urlRepository.findByUrl(url.getUrl());
                Optional<String> optContentType = Optional.empty();
                if (!existingURLOpt.isEmpty()) {
                    URL existingURL = existingURLOpt.get();
                    // we are going to allow processing if the URL has been processed more than 7 days ago
                    if (existingURL.getLastProcessed().getTime() + TimeUnit.DAYS.toMillis(cooldown) > System.currentTimeMillis()) {
                        LOG.info("URL {} already processed on {}", existingURL.getUrl(), existingURL.getLastProcessed());
                        cacheService.set(existingURL);
                        return;
                    }
                    url = existingURL;
                    optContentType = Optional.of(existingURL.getContentType());
                }
                url.setLastProcessed(new Timestamp(System.currentTimeMillis()));
                url.setTimesProcessed(url.getTimesProcessed() + 1);
                if (optContentType.isEmpty()) {
                    optContentType = getContentType(url.getUrl());
                }
                if (optContentType.isEmpty()) {
                    LOG.warn("Content type not found for URL: {}", url.getUrl());
                    return;
                }
                Optional<String> optTopic = getTopicByContentType(optContentType.get());
                if (optTopic.isEmpty()) {
                    LOG.warn("Content type {} not mapped", optContentType.get());
                    return;
                }
                String topic = optTopic.get();
                if (url.getContentType() == null || url.getContentType().isEmpty()) {
                    url.setContentType(optContentType.get());
                }
                LOG.info("URL: {}, sending to topic: {}", url.getUrl(), topic);
              //kafkaService.send(topic, url.getUrl());
                kafkaService.send(topic, url);

                cacheService.set(url);
                urlRepository.save(url);
            } catch (IOException ex) {
                LOG.error("Exception: ", ex);
            }
        }
    }

    private Optional<String> getContentType(String path) throws IOException {
        /*
        java.net.URL url = new java.net.URL(path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("HEAD");
        connection.connect();
        return Optional.of(connection.getContentType());

         */
        try {
            java.net.URL url = new java.net.URL(path);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.connect();
            return Optional.of(connection.getContentType());
        } catch (IOException e) {
            // Log the exception message
            e.printStackTrace();
            return Optional.empty();
        }
    }

    private Optional<String> getTopicByContentType(String rawContentType) {
        String contentType = rawContentType.split(";")[0];
        LOG.info("Key: {}", contentType);
        if (kafkaTopics.containsKey(contentType)) {
            return Optional.of(kafkaTopics.get(contentType));
        }
        return Optional.empty();
    }
}


