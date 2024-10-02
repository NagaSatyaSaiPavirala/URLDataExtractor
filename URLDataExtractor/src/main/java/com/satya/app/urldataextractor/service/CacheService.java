package com.satya.app.urldataextractor.service;

import com.satya.app.urldataextractor.codec.URLSerializationCodec;
import com.satya.app.urldataextractor.model.URL;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class CacheService {
    private static final Logger LOG= LoggerFactory.getLogger(CacheService.class);
    @Value("${com.satya.app.urldataextractor.service.cache.ttl}")
    private Integer ttl;
    @Value("${com.satya.app.urldataextractor.cache.url}")
    private String url;
    private RedisClient redisClient=null;
    private StatefulRedisConnection<String, URL> statefulRedisConnection=null;
    public URL get(String key)
    {
        URL url=statefulRedisConnection.sync().get(key);
        if(url!=null)
        {
            LOG.info("Serving from cache,for key:{}!",key);
        }
        else
        {
            LOG.info("Cache miss.for the key:{}!",key);
        }
        return url;
    }
    public void set(URL url)
    {
        //Long ttlSeconds=30L;
        //Long ttlSeconds= TimeUnit.DAYS.toSeconds(this.ttl);//for sql
        long ttlSeconds= TimeUnit.DAYS.toSeconds(this.ttl);//for cassandra
        statefulRedisConnection.sync().setex(url.getUrl(),ttlSeconds,url);
    }
    @PostConstruct
    private void init()
    {
        LOG.info("Post init called");
        redisClient= RedisClient.create(url);
        statefulRedisConnection=redisClient.connect(new URLSerializationCodec());

    }
    @PreDestroy
    private void destroy()
    {
if(statefulRedisConnection!=null)
{
    statefulRedisConnection.close();
}
if(redisClient!=null)
{
    redisClient.shutdown();
}
    }
}
