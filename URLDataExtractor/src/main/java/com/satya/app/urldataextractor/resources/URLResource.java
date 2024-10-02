package com.satya.app.urldataextractor.resources;
import com.satya.app.urldataextractor.model.URL;
import com.satya.app.urldataextractor.common.Constants;
import com.satya.app.urldataextractor.service.URLService;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.sql.Timestamp;



@RestController
public class URLResource {
    private static final Logger LOG= LoggerFactory.getLogger(URLResource.class);
@Autowired
private URLService urlService;
    @GetMapping("/ping")
    public String ping()
    {
//        URL u=new URL();
//        u.
        return "pong from urldataextractor";
    }
    @GetMapping("/{id}")
    public ResponseEntity<URL> get(@PathVariable String id) {
        Optional<URL> opt = urlService.get(id);
        if (opt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(opt.get());
    }
    @PostMapping("/batch")
    public ResponseEntity<Void> submitBatchURL(@RequestBody Set<URL> urls) {
        long startTime = System.currentTimeMillis();
        LOG.info("Batch request received: {}", urls);
        urls.forEach(u -> {
            u.setId(Constants.URL_UUID_PREFIX + UUID.randomUUID().toString());
            u.setCreatedDate(new Timestamp(System.currentTimeMillis()));
            u.setTimesProcessed(0);
        });
        urlService.save(urls);
        LOG.info("Request processed in {} mills", (System.currentTimeMillis() - startTime));
        return ResponseEntity.ok().build();
    }

    /*
    @PostMapping
    public ResponseEntity<Void> submitURL(@RequestBody String url) {
        System.out.println(url);
        return ResponseEntity.ok().build();
    }
     */
    @PostMapping
    public ResponseEntity<URL> submitURL(@RequestBody URL url) {
        long startTime=System.currentTimeMillis();
        url.setId(Constants.URL_UUID_PREFIX +UUID.randomUUID().toString());
        url.setCreatedDate(new Timestamp(System.currentTimeMillis()));
        url.setTimesProcessed(0);
       // urlService.save(url);
        LOG.info("URL received:{}",url);
        //System.out.println(url);
        urlService.save(new HashSet<>(){{
            add(url);
        }});
LOG.info("Request processed in {} mills",(System.currentTimeMillis()-startTime));
       // return ResponseEntity.ok().build(); //public ResponseEntity<Void> submitURL(@RequestBody URL url) {
        return ResponseEntity.ok(url); //public ResponseEntity<URL> submitURL(@RequestBody URL url) {
    }

}
