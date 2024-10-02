package com.satya.app.HTMLFileWorker.resource;


import com.satya.app.HTMLFileWorker.model.PageInfo;
import com.satya.app.HTMLFileWorker.model.URL;
import com.satya.app.HTMLFileWorker.service.URLProcessor;
import com.satya.app.HTMLFileWorker.service.URLService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;
import java.util.UUID;

@RestController
public class HTMLURLResource {

    private static final Logger LOG = LoggerFactory.getLogger(HTMLURLResource.class);

    @Autowired
    private URLProcessor urlProcessor;

    @Autowired
    private URLService urlService;

    @GetMapping("/ping")
    public String ping() {
        return "pong from htmlworker";
    }

    @GetMapping("/{urlId}")
    public ResponseEntity<PageInfo> get(@PathVariable String urlId) {
        Optional<PageInfo> opt = urlService.get(urlId);
        if (opt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(opt.get());
    }

    @PostMapping
    public ResponseEntity<Void> submitURL(@RequestBody String url) {
        LOG.info("URL received: {}", url);
        urlProcessor.process(url, UUID.randomUUID().toString());
        return ResponseEntity.ok().build();
    }

    /*
@PostMapping
public ResponseEntity<Void> submitURL(@RequestBody String url) {
    LOG.info("URL received: {}", url);
    urlProcessor.process(url, UUID.randomUUID().toString());
    return ResponseEntity.ok().build();
}
     */
}
