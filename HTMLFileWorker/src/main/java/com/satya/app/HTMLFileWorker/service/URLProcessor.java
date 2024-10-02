package com.satya.app.HTMLFileWorker.service;

import com.satya.app.HTMLFileWorker.client.URLFeederService;
import com.satya.app.HTMLFileWorker.model.PageInfo;
import com.satya.app.HTMLFileWorker.dao.PageRepository;
import com.satya.app.HTMLFileWorker.model.URL;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.IOException;
import java.util.*;

@Service

public class URLProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(URLProcessor.class);
    @Autowired
    private Retrofit retrofit;
    @Autowired
    private PageRepository pageRepository;
    /*
    @Async
    public void process(String url) {
        try {
            Set<URL> urls = new HashSet<>();
            Document doc = Jsoup.connect(url).get();
            Elements links = doc.select("a[href]");
            for (Element link : links) {
                //LOG.info("Extracted: {}", link.attr("href"));
                String path = link.attr("href");
                LOG.info("Extracted: {}", path);
                urls.add(URL.builder().url(path).build());
            }
            Response<Void> response = retrofit.create(URLFeederService.class).batchPublish(urls).execute();
            if (!response.isSuccessful())//if successful 2xx
            {
                LOG.info("Retrofit called failed, with response code {}", response.code());
            }
        } catch (IOException ex) {
            LOG.error("Exception: ", ex);
        }
    }
     */
    /*
    @Async
    public void process(String url) {
        try {
            Document doc = Jsoup.connect(url).get();
            Set<URL> urls = fetchAnchorTags(doc);
            PageInfo pageInfo = fetchMetaInformation(doc);
            pageInfo.setUrlId(UUID.randomUUID().toString());
            pageInfo.setUrl(url);
            LOG.info("PageInfo: {}", pageInfo);
            pageRepository.save(pageInfo);
//            Response<Void> response = retrofit.create(URLFeederService.class).batchPublish(urls).execute();
//            if (!response.isSuccessful()) {
//                LOG.info("Retrofit called failed, with response code {}", response.code());
//            }
        } catch (IOException ex) {
            LOG.error("Exception: ", ex);
        }
    }

     */

    @Async
    public void process(String url, String urlId) {
        try {
            Document doc = Jsoup.connect(url).get();
            Set<URL> urls = fetchAnchorTags(doc);
            PageInfo pageInfo = fetchMetaInformation(doc);
            pageInfo.setUrlId(urlId);
            pageInfo.setUrl(url);
            LOG.info("PageInfo: {}", pageInfo);
            pageRepository.save(pageInfo);
//            Response<Void> response = retrofit.create(URLFeederService.class).batchPublish(urls).execute();
//            if (!response.isSuccessful()) {
//                LOG.info("Retrofit called failed, with response code {}", response.code());
//            }
        } catch (IOException ex) {
            LOG.error("Exception: ", ex);
        }
    }



    private PageInfo fetchMetaInformation(Document doc) {
        return PageInfo.builder()
                .id(UUID.randomUUID().toString())
                .title(doc.title())
                .description(description(doc))
                .body(body(doc))
                .keywords(keywords(doc))
                .createdTime(new Date())
                .build();
    }

    private String body(Document doc) {
        return doc.body().text();
    }

    private String description(Document doc) {
        Elements ele = doc.select("meta[name=description]");
        if (!ele.isEmpty()) {
            return ele.first().attr("content");
        }
        return null;
    }

    private List<String> keywords(Document doc) {
        Elements ele = doc.select("meta[name=keywords]");
        if(!ele.isEmpty()) {
            String keywords = ele.first().attr("content");
            return Arrays.asList(keywords.split(","));
        }
        return Collections.emptyList();
    }

    public Set<URL> fetchAnchorTags(Document doc) {
        Set<URL> urls = new HashSet<>();
        Elements links = doc.select("a[href]");
        for (Element link : links) {
            String path = link.attr("href");
            urls.add(URL.builder().url(path).build());
        }
        return urls;
    }
}
