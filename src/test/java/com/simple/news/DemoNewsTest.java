package com.simple.news;

import com.alibaba.fastjson2.JSON;
import com.simple.model.News;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class DemoNewsTest {
    private DemoNews demoNews;

    @Before
    public void setUp() throws Exception {
        demoNews = new DemoNews();
    }

    @After
    public void tearDown() throws Exception {
        demoNews = null;
    }

    @Test
    public void prepareDate() throws IOException {
        String indexName = "news_index";

        ArrayList<String> newsList = new ArrayList<>();
        newsList.add(JSON.toJSONString(new News("Nanjing is the capital of Jiangsu Province", 100, "wechat")));
        newsList.add(JSON.toJSONString(new News("Nanjing is located in southeastern China", 10, "blog")));
        newsList.add(JSON.toJSONString(new News("Nanjing was once the capital of the Six Dynasties period", 80, "news")));
        newsList.add(JSON.toJSONString(new News("There are many famous tourist attractions in Nanjing", 80, "weibo")));
        newsList.add(JSON.toJSONString(new News("Nanjing has strong economic strength and rapid development", 30, "weibo")));

        boolean success = demoNews.bulkInsertDoc(indexName, newsList);
        Assert.assertTrue(success);
    }

    private void printDocsSource(SearchResponse response) {
        response.getHits().forEach(hit -> {
            System.out.println(hit.getSourceAsMap());
        });
        System.out.println();
    }

    @Test
    public void search_term() throws IOException {
        SearchResponse response;
        response = demoNews.search_term("news_index", "source_class", "weibo");
        printDocsSource(response);

        response = demoNews.search_term("news_index", "popular_degree", 80);
        printDocsSource(response);
    }

    @Test
    public void search_terms() throws IOException {
        SearchResponse response;
        response = demoNews.search_terms("news_index", "source_class", Arrays.asList("weibo", "wechat"));
        printDocsSource(response);
    }

    @Test
    public void search_prefix() throws IOException {
        SearchResponse response;
        response = demoNews.search_prefix("news_index", "title.keyword", "Nanjing");
        printDocsSource(response);
    }

    @Test
    public void search_wildcard() throws IOException {
        SearchResponse response;
        response = demoNews.search_wildcard("news_index", "title.keyword", "Nanjing*China");
        printDocsSource(response);
    }

    @Test
    public void search_range() throws IOException {
        SearchResponse response;
        response = demoNews.search_range("news_index", "popular_degree", 30, 80);
        printDocsSource(response);
    }

    @Test
    public void search_match() throws IOException {
        SearchResponse response;
        response = demoNews.search_match("news_index", "title", " in is");
        printDocsSource(response);
    }

    @Test
    public void search_matchPhrase() throws IOException {
        SearchResponse response;
        response = demoNews.search_matchPhrase("news_index", "title", "is the");
        printDocsSource(response);
    }

    @Test
    public void searchExample_bool() throws IOException {
        printDocsSource(demoNews.searchExample_bool());
    }

    @Test
    public void searchExample_filter() throws IOException {
        demoNews.searchExample_filter().forEach(this::printDocsSource);
    }
}