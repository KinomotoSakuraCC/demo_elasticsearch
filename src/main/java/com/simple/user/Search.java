package com.simple.user;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Search extends Client {
    public List<String> search_all(String index) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(index);
        // builder all
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // hits
        List<String> resultList = new ArrayList<>();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            resultList.add(hit.getSourceAsString());
        }
        return resultList;
    }

    public List<String> search_condition(String index, String name, Object value, int from, int size, String sortField, String sortOrder) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(index);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        // condition query
        builder.query(QueryBuilders.matchQuery(name, value));
        // pages query, from = (currentPage - 1) * size
        builder.from(from);
        builder.size(size);
        // sort query
        if (StringUtils.isNotEmpty(sortField)) {
            if (StringUtils.isEmpty(sortOrder)) {
                // default ascending order
                builder.sort(sortField);
            } else if (SortOrder.ASC.toString().equals(sortOrder)) {
                builder.sort(sortField, SortOrder.ASC);
            } else if (SortOrder.DESC.toString().equals(sortOrder)) {
                builder.sort(sortField, SortOrder.DESC);
            }
        }

        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // hits
        List<String> resultList = new ArrayList<>();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            resultList.add(hit.getSourceAsString());
        }
        return resultList;
    }

    public List<String> search_bool(String index, Map<String, Object> conditionMap) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices(index);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("age", 20));

        SearchSourceBuilder builder = new SearchSourceBuilder().query(boolQueryBuilder);
        conditionMap.forEach((k, v) -> {
            boolQueryBuilder.must(QueryBuilders.matchQuery(k, v));
        });

        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // hits
        List<String> resultList = new ArrayList<>();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            resultList.add(hit.getSourceAsString());
        }
        return resultList;
    }
}
