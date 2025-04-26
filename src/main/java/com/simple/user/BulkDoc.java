package com.simple.user;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BulkDoc extends Client {
    public List<String> bulkIndex(String index, List<String> jsonList) throws IOException {
        BulkRequest requests = new BulkRequest();
        for (String json : jsonList) {
            IndexRequest request = new IndexRequest();
            request.index(index)
                    .source(json, XContentType.JSON);
            requests.add(request);
        }
        BulkResponse responses = client.bulk(requests, RequestOptions.DEFAULT);
        return Arrays.stream(responses.getItems())
                .map(BulkItemResponse::getId)
                .collect(Collectors.toList());
    }

    public List<String> bulkDelete(String index, List<String> idList) throws IOException {
        BulkRequest requests = new BulkRequest();
        for (String id : idList) {
            DeleteRequest request = new DeleteRequest();
            request.index(index)
                    .id(id);
            requests.add(request);
        }
        BulkResponse responses = client.bulk(requests, RequestOptions.DEFAULT);
        return Arrays.stream(responses.getItems())
                .map(item -> item.getResponse().toString())
                .collect(Collectors.toList());
    }
}
