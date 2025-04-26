package com.simple.user;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class Doc extends Client {
    public String index_insertDoc(String index, String json) throws IOException {
        IndexRequest request = new IndexRequest();
        request.index(index)
                .source(json, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        return response.getId();
    }

    public String delete_deleteDoc(String index, String id) throws IOException {
        DeleteRequest request = new DeleteRequest();
        request.index(index)
                .id(id);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        return response.toString();
    }

    public String update_updateDoc(String index, String id, String fieldName, Object fieldValue) throws IOException {
        UpdateRequest request = new UpdateRequest();
        request.index(index)
                .id(id)
                .doc(XContentType.JSON, fieldName, fieldValue);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        return response.toString();
    }

    public String get_getDoc(String index, String id) throws IOException {
        GetRequest request = new GetRequest();
        request.index(index)
                .id(id);
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        return response.getSourceAsString();
    }
}
