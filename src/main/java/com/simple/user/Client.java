package com.simple.user;

import com.simple.client.EsClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Client {
    protected RestHighLevelClient client = EsClient.getInstance().getClientInstance();
}
