package com.simple.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class EsClient {
    private static volatile EsClient instance;

    /**
     * 单例模式
     * 构造方法私有
     */
    private EsClient() { }

    /**
     * 单例模式
     * 懒汉式（双重检查锁）
     *
     * @return instance
     */
    public static EsClient getInstance() {
        if (instance == null) {
            synchronized (EsClient.class) {
                if (instance == null) {
                    instance = new EsClient();
                }
            }
        }
        return instance;
    }

    private volatile RestHighLevelClient client;

    public RestHighLevelClient getClientInstance() {
        if (client == null) {
            synchronized (EsClient.class) {
                if (client == null) {
                    client = new RestHighLevelClient(
                            RestClient.builder(new HttpHost("localhost", 9200, "http"))
                    );
                }
            }
        }
        return client;
    }
}
