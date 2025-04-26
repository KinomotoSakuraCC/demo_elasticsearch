package com.simple.news;

import com.simple.client.EsClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DemoNews {
    private final RestHighLevelClient client = EsClient.getInstance().getClientInstance();

    public boolean bulkInsertDoc(String indexName, List<String> docList) throws IOException {
        // 批量请求
        BulkRequest requests = new BulkRequest();
        for (String doc : docList) {
            // 插入文档请求
            IndexRequest request = new IndexRequest();
            // 索引 => 表
            request.index(indexName);
            // id => 主键
            // 文档 => 一行数据
            request.source(doc, XContentType.JSON);

            requests.add(request);
        }
        BulkResponse responses = client.bulk(requests, RequestOptions.DEFAULT);

        return !responses.hasFailures();
    }

    /**
     * term
     * 单字段精准匹配，应用于非text类型 （等值查询，区分大小写及空白）
     * select * from indexName where condition = 'value';
     */
    public SearchResponse search_term(String indexName, String condition, Object value) throws IOException {
        // 单字段精准查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(condition, value);

        // 关闭根据匹配程度进行评分，提高查询性能
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(termQueryBuilder);

        // 构建查询语句
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // builder.query(termQueryBuilder);
        builder.query(constantScoreQueryBuilder);

        // 根据索引创建查询条件
        SearchRequest request = new SearchRequest(indexName);
        request.source(builder);

        return client.search(request, RequestOptions.DEFAULT);
    }

    /**
     * terms
     * 多字段精准匹配
     * select * from indexName where condition in ('value1', 'value2');
     */
    public SearchResponse search_terms(String indexName, String condition, List<Object> valueList) throws IOException {
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(condition, valueList);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(termsQueryBuilder);

        SearchRequest request = new SearchRequest(indexName);
        request.source(builder);

        return client.search(request, RequestOptions.DEFAULT);
    }

    /**
     * prefix
     * 前缀查询，应用于keyword （区分大小写）
     * select * from indexName where condition like 'prefix%';
     */
    public SearchResponse search_prefix(String indexName, String condition, String prefix) throws IOException {
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery(condition, prefix);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(prefixQueryBuilder);

        SearchRequest request = new SearchRequest(indexName);
        request.source(builder);

        return client.search(request, RequestOptions.DEFAULT);
    }

    /**
     *
     * wildcard
     * 通配符查询，使用包含通配符（* ?）的表达式，应用于未分析的字段（keyword），计算负担较高
     * select * from indexName where condition like 'expression';
     */
    public SearchResponse search_wildcard(String indexName, String condition, String expression) throws IOException {
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery(condition, expression);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(wildcardQueryBuilder);

        SearchRequest request = new SearchRequest(indexName);
        request.source(builder);

        return client.search(request, RequestOptions.DEFAULT);
    }

    /**
     * range
     * 范围查询
     * select * from indexName where condition between fromValue and toValue;
     */
    public SearchResponse search_range(String indexName, String condition, Object fromValue, Object toValue) throws IOException {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(condition)
                .gte(fromValue).lte(toValue);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(rangeQueryBuilder);

        SearchRequest request = new SearchRequest(indexName);
        request.source(builder);

        return client.search(request, RequestOptions.DEFAULT);
    }

    /**
     * match 分词检索
     * 检索语句根据分词器分解为独立词项单元，分别进行term检索并bool组合（大小写不敏感）
     * 高召回率、结果精准度较低的场景
     */
    public SearchResponse search_match(String indexName, String condition, String sentence) throws IOException {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(condition, sentence);
        SearchSourceBuilder builder = new SearchSourceBuilder().query(matchQueryBuilder);
        SearchRequest request = new SearchRequest(indexName).source(builder);
        return client.search(request, RequestOptions.DEFAULT);
    }

    /**
     * matchPhrase 短语检索
     */
    public SearchResponse search_matchPhrase(String indexName, String condition, String phrase) throws IOException {
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery(condition, phrase);
        SearchSourceBuilder builder = new SearchSourceBuilder().query(matchPhraseQueryBuilder);
        SearchRequest request = new SearchRequest(indexName).source(builder);
        return client.search(request, RequestOptions.DEFAULT);
    }

    /**
     * bool查询
     * must，mustNot，should
     */
    public SearchResponse searchExample_bool() throws IOException {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.prefixQuery("title.keyword", "Nanjing"))
                .must(QueryBuilders.rangeQuery("popular_degree").gte(30))
                .mustNot(QueryBuilders.termQuery("source_class", "news"))
                .should(QueryBuilders.matchQuery("title", "capital"))
                .should(QueryBuilders.matchPhraseQuery("title", "strong economic"))
                .minimumShouldMatch(1);
        SearchSourceBuilder builder = new SearchSourceBuilder().query(boolQueryBuilder);
        SearchRequest request = new SearchRequest("news_index").source(builder);
        return client.search(request, RequestOptions.DEFAULT);
    }

    /**
     * filter 过滤
     */
    public List<SearchResponse> searchExample_filter() throws IOException {
        List<SearchResponse> responseList = new ArrayList<>();

        // 单独使用filter
        BoolQueryBuilder boolQueryBuilder1 = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("source_class", "weibo"));
        SearchSourceBuilder builder1 = new SearchSourceBuilder().query(boolQueryBuilder1);
        SearchRequest request1 = new SearchRequest("news_index").source(builder1);
        responseList.add(client.search(request1, RequestOptions.DEFAULT));

        // 子查询，与must、mustNot同级别
        BoolQueryBuilder boolQueryBuilder2 = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("popular_degree").gte(50).lte(100))
                .filter(QueryBuilders.termQuery("source_class", "weibo"));
        SearchSourceBuilder builder2 = new SearchSourceBuilder().query(boolQueryBuilder2);
        SearchRequest request2 = new SearchRequest("news_index").source(builder2);
        responseList.add(client.search(request2, RequestOptions.DEFAULT));

        // 常用写法
        // filter 下辖 must、mustNot
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.boolQuery()
                        .must(QueryBuilders.rangeQuery("popular_degree").gte(50).lte(100))
                        .mustNot(QueryBuilders.termQuery("source_class", "weibo"))
                );
        SearchSourceBuilder builder = new SearchSourceBuilder().query(boolQueryBuilder);
        SearchRequest request = new SearchRequest("news_index").source(builder);
        responseList.add(client.search(request, RequestOptions.DEFAULT));

        return responseList;
    }
}
