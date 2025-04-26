## query template
```java
public SearchResponse search(String indexName, String condition, Object value) throws IOException {
    Specific_QueryBuilder specific_QueryBuilder = QueryBuilders.specific_Query(condition, value);
    // 构建查询语句
    SearchSourceBuilder builder = new SearchSourceBuilder();
    builder.query(specific_QueryBuilder);
    // 根据索引创建查询条件
    SearchRequest request = new SearchRequest(indexName);
    request.source(builder);
    return restHighLevelClient.search(request, RequestOptions.DEFAULT);
}
```

## 精准匹配

### term 单字段精准查询
* 应用于 非text 类型，如关键字、数字、日期等结构化数据
* 针对未分析的字段
* 等值匹配
* select * from indexName where condition = 'value';

### terms 多字段精准查询
* where condition in ('value1', 'value2', ...);

### prefix 前缀查询
* 仅适用于 keyword 类型
* where condition like 'value%';

### wildcard 通配符查询
* 应用于 keyword 类型，未分析的字段
* 使用包含通配符（* ?）的表达式
* 计算负担较高

### range 范围查询
* 应用于数字、日期或其他可排序数据类型
* 区间查询

### exists
* 是否存在某个字段，或字段值是否为空
* 用于筛选具有特定字段值的文档，应用于数据完整性校验、包含特定属性的文档、筛选可选字段

## 全文检索

### match 分词检索
* 检索语句根据分词器分解为独立词项单元，分别进行term检索并bool组合
* 词项单元对大小写不敏感
* 高召回率、结果精准度较低的场景

### matchPhrase 短语匹配检索
* 强调短语的完整性和顺序（连续的短语）
* 待检索文本和原文档关键词在分词后具有相同的顺序
* 有更高的精准度

## 组合查询

### boolQuery 查询
* must, mustNot, should

### boolQuery filter 过滤
* 常用写法：filter下辖must、mustNot
* 可以提高查询效率（没有评分步骤）