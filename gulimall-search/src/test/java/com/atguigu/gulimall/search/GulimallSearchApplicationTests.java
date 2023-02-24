package com.atguigu.gulimall.search;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gulimall.search.config.GulimallElasticSearchConfig;
import lombok.Data;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;


@RunWith(SpringRunner.class)
@SpringBootTest
public class GulimallSearchApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    @Data
    public static class Account{
        private int account_number;

        private int balance;

        private String firstname;

        private String lastname;

        private int age;

        private String gender;

        private String address;

        private String employer;

        private String email;

        private String city;

        private String state;

    }




    @Test
    public void searchData() throws IOException {
        //1.创建检索请求
        SearchRequest searchRequest=new SearchRequest();
        //指定索引
        searchRequest.indices("bank");
        //指定DSL,检索条件
        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();

        searchSourceBuilder.query(QueryBuilders.matchQuery("address","mill"));
        //按照年龄的值分布进行聚合
        TermsAggregationBuilder ageAgg = AggregationBuilders.terms("ageAgg").field("age").size(10);
        searchSourceBuilder.aggregation(ageAgg);
        //按照平均薪资进行聚合

        AvgAggregationBuilder balanceAVG = AggregationBuilders.avg("balanceAVG").field("balance");

        searchSourceBuilder.aggregation(balanceAVG);
        System.err.println(searchSourceBuilder);


        searchRequest.source(searchSourceBuilder);

        //执行检索
        SearchResponse searchResponse = client.search(searchRequest, GulimallElasticSearchConfig.COMMON_OPTIONS);

        //获取所有查到的数据
        SearchHit[] hits = searchResponse.getHits().getHits();
        /**
         *         "_index" : "newbank",
         *         "_type" : "_doc",
         *         "_id" : "970",
         *         "_score" : 5.4032025,
         */

        for (SearchHit hit : hits) {
            System.out.println(hit.getIndex());
            System.out.println(hit.getType());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
//            System.out.println(hit.getsource);
            String string = hit.getSourceAsString();
            Account account = JSON.parseObject(string,Account.class);
            System.out.println(account);
        }
        System.out.println("--------------------------------------------------------");
        //获取这次检索的分析信息
        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations.asList()) {
            System.out.println(aggregation.getName());
        }
        Terms ageAggTerm = aggregations.get("ageAgg");
        for (Terms.Bucket bucket : ageAggTerm.getBuckets()) {
            String keyAsString = bucket.getKeyAsString();
            System.out.println("年龄: " + keyAsString+" 数量:"+bucket.getDocCount());
        }
        Avg balanceAvg = aggregations.get("balanceAVG");
        System.out.println("平均薪资:" + balanceAvg.getValue());


    }


    /**
     * 测试数据存储到es中
     */
    @Test
    public void indexData() throws IOException {

        IndexRequest indexRequest = new IndexRequest("users");
        indexRequest.id("1");
//        indexRequest.source("userName","zhangsan","age",18,"gender","男");
        User user = new User();
        user.setUserName("张三");
        user.setAge(18);
        user.setGender("男");
        String jsonString = JSON.toJSONString(user);
        indexRequest.source(jsonString, XContentType.JSON);
        //执行操作
        IndexResponse index = client.index(indexRequest, GulimallElasticSearchConfig.COMMON_OPTIONS);
        System.out.println(index);


    }

    @Data
    class User{
//        public String getUserName() {
//            return userName;
//        }
//
//        public void setUserName(String userName) {
//            this.userName = userName;
//        }
//
//        public Integer getAge() {
//            return age;
//        }
//
//        public void setAge(Integer age) {
//            this.age = age;
//        }
//
//        public String getGender() {
//            return gender;
//        }
//
//        public void setGender(String gender) {
//            this.gender = gender;
//        }

        private String userName;
        private Integer age;
        private String gender;

    }

    @Test
    public void contextLoads() {
        System.out.println(client);

    }

}
