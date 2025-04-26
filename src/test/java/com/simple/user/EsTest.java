package com.simple.user;

import com.alibaba.fastjson2.JSON;
import com.simple.model.User;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EsTest {
    private Indices indices;

    private Doc doc;

    private BulkDoc bulkDoc;

    private Search search;

    @Before
    public void setUp() throws Exception {
        indices = new Indices();
        doc = new Doc();
        bulkDoc = new BulkDoc();
        search = new Search();
    }

    @After
    public void tearDown() throws Exception {
        indices = null;
        doc = null;
        bulkDoc = null;
        search = null;
    }

    @Test
    public void testIndicesCreate() {
        boolean isAcknowledged = false;
        try {
            isAcknowledged = indices.indicesCreate("user");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(isAcknowledged);
    }

    @Test
    public void testIndicesDelete() {
        boolean isAcknowledged = false;
        try {
            isAcknowledged = indices.indicesDelete("user");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(isAcknowledged);
    }

    @Test
    public void testDoc() throws IOException {
        String index = "user";
        User user = new User("sakura", "girl", 20);
        String userJson = JSON.toJSONString(user);

        String id = doc.index_insertDoc(index, userJson);
        String doc1 = doc.get_getDoc(index, id);
        String update = doc.update_updateDoc(index, id, "age", 18);
        String doc2 = doc.get_getDoc(index, id);
        String delete = doc.delete_deleteDoc(index, id);
        String doc3 = doc.get_getDoc(index, id);

        System.out.println(id);
        System.out.println(doc1);
        System.out.println(update);
        System.out.println(doc2);
        System.out.println(delete);
        System.out.println(doc3);
    }

    @Test
    public void testBulkDoc() throws IOException {
        String index = "user";
        User sakura = new User("sakura", "girl", 20);
        User aixin = new User("aixin", "girl", 23);
        User xiaolang = new User("xiaolang", "boy", 21);
        List<String> jsonList = new ArrayList<>();
        jsonList.add(JSON.toJSONString(sakura));
        jsonList.add(JSON.toJSONString(aixin));
        jsonList.add(JSON.toJSONString(xiaolang));

        List<String> idList = bulkDoc.bulkIndex(index, jsonList);
        List<String> deleteList = bulkDoc.bulkDelete(index, idList);

        System.out.println(idList);
        System.out.println(deleteList);
    }

    @Test
    public void testBulkDoc2() throws IOException {
        String index = "user";
        System.out.println(search.search_all(index));

        System.out.println(search.search_condition(index, "sex", "girl boy", 0, 10, "age", null));
        System.out.println(search.search_condition(index, "sex", "girl", 0, 10, "age", "asc"));
        System.out.println(search.search_condition(index, "sex", "girl", 0, 10, "age", "desc"));
        System.out.println(search.search_condition(index, "sex", "girl", 0, 1, null, null));
        System.out.println(search.search_condition(index, "sex", "girl", 1, 1, null, null));

        HashMap<String, Object> conditionMap = new HashMap<>();
        conditionMap.put("sex", "girl");
        conditionMap.put("name", "sakura");
        System.out.println(search.search_bool(index, conditionMap));
    }
}