package com.alibaba.otter.dao;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.consts.ES;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CourseES {
    private RestHighLevelClient restHighLevelClient;

    public CourseES(RestHighLevelClient restHighLevelClient){
        this.restHighLevelClient = restHighLevelClient;
    }
    public void disconnect(){
        try {
            this.restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final HashMap<String, Integer> Fields2Skip = new HashMap<String, Integer>() {{
        put("courseCoverPicUrl", 1);
        put("courseVideoUrl", 1);
        put("courseMaxNum", 1);
        put("version", 1);
        put("courseSaleProperty", 1);
    }};

    private static final String CoursePrimaryKeyField = "courseId";

    public void addCourse(java.util.List<CanalEntry.Column> cols){
        try {
            Map<String, Object> map = new HashMap<>();
            for (CanalEntry.Column  c : cols) {
                if (Fields2Skip.get(c.getName()) == null)
                    map.put(c.getName(), c.getValue());
            }
            IndexRequest request = new IndexRequest(ES.ESCourseIndexName);
            request.timeout(ES.DefaultInsertTimeout);
            request.source(map, XContentType.JSON);
            this.restHighLevelClient.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteCourse(java.util.List<CanalEntry.Column> cols){
        try {
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(ES.ESCourseIndexName);
            String courseId = "";
            for (CanalEntry.Column  c : cols) {
                if(CoursePrimaryKeyField.equals(c.getName())){
                    courseId = c.getValue();
                    break;
                }
            }
            if ("".equals(courseId)){
                throw new RuntimeException("courseId is empty!");
            }
            deleteByQueryRequest.setQuery(new TermQueryBuilder(CoursePrimaryKeyField, courseId));
            deleteByQueryRequest.setTimeout(ES.DefaultDeleteTimeout);
            deleteByQueryRequest.setMaxDocs(1);
            this.restHighLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void updateCourse(java.util.List<CanalEntry.Column> cols){
        try {
            java.util.Map<java.lang.String,java.lang.Object> params = new HashMap<>();
            Map<String, Object> updatedField2Value = new HashMap<>();
            List<String> updatedFields = new ArrayList<>();
            params.put("map", updatedField2Value);
            params.put("list", updatedFields);
            String courseId = "";
            for (CanalEntry.Column  c : cols) {
                if(CoursePrimaryKeyField.equals(c.getName()))
                    courseId = c.getValue();
                else if (c.getUpdated()){ // 如果数据库更新了这个字段，那么索引也更新这个字段
                    if (Fields2Skip.get(c.getName()) == null){
                        updatedField2Value.put(c.getName(), c.getValue());
                        updatedFields.add(c.getName());
                    }
                }
            }
            if(updatedFields.size() == 0){
                // no need to update es index
                return;
            }
            if ("".equals(courseId)){
                throw new RuntimeException("courseId is empty!");
            }

            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(ES.ESCourseIndexName);
            updateByQueryRequest.setTimeout(ES.DefaultUpdateTimeout);
            updateByQueryRequest.setQuery(new TermQueryBuilder(CoursePrimaryKeyField, courseId));
            updateByQueryRequest.setMaxDocs(1);

            updateByQueryRequest.setScript(new Script(ScriptType.INLINE, ES.ESDefaultLang, "for(item in params.list) {ctx._source[item] = params.map.get(item);}", params));

            this.restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
