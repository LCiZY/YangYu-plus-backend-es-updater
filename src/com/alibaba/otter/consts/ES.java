package com.alibaba.otter.consts;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ES {
    public static final String ESHost = "localhost";
    public static final int    ESPort = 9200;
    public static final String ESScheme = "http";
    public static final String ESUserName = "elastic";
    public static final String ESPassword = "yangyuespwd2021";

    public static final String ESDefaultLang = "painless";
    public static final String ESCourseIndexName = "course";

    public static final String DefaultInsertTimeout = "2s";
    public static final String DefaultDeleteTimeout = "1s";
    public static final String DefaultUpdateTimeout = "2s";


    public static RestHighLevelClient restHighLevelClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ES.ESUserName, ES.ESPassword));

        RestClientBuilder builder = RestClient.builder(new HttpHost(ESHost, ESPort, ESScheme)).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(-1);
                requestConfigBuilder.setSocketTimeout(-1);
                requestConfigBuilder.setConnectionRequestTimeout(-1);
                return requestConfigBuilder;
            }
        }).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        return new RestHighLevelClient(builder);
    }

}

