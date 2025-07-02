package com.icho.nifi.processors;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.HttpGet;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import java.io.IOException;

import java.util.UUID;



@Tags({"streamLoad", "doris"})
@CapabilityDescription("streamLoad写入doris表")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
public class InvokeStreamLoad extends AbstractProcessor {

    public static final Relationship RESPONSE = new Relationship.Builder()
            .name("success")
            .description("http请求结果响应")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("请求失败")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            RESPONSE,
            FAILURE
    )));

    public static final PropertyDescriptor DORIS_FE_HOST = new PropertyDescriptor.Builder()
            .name("doris host")
            .description("doris fe或者be的ip")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DORIS_HTTP_PORT = new PropertyDescriptor.Builder()
            .name("doris port")
            .description("doris http端口")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DORIS_DB = new PropertyDescriptor.Builder()
            .name("doris db")
            .description("插入表所在的库")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DORIS_TABLE = new PropertyDescriptor.Builder()
            .name("doris table")
            .description("doris 插入表名")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DORIS_USER = new PropertyDescriptor.Builder()
            .name("doris user")
            .description("doris user name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DORIS_PASSWORD = new PropertyDescriptor.Builder()
            .name("doris password")
            .description("doris password")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor HEADERS = new PropertyDescriptor.Builder()
            .name("headers")
            .description("添加请求头参数，请输入json格式，例如 {\"key1\":\"value1\"}")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            DORIS_FE_HOST,
            DORIS_HTTP_PORT,
            DORIS_DB,
            DORIS_TABLE,
            DORIS_USER,
            DORIS_PASSWORD,
            HEADERS
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        // Initialization logic when the processor is scheduled to run
        getLogger().info("StreamLoad onScheduled");
    }

    @OnStopped
    public void onStopped() {
        // Cleanup logic when the processor is stopped
        getLogger().info("StreamLoad onStopped");
    }

    private final static HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    // If the connection target is FE, you need to deal with 307 redirect。
                    return true;
                }
            });

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            getLogger().error("FlowFile is null");
            return;
        }

        String dorisHost = context.getProperty(DORIS_FE_HOST).evaluateAttributeExpressions(flowFile).getValue();
        int dorisPort = context.getProperty(DORIS_HTTP_PORT).evaluateAttributeExpressions(flowFile).asInteger();
        String dorisDb = context.getProperty(DORIS_DB).evaluateAttributeExpressions(flowFile).getValue();
        String dorisTable = context.getProperty(DORIS_TABLE).evaluateAttributeExpressions(flowFile).getValue(); //
        String dorisUser = context.getProperty(DORIS_USER).getValue();
        String dorisPassword = context.getProperty(DORIS_PASSWORD).getValue();
        String headers = context.getProperty(HEADERS).getValue();
        // headers is a json string, such as {"key1":"value1"}
        Map<String, String> headersMap = new HashMap<>();
        if (headers != null && !headers.isEmpty()){
            try {
                JSONObject jsonObject = JSONObject.parseObject(headers);
                for (String key : jsonObject.keySet()) {
                    String value = jsonObject.getString(key);
                    headersMap.put(key, value);
                }
            } catch (Exception e) {
                getLogger().error("parse json headers error", e);
                session.transfer(flowFile, FAILURE);
                return;
            }
        }
        String urlStr = String.format("http://%s:%d/api/%s/%s/_stream_load", dorisHost, dorisPort, dorisDb, dorisTable);
        getLogger().info("StreamLoad onTrigger urlStr={}", urlStr);
        try {
            String jsonData = readFlowFileContent(session, flowFile);
            getLogger().info("StreamLoad onTrigger jsonData length: {}", jsonData != null ? jsonData.length() : 0);
            try (CloseableHttpClient client = httpClientBuilder.build()) {
                HttpPut put = new HttpPut(urlStr);
                put.removeHeaders(HttpHeaders.CONTENT_LENGTH);
                put.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
                put.setHeader(HttpHeaders.EXPECT, "100-continue");
                put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(dorisUser, dorisPassword));
                // You can set stream load related properties in the Header, here we set label and column_separator.
                put.setHeader("label", headersMap.getOrDefault("label",UUID.randomUUID().toString()));
                // 如果存在label,并移除map里的label
                headersMap.remove("label");
                put.setHeader("column_separator", headersMap.getOrDefault("column_separator", ","));
                headersMap.remove("column_separator");
                put.setHeader("format", headersMap.getOrDefault("format", "json"));
                headersMap.remove("format");
                //导入json数组
                put.setHeader("strip_outer_array", headersMap.getOrDefault("strip_outer_array", "true"));
                headersMap.remove("strip_outer_array");

                //将headersMap中的key和value设置到put的header中
                for (String key : headersMap.keySet()) {
                    String value = headersMap.get(key);
                    if (value != null) {
                        put.setHeader(key, value);
                    }
                }

                getLogger().info("StreamLoad onTrigger put header: {}", Arrays.toString(put.getAllHeaders()));
                if (jsonData == null || jsonData.isEmpty()) {
                    getLogger().error("StreamLoad onTrigger jsonData is null or empty");
                    session.transfer(flowFile, FAILURE);
                    return;
                }
                StringEntity entity = new StringEntity(jsonData, StandardCharsets.UTF_8);
                put.setEntity(entity);

                try (CloseableHttpResponse response = client.execute(put)) {
                    String loadResult = "";
                    if (response.getEntity() != null) {
                        loadResult = EntityUtils.toString(response.getEntity());
                    } else {
                        getLogger().error("StreamLoad fail,post api error");
                        session.transfer(flowFile, FAILURE);
                    }
                    //将loadResult 转为json，如果loadResult不为空并且loadResult，key = status = "Success"，则认为导入成功
                    JSONObject result = JSONObject.parseObject(loadResult);
                    getLogger().info("StreamLoad onTrigger result={}", result);
                    if (result.containsKey("Status") && result.getString("Status").equals("Success")) {
                        // success write result to flowFile content
                        flowFile = session.write(flowFile, out -> out.write(result.toString().getBytes(StandardCharsets.UTF_8)));
                        session.transfer(flowFile, RESPONSE);
                    } else {
                        //from result get error url and visit error url to get error message
                        String errorUrl = result.getString("ErrorURL");
                        getLogger().error("StreamLoad onTrigger errorUrl={}", errorUrl);
                        session.transfer(flowFile, FAILURE);
                    }
                }
            }
        } catch (Exception e) {
            getLogger().error("StreamLoad onTrigger error", e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private String readFlowFileContent(ProcessSession session, FlowFile flowFile) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        session.read(flowFile, inputStream -> {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        });
        return outputStream.toString(StandardCharsets.UTF_8.name());
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }


}
