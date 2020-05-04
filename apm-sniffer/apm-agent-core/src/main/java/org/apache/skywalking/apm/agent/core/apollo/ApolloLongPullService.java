/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.agent.core.apollo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: yangrusheng
 * @Description: long pull get config from apollo
 * @Date: Created in 1:17 2020/5/2
 * @Modified By:
 */
public class ApolloLongPullService implements Runnable {

    private static final ILog logger = LogManager.getLogger(ApolloLongPullService.class);

    private static String IS_SPAN_TRACE = "IS_SPAN_TRACE";

    private static String IS_COLLECT_JVM_INFO = "IS_COLLECT_JVM_INFO";

    private static List<String> CONFIG_KEYS = new ArrayList<>();

    private volatile Long notificationId = -1L;

    private static CloseableHttpClient HTTP_CLIENT;

    static {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(5);
        cm.setDefaultMaxPerRoute(5);
        RequestConfig config = RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(61000).build();
        HTTP_CLIENT = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(config)
                .build();
        List<String> keys = new ArrayList<>();
        keys.add(IS_SPAN_TRACE);
        keys.add(IS_COLLECT_JVM_INFO);
        CONFIG_KEYS = keys;
    }

    @Override
    public void run() {
        while (true) {
            try {
                apolloLongPull();
            } catch (Exception e) {
                logger.error("apollo long pull exception: ", e);
                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException e1) {
                    logger.error("apollo long pull sleep exception: ", e);
                }
            }
        }
    }

    private void apolloLongPull() throws IOException {
        List<ApolloConfigNotification> notifications = new ArrayList<>();
        ApolloConfigNotification notification = new ApolloConfigNotification(
                System.getProperty(Constants.SKYWALKING_NAMESPACE), notificationId);
        notifications.add(notification);
        Gson gson = new Gson();
        String notificationsEncode = URLEncoder.encode(gson.toJson(notifications), "UTF-8");
        String url = System.getProperty(Constants.APOLLO_CONFIG_SERVICE) + "/notifications/v2?appId="
                + System.getProperty(Constants.SKYWALKING_APP_ID) + "&cluster="
                + System.getProperty(Constants.APOLLO_CLUSTER) + "&notifications=" + notificationsEncode;
        HttpGet httpGet = new HttpGet(url);

        CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet);
        if (response.getStatusLine().getStatusCode() == Constants.HTTP_200) {
            // config have change
            String result = EntityUtils.toString(response.getEntity());
            if (result == null) {
                return;
            }
            Type type = new TypeToken<ArrayList<ApolloConfigNotification>>() { }.getType();
            List<ApolloConfigNotification> newNotifications = gson.fromJson(result, type);
            if (newNotifications == null || newNotifications.size() == 0) {
                return;
            }
            for (ApolloConfigNotification item: newNotifications) {
                if (System.getProperty(Constants.SKYWALKING_NAMESPACE).equals(item.getNamespaceName())) {
                    long notificationId = item.getNotificationId();
                    if (notificationId > this.notificationId) {
                        this.notificationId = notificationId;
                    }
                    // get latest config, and update
                    ApolloConfiguration configuration = getLatestConfig();
                    if (configuration == null) {
                        return;
                    }
                    updateBooleanConfigValue(configuration.getConfigurations(), CONFIG_KEYS);
                    logger.info("skyWalking config update success.");
                    break;
                }
            }
        } else if (response.getStatusLine().getStatusCode() == Constants.HTTP_304) {
            // config have not change
            return;
        } else {
            logger.info("apollo long pull http request status: {}, result: {}",
                    response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity()));
        }
    }

    private ApolloConfiguration getLatestConfig() {
        String url = System.getProperty(Constants.APOLLO_CONFIG_SERVICE) + "/configs/"
                + System.getProperty(Constants.SKYWALKING_APP_ID) + "/" + System.getProperty(Constants.APOLLO_CLUSTER)
                + "/" + System.getProperty(Constants.SKYWALKING_NAMESPACE);
        HttpGet httpGet = new HttpGet(url);
        String response = sendHttpRequest(httpGet);
        if (response == null || "".equals(response)) {
            return null;
        }
        Gson gson = new Gson();
        return gson.fromJson(response, ApolloConfiguration.class);
    }

    private String sendHttpRequest(HttpUriRequest request) {
        String result = "";
        int retries = 0;
        while (retries < 2) {
            try (CloseableHttpResponse response = HTTP_CLIENT.execute(request)) {
                result = EntityUtils.toString(response.getEntity());
                break;
            } catch (NoHttpResponseException e) {
                retries++;
                if (retries >= 2) {
                    logger.warn("send http request for string error", e);
                }
            } catch (IOException e) {
                logger.warn("send http request for string error", e);
                break;
            }
        }
        return result;
    }

    private static void updateBooleanConfigValue(Map<String, String> configurations, List<String> keys) {
        for (String key: keys) {
            String value = configurations.get(key);
            if ("true".equals(value)) {
                updateConfigValue(key, true);
            } else if ("false".equals(value)) {
                updateConfigValue(key, false);
            }
        }
    }

    private static void updateConfigValue(String key, boolean state) {
        if (IS_SPAN_TRACE.equals(key)) {
            Config.Agent.setIsSpanTrace(state);
        }
        if (IS_COLLECT_JVM_INFO.equals(key)) {
            Config.Jvm.setIsCollectJvmInfo(state);
        }
    }
}
