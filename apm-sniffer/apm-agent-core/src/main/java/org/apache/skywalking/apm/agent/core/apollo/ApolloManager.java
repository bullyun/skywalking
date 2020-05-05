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

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Author: yangrusheng
 * @Description: get config from Apollo and listening change
 * @Date: Created in 21:43 2020/4/30
 * @Modified By:
 */
public class ApolloManager implements BootService, Runnable {

    private static final ILog logger = LogManager.getLogger(ApolloManager.class);

    private volatile ScheduledFuture<?> apolloFuture;

    private volatile boolean connected = false;

    private volatile Thread thread;


    @Override
    public void prepare() throws Throwable {

    }

    @Override
    public void boot() throws Throwable {
        if (!Config.Apollo.IS_OBTAIN_APOLLO_CONFIG) {
            logger.info("skyWalking not to Apollo get config");
            return;
        }
        apolloFuture = Executors
                .newSingleThreadScheduledExecutor(new DefaultNamedThreadFactory("ApolloManager"))
                .scheduleAtFixedRate(new RunnableWithExceptionProtection(this, new RunnableWithExceptionProtection.CallbackWhenException() {
                    @Override
                    public void handle(Throwable t) {
                        logger.error("unexpected exception.", t);
                    }
                }), 0, Config.Apollo.APOLLO_CONNECT_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        if (connected) {
            return;
        }
        if (System.getProperty(Constants.APP_ID) == null
                || System.getProperty(Constants.APOLLO_META) == null
                || System.getProperty(Constants.APOLLO_CLUSTER) == null
                || System.getProperty(Constants.SKYWALKING_NAMESPACE) == null) {
            logger.info("apollo connect parameters are not full.");
            return;
        }
        try {
            if (thread == null) {
                synchronized (ApolloManager.class) {
                    if (thread == null) {
                        this.thread = new Thread(new ApolloLongPullService());
                        this.thread.start();
                        connected = true;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("skyWalking get config failure from apollo, will retry. exception: ", e);
        }

    }

    @Override
    public void onComplete() throws Throwable {

    }

    @Override
    public void shutdown() throws Throwable {
        if (apolloFuture != null) {
            apolloFuture.cancel(true);
        }
        if (thread != null) {
            thread.interrupt();
        }
    }
}
