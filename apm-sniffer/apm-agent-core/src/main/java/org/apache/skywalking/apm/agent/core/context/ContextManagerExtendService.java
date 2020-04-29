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

package org.apache.skywalking.apm.agent.core.context;

import org.apache.skywalking.apm.agent.core.boot.*;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.sampling.SamplingService;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author wusheng
 */
@DefaultImplementor
public class ContextManagerExtendService implements BootService {
    @Override public void prepare() {

    }

    @Override public void boot() {

    }

    @Override public void onComplete() {

    }

    @Override public void shutdown() {

    }

    private List<Pattern> ignoreOperatorPatterns;
    private String wildcardToRegex(String src) {
        return "^" + src.replace("/", "\\/").
                replace("?", "\\?").
                replace(".", "\\.").
                replace("*", ".*") + "$";
    }

    private boolean isIgnoreOperator(String operationName) {
        if ("".equals(Config.Agent.IGNORE_OPERATOR)) {
            return false;
        }
        if (ignoreOperatorPatterns == null) {
            List<Pattern> ignoreOperatorPatterns1 = new ArrayList<Pattern>();

            String[] ignoreOperators = Config.Agent.IGNORE_OPERATOR.split(",");
            for (int i = 0;i < ignoreOperators.length;i++) {
                ignoreOperators[i] = wildcardToRegex(ignoreOperators[i]);

                try {
                    Pattern p = Pattern.compile(ignoreOperators[i]);
                    ignoreOperatorPatterns1.add(p);
                } catch (Exception e) {
                }
            }
            ignoreOperatorPatterns = ignoreOperatorPatterns1;
        }
        for (Pattern p : ignoreOperatorPatterns) {
            if (p.matcher(operationName).matches()) {
                return true;
            }
        }
        return false;
    }

    public AbstractTracerContext createTraceContext(String operationName, boolean forceSampling) {
        AbstractTracerContext context;
        int suffixIdx = operationName.lastIndexOf(".");
        if ((suffixIdx > -1 && Config.Agent.IGNORE_SUFFIX.contains(operationName.substring(suffixIdx))) ||
                isIgnoreOperator(operationName)) {
            context = new IgnoredTracerContext();
        } else {
            SamplingService samplingService = ServiceManager.INSTANCE.findService(SamplingService.class);
            if (forceSampling || samplingService.trySampling()) {
                context = new TracingContext();
            } else {
                context = new IgnoredTracerContext();
            }
        }

        return context;
    }
}
