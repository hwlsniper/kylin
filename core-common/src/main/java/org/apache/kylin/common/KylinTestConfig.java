/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinTestConfig extends KylinConfig {

    private static final Logger logger = LoggerFactory.getLogger(KylinTestConfig.class);

    // static cached instances
    private static KylinTestConfig ENV_INSTANCE = null;

    private KylinTestConfig(KylinConfig kylinConfig) {
        super(kylinConfig.getAllProperties(), true);
    }

    public static KylinTestConfig getInstanceFromEnv() {
        synchronized (KylinTestConfig.class) {
            if (ENV_INSTANCE == null) {
                try {
                    KylinConfig kylinConfig= KylinConfig.getInstanceFromEnv();
                    KylinTestConfig testConfig = new KylinTestConfig(kylinConfig);

                    logger.info("Initialized a new KylinTestConfig from getInstanceFromEnv : " + System.identityHashCode(testConfig));
                    ENV_INSTANCE = testConfig;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KylinTestConfig ", e);
                }
            }
            return ENV_INSTANCE;
        }
    }

    //Only used in test cases!!!
    public static void destroyInstance() {
        logger.info("Destory KylinTestConfig");
        ENV_INSTANCE = null;
        KylinConfig.destroyInstance();
    }

    public void setAppendDictEntrySize(int entrySize) {
        setProperty("kylin.dictionary.append-entry-size", String.valueOf(entrySize));
    }

    public void setHBaseHFileSizeGB(float size) {
        setProperty("kylin.storage.hbase.hfile-size-gb", String.valueOf(size));
    }

    public void setMaxBuildingSegments(int maxBuildingSegments) {
        setProperty("kylin.cube.max-building-segments", String.valueOf(maxBuildingSegments));
    }

    public void setRunAsRemoteCommand(String v) {
        setProperty("kylin.job.use-remote-cli", v);
    }

    public void setRemoteHadoopCliHostname(String v) {
        setProperty("kylin.job.remote-cli-hostname", v);
    }

    public void setRemoteHadoopCliUsername(String v) {
        setProperty("kylin.job.remote-cli-username", v);
    }

    public void setRemoteHadoopCliPassword(String v) {
        setProperty("kylin.job.remote-cli-password", v);
    }

    public void setMailEnabled(boolean enable) {
        setProperty("kylin.job.notification-enabled", "" + enable);
    }

    public void setStorageUrl(String storageUrl) {
        setProperty("kylin.storage.url", storageUrl);
    }

}
