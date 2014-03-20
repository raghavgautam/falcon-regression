/**
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

package org.apache.falcon.regression.core.util;

public class OSUtil {

    public static String SEPARATOR = System.getProperty("file.separator", "/");
    public static String RESOURCES = String.format("src%stest%sresources%s", SEPARATOR, SEPARATOR, SEPARATOR);
    public static String RESOURCES_OOZIE = String.format(RESOURCES + "oozie%s", SEPARATOR);
    public static String OOZIE_EXAMPLE_INPUT_DATA =
            String.format(RESOURCES + "OozieExampleInputData%s", SEPARATOR);
    public static String NORMAL_INPUT =
            String.format(OOZIE_EXAMPLE_INPUT_DATA + "normalInput%s", SEPARATOR);

    public static String getPath(String... pathParts) {
        StringBuilder path = new StringBuilder();
        if (pathParts.length == 0) return "";

        path.append(pathParts[0]);
        for (int i = 1; i < pathParts.length; i++) {
            path.append(OSUtil.SEPARATOR);
            path.append(pathParts[i]);
        }
        return path.toString();
    }
}