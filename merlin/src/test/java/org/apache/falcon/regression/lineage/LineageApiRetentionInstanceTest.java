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

package org.apache.falcon.regression.lineage;

import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.enumsAndConstants.FEED_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.LineageHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.GraphAssert;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "lineage-rest")
public class LineageApiRetentionInstanceTest extends BaseTestClass {
    String baseTestHDFSDir = baseHDFSDir + "/RetentionTest/";
    String testHDFSDir = baseTestHDFSDir;
    final String testHDFSDirPattern = testHDFSDir + "${YEAR}/${MONTH}/${DAY}/${HOUR}";
    static Logger logger = Logger.getLogger(LineageApiRetentionInstanceTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    FeedMerlin feedMerlin;

    LineageHelper lineageHelper;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        lineageHelper = new LineageHelper(prism);
    }

    @BeforeMethod(alwaysRun = true, firstTimeOnly = true)
    public void setUp() throws Exception {
        bundles[0] = new Bundle(BundleUtil.getBundleData("RetentionBundles")[0], cluster);
        bundles[0].setInputFeedDataPath(testHDFSDir);
        bundles[0].generateUniqueBundle();
        String period = "10";
        String unit = "minutes";

        feedMerlin = new FeedMerlin(BundleUtil.getInputFeedFromBundle(bundles[0]));
        feedMerlin.setLocation(LocationType.DATA, testHDFSDirPattern);
        feedMerlin.insertRetentionValueInFeed(unit + "(" + period + ")");
        feedMerlin.setLateArrival(null);

        final DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(10);
        final DateTime endDate = startDate.plusMinutes(10);
        final List<DateTime> dataDates =
            TimeUtil.getDatesOnEitherSide(startDate, endDate, FEED_TYPE.MINUTELY);
        HadoopUtil.createPeriodicDataset(
            TimeUtil.convertDatesToString(dataDates,
                TimeUtil.getFormatStringForFeedType(FEED_TYPE.MINUTELY)),
            OSUtil.NORMAL_INPUT, clusterFS, testHDFSDir);

        bundles[0].submitClusters(prism);
        final ServiceResponse response = prism.getFeedHelper().submitAndSchedule(
            Util.URLS.SUBMIT_AND_SCHEDULE_URL, feedMerlin.toString());
        AssertUtil.assertSucceeded(response);

        String bundleId = OozieUtil.getBundles(
            clusterOC, feedMerlin.getName(), ENTITY_TYPE.FEED).get(0);
        List<String> workflows = OozieUtil.waitForRetentionWorkflowToSucceed(bundleId, clusterOC);
        Assert.assertEquals(workflows.size(), 1, "Expecting one retention workflow");
    }

    @AfterMethod(alwaysRun = true, lastTimeOnly = true)
    public void tearDown() throws Exception {
        prism.getFeedHelper().delete(
            Util.URLS.DELETE_URL, BundleUtil.getInputFeedFromBundle(bundles[0]));
        removeBundles();
    }

    @Test
    public void testFeedToRetentionInstanceNodes() throws Exception {
        final String feedName = feedMerlin.getName();
        // fetching feed instances info
        final VerticesResult feedVertex = lineageHelper.getVerticesByName(feedName);
        GraphAssert.assertVertexSanity(feedVertex);

    }

}
