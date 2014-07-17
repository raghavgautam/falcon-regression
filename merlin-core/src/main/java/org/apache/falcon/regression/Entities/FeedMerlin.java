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

package org.apache.falcon.regression.Entities;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.log4j.Logger;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;

public class FeedMerlin extends Feed {

    private static final Logger logger = Logger.getLogger(FeedMerlin.class);

    public FeedMerlin(String feedData) {
        this((Feed) fromString(EntityType.FEED, feedData));
    }

    public FeedMerlin(final Feed feed) {
        try {
            PropertyUtils.copyProperties(this, feed);
        } catch (IllegalAccessException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (InvocationTargetException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (NoSuchMethodException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        }
    }

    public void insertRetentionValueInFeed(String retentionValue) {
        for (org.apache.falcon.entity.v0.feed.Cluster cluster : getClusters().getClusters()) {
            cluster.getRetention().setLimit(new Frequency(retentionValue));
        }
    }

    public void setTableValue(String dBName, String tableName, String pathValue) {
        getTable().setUri("catalog:" + dBName + ":" + tableName + "#" + pathValue);
    }

    @Override
    public String toString() {
        try {
            StringWriter sw = new StringWriter();
            EntityType.FEED.getMarshaller().marshal(this, sw);
            return sw.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    public void setLocation(LocationType locationType, String feedInputPath) {
        for (Location location : getLocations().getLocations()) {
            if(location.getType() == locationType) {
                location.setPath(feedInputPath);
            }
        }
    }

    public void addProperty(String someProp, String someVal) {
            Property property = new Property();
            property.setName(someProp);
            property.setValue(someVal);
            this.getProperties().getProperties().add(property);
    }

}
