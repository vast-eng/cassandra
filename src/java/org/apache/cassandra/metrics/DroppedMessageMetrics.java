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
package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import org.apache.cassandra.net.MessagingService;

/**
 * Metrics for dropped messages by verb.
 */
public class DroppedMessageMetrics
{
    /** Number of dropped messages */
    public final Meter dropped;
    public final Meter droppedAtDelivery;
    public final Histogram droppedHistogram;
    public final Histogram droppedAtDeliveryHistogram;
    public final Histogram unDroppedHistogram;
    public final Histogram unDroppedAtDeliveryHistogram;

    private long lastDropped = 0;

    public DroppedMessageMetrics(MessagingService.Verb verb)
    {
        MetricNameFactory factory = new DefaultNameFactory("DroppedMessage", verb.toString());
        dropped = Metrics.newMeter(factory.createMetricName("Dropped"), "dropped", TimeUnit.SECONDS);
        droppedAtDelivery = Metrics.newMeter(factory.createMetricName("DroppedAtDelivery"), "droppedAtDelivery", TimeUnit.SECONDS);
        droppedHistogram = Metrics.newHistogram(factory.createMetricName("DroppedHistogram"), true);
        droppedAtDeliveryHistogram = Metrics.newHistogram(factory.createMetricName("DroppedAtDeliveryHistogram"), true);
        unDroppedHistogram = Metrics.newHistogram(factory.createMetricName("UnDroppedHistogram"), true);
        unDroppedAtDeliveryHistogram = Metrics.newHistogram(factory.createMetricName("UnDroppedAtDeliveryHistogram"), true);
    }

    @Deprecated
    public int getRecentlyDropped()
    {
        long currentDropped = dropped.count();
        long recentlyDropped = currentDropped - lastDropped;
        lastDropped = currentDropped;
        return (int)recentlyDropped;
    }
}
