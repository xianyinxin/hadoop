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

package org.apache.hadoop.yarn.server.resourcemanager.notificationsmanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.RMNotificationHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.client.RMNotificationInbox;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationResponse;
import org.apache.hadoop.yarn.api.RMNotificationProtocol;
import org.apache.hadoop.yarn.util.Records;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 *
 */
public class TestNotificationsManager {
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private static NotificationsManager manager = new NotificationsManager();
  private static RMNotificationInbox inbox;
  private static InetSocketAddress addr;
  private static Handler handler;

  static class Handler implements RMNotificationHandler {
    private boolean received = false;

    @Override
    public NotificationResponse handle(NotificationRequest noti) {
      received = true;
      return recordFactory.newRecordInstance(NotificationResponse.class);
    }

    public boolean getResult() {
      return received;
    }
  }

  @BeforeClass
  public static void setup() {
    Configuration conf = new YarnConfiguration();
    manager.init(conf);
    manager.start();
    ApplicationAttemptId amInstance =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0L,0), 0);
    manager.registerAddressForAppAttempt(amInstance, "127.0.0.1", 10800);
    handler = new Handler();
    inbox = new RMNotificationInbox(RMNotificationProtocol.class, addr, conf, handler);
    inbox.start();
  }

  @AfterClass
  public static void finish() {
    manager.stop();
    inbox.stop();
  }

  @Test
  public void TestNotify() throws Exception {
    manager.handle(new NotificationEvent(addr, NotificationEventType.RM_CONTAINER_ALLOCATED_EVENT));
    Thread.sleep(2000);
    Assert.assertEquals(true, handler.getResult());
  }
}
