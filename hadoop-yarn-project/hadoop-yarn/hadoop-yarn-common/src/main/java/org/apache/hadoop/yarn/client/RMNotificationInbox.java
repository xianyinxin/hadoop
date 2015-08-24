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

package org.apache.hadoop.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.RMNotificationHandler;
import org.apache.hadoop.yarn.api.RMNotificationProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RpcFactoryProvider;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationResponse;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A rpc server that handles RM's notification.
 */
public class RMNotificationInbox implements RMNotificationProtocol {
  private RMNotificationHandler handler = null;
  private Server server = null;
  private InetSocketAddress address = null;

  public RMNotificationInbox(Class protocol, InetSocketAddress address,
                             Configuration conf, RMNotificationHandler handler) {
    server = RpcFactoryProvider.getServerFactory(conf).getServer(protocol,
        this, address, conf, null, 1, null);
    this.address = address;
    if (handler != null) {
      this.handler = handler;
    } else {
      throw new IllegalArgumentException("Null RMNotificationHandler.");
    }
  }

  public void start() {
    if (this.handler != null) {
      server.start();
    } else {
      throw new YarnRuntimeException("RMNotificationInbox hasn't been initialized.");
    }
  }

  public void stop() {
    server.stop();
  }

  public InetSocketAddress getAddress() {
    return this.address;
  }

  @Override
  public NotificationResponse handleRMNotification(NotificationRequest notification)
      throws YarnException, IOException {
    return handler.handle(notification);
  }
}
