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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationType;
import org.apache.hadoop.yarn.api.RMNotificationProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.annotations.VisibleForTesting;

/**
 *
 */
public class NotificationsManager extends AbstractService
    implements EventHandler<NotificationEvent> {
  private Map<InetSocketAddress, RMNotificationProtocol> clientNotifiers=
      new HashMap<>();
  private Map<ApplicationAttemptId, InetSocketAddress> addrBook =
      new HashMap<>();
  private final BlockingQueue<NotificationEvent> dispatchQueue =
      new LinkedBlockingQueue<NotificationEvent>();
  private final ExecutorService postPool = Executors.newCachedThreadPool();
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final long THREAD_JOIN_TIMEOUT_MS = 1000;

  private Thread notifyThread;

  private static final Log LOG = LogFactory.getLog(NotificationsManager.class);

  public NotificationsManager() {
    super(NotificationsManager.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    notifyThread = new Thread(new EventProcessor());
  }

  @Override
  protected void serviceStart() throws Exception {
    notifyThread.start();
  }

  @Override
  protected void serviceStop() throws Exception{
    if (notifyThread != null){
      notifyThread.interrupt();
      notifyThread.join(THREAD_JOIN_TIMEOUT_MS);
    }
    if (postPool != null) {
      postPool.shutdown();
    }
  }

  @Override
  public void handle(NotificationEvent event) {
    try {
      dispatchQueue.put(event);
    } catch (InterruptedException e) {
      LOG.info("Interrupted.");
    }
  }

  private final class EventProcessor implements Runnable {
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          NotificationEvent event = dispatchQueue.take();
          final RMNotificationProtocol client =
              clientNotifiers.get(event.getTargetAddress());
          if (client == null) {
            LOG.warn("Target address " + event.getTargetAddress()
                + " hasn't been registered or has been removed.");
            continue;
          }
          final NotificationRequest notification =
              recordFactory.newRecordInstance(NotificationRequest.class);
          if (event.getType() ==
              NotificationEventType.RM_CONTAINER_ALLOCATED_EVENT) {
            notification.setNotificationType(NotificationType.RM_CONTAINER_ALLOCATED);
          } else {
            throw new YarnRuntimeException("Unknown NotificationEventType: "
                + event.getType());
          }

          // Now we don't capture the return value and any exceptions;
          postPool.submit(new Callable<NotificationResponse>() {
            @Override
            public NotificationResponse call() throws Exception {
              return client.handleRMNotification(notification);
            }
          });
        } catch (InterruptedException e) {
          LOG.info("Interrupted, now stop sending out notifications.");
          return;
        }
      }
    }
  }

  private synchronized InetSocketAddress registerAddress(String hostname, int port) {
    InetSocketAddress addr = new InetSocketAddress(hostname, port);
    if (clientNotifiers.containsKey(addr)) {
      LOG.warn("Address " + addr + "has been registered, please check.");
      return null;
    }
    allocateNotifier(addr);
    return addr;
  }

  private synchronized RMNotificationProtocol allocateNotifier(InetSocketAddress addr) {
    RMNotificationProtocol inboxProxy;
    try {
      inboxProxy = RMClientProxy.createClientProxy(getConfig(),
          RMNotificationProtocol.class, addr);
      clientNotifiers.put(addr, inboxProxy);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    return inboxProxy;
  }

  private synchronized void removeNotifier(InetSocketAddress addr) {
    if (clientNotifiers.containsKey(addr)) {
      clientNotifiers.remove(addr);
    }
  }

  public boolean registerAddressForAppAttempt(
      ApplicationAttemptId id, String hostname, int port) {
    if (id != null) {
      InetSocketAddress addr = registerAddress(hostname, port);
      if (addr != null) {
        addToAddressBook(id, addr);
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  public void removeNotifierForAppAttempt(ApplicationAttemptId id) {
    if (id != null) {
      InetSocketAddress addr = lookUpAddressBook(id);
      removeFromAddressBook(id);
      if (addr != null) {
        removeNotifier(addr);
      }
    }
  }

  private synchronized void addToAddressBook(ApplicationAttemptId id, InetSocketAddress addr) {
    addrBook.put(id, addr);
  }

  private synchronized void removeFromAddressBook(ApplicationAttemptId id) {
    addrBook.remove(id);
  }

  public InetSocketAddress lookUpAddressBook(ApplicationAttemptId id) {
    return addrBook.get(id);
  }
}
