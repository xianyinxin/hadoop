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

package org.apache.hadoop.yarn.client.api.async.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.RMNotificationHandler;
import org.apache.hadoop.yarn.api.RMNotificationProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationType;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterNotificationAddressRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterNotificationAddressResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.RMNotificationInbox;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

@Private
@Unstable
public class AMRMClientAsyncImpl<T extends ContainerRequest> 
extends AMRMClientAsync<T> {
  
  private static final Log LOG = LogFactory.getLog(AMRMClientAsyncImpl.class);
  
  private final HeartbeatThread heartbeatThread;
  private final CallbackHandlerThread handlerThread;

  private final BlockingQueue<AllocateResponse> responseQueue;
  
  private final Object unregisterHeartbeatLock = new Object();
  
  private volatile boolean keepRunning;
  private volatile float progress;

  private RMNotificationInbox inbox = null;
  
  private volatile Throwable savedException;
  
  public AMRMClientAsyncImpl(int intervalMs, CallbackHandler callbackHandler) {
    this(new AMRMClientImpl<T>(), intervalMs, callbackHandler);
  }
  
  @Private
  @VisibleForTesting
  public AMRMClientAsyncImpl(AMRMClient<T> client, int intervalMs,
      CallbackHandler callbackHandler) {
    super(client, intervalMs, callbackHandler);
    heartbeatThread = new HeartbeatThread();
    handlerThread = new CallbackHandlerThread();
    responseQueue = new LinkedBlockingQueue<AllocateResponse>();
    keepRunning = true;
    savedException = null;
  }
    
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    client.init(conf);
  }  
  
  @Override
  protected void serviceStart() throws Exception {
    handlerThread.setDaemon(true);
    handlerThread.start();
    client.start();
    super.serviceStart();
  }
  
  /**
   * Tells the heartbeat and handler threads to stop and waits for them to
   * terminate.
   */
  @Override
  protected void serviceStop() throws Exception {
    keepRunning = false;
    heartbeatThread.interrupt();
    try {
      heartbeatThread.join();
    } catch (InterruptedException ex) {
      LOG.error("Error joining with heartbeat thread", ex);
    }
    client.stop();
    handlerThread.interrupt();
    super.serviceStop();
  }
  
  public void setHeartbeatInterval(int interval) {
    heartbeatIntervalMs.set(interval);
  }
  
  public List<? extends Collection<T>> getMatchingRequests(
                                                   Priority priority, 
                                                   String resourceName, 
                                                   Resource capability) {
    return client.getMatchingRequests(priority, resourceName, capability);
  }
  
  /**
   * Registers this application master with the resource manager. On successful
   * registration, starts the heartbeating thread.
   * @throws YarnException
   * @throws IOException
   */
  public RegisterApplicationMasterResponse registerApplicationMaster(
      String appHostName, int appHostPort, String appTrackingUrl)
      throws YarnException, IOException {
    RegisterApplicationMasterResponse response = client
        .registerApplicationMaster(appHostName, appHostPort, appTrackingUrl);
    heartbeatThread.start();

    if (getConfig().getBoolean(
        YarnConfiguration.YARN_CLIENT_RM_NOTIFICATION_ENABLED,
        YarnConfiguration.DEFAULT_YARN_CLIENT_RM_NOTIFICATION_ENABLED
    )) {
      // Init and start notification inbox
      Configuration.IntegerRanges portsRange;
      portsRange =
          getConfig().getRange(
              YarnConfiguration.YARN_CLIENT_RM_NOTIFICATION_PROTOCOL_PORTS_POOL,
              YarnConfiguration
                  .DEFAULT_YARN_CLIENT_RM_NOTIFICATION_PROTOCOL_PORTS_POOL);
      int port = -1;
      for (Integer aPort : portsRange) {
        if (isPortAvailable(port)) {
          port = aPort;
          break;
        }
      }
      if (port != -1) {
        initNotificationInbox(appHostName, port);
      } else {
        LOG.warn("Register notification address failed: no port can be bind.");
      }
    }
    return response;
  }

  private void initNotificationInbox(String host, int port) {
    try {
      RegisterNotificationAddressResponse response =
          client.registerNotificationAddress(host, port);
      if (response.getIfSuccess()) {
        inbox = new RMNotificationInbox(RMNotificationProtocol.class,
            new InetSocketAddress(host, port),
            getConfig(), new AMRMClientNotificationHandler());
        inbox.start();
        LOG.info("Initialized notification inbox with address: "
            + host + ", " + port);
      } else {
        LOG.warn("Register notification address failed: "
            + response.getDiagnostics());
      }
    } catch (Exception e) {
      LOG.warn("Exception while initialize notification inbox: " + e);
    }
  }

  private boolean isPortAvailable(int port) {
    boolean flag = false;
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(port);
      flag = true;
    } catch (IOException e) {
      LOG.info("Port " + port + "is not available.");
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          LOG.warn("Exception while closing temp socket.");
        }
      }
    }
    return flag;
  }

  public class AMRMClientNotificationHandler implements RMNotificationHandler {

    @Override
    public NotificationResponse handle(NotificationRequest notification) {
      if (notification.getNotificationType() == NotificationType.RM_CONTAINER_ALLOCATED) {
//        heartbeatThread.sendOutofBandHeartbeat();
      }
      NotificationResponse response =
          RecordFactoryProvider.getRecordFactory(null)
              .newRecordInstance(NotificationResponse.class);
      response.setNotificationReceived(true);
      return response;
    }
  }

  /**
   * Unregister the application master. This must be called in the end.
   * @param appStatus Success/Failure status of the master
   * @param appMessage Diagnostics message on failure
   * @param appTrackingUrl New URL to get master info
   * @throws YarnException
   * @throws IOException
   */
  public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
      String appMessage, String appTrackingUrl) throws YarnException,
      IOException {
    synchronized (unregisterHeartbeatLock) {
      keepRunning = false;
      client.unregisterApplicationMaster(appStatus, appMessage, appTrackingUrl);
    }
    if (inbox != null) inbox.stop();
  }

  /**
   * Request containers for resources before calling <code>allocate</code>
   * @param req Resource request
   */
  public void addContainerRequest(T req) {
    client.addContainerRequest(req);
  }

  /**
   * Remove previous container request. The previous container request may have 
   * already been sent to the ResourceManager. So even after the remove request 
   * the app must be prepared to receive an allocation for the previous request 
   * even after the remove request
   * @param req Resource request
   */
  public void removeContainerRequest(T req) {
    client.removeContainerRequest(req);
  }

  /**
   * Release containers assigned by the Resource Manager. If the app cannot use
   * the container or wants to give up the container then it can release them.
   * The app needs to make new requests for the released resource capability if
   * it still needs it. eg. it released non-local resources
   * @param containerId
   */
  public void releaseAssignedContainer(ContainerId containerId) {
    client.releaseAssignedContainer(containerId);
  }

  /**
   * Get the currently available resources in the cluster.
   * A valid value is available after a call to allocate has been made
   * @return Currently available resources
   */
  public Resource getAvailableResources() {
    return client.getAvailableResources();
  }

  /**
   * Get the current number of nodes in the cluster.
   * A valid values is available after a call to allocate has been made
   * @return Current number of nodes in the cluster
   */
  public int getClusterNodeCount() {
    return client.getClusterNodeCount();
  }

  /**
   * Update application's blacklist with addition or removal resources.
   *
   * @param blacklistAdditions list of resources which should be added to the
   *        application blacklist
   * @param blacklistRemovals list of resources which should be removed from the
   *        application blacklist
   */
  public void updateBlacklist(List<String> blacklistAdditions,
                              List<String> blacklistRemovals) {
    client.updateBlacklist(blacklistAdditions, blacklistRemovals);
  }
  
  private class HeartbeatThread extends Thread {
    public HeartbeatThread() {
      super("AMRM Heartbeater thread");
    }
    
    public void run() {
      while (true) {
        AllocateResponse response = null;
        // synchronization ensures we don't send heartbeats after unregistering
        synchronized (unregisterHeartbeatLock) {
          if (!keepRunning) {
            return;
          }

          try {
            response = client.allocate(progress);
          } catch (ApplicationAttemptNotFoundException e) {
            handler.onShutdownRequest();
            LOG.info("Shutdown requested. Stopping callback.");
            return;
          } catch (Throwable ex) {
            LOG.error("Exception on heartbeat", ex);
            savedException = ex;
            // interrupt handler thread in case it waiting on the queue
            handlerThread.interrupt();
            return;
          }
          if (response != null) {
            while (true) {
              try {
                responseQueue.put(response);
                break;
              } catch (InterruptedException ex) {
                LOG.debug("Interrupted while waiting to put on response queue", ex);
              }
            }
          }
        }
        try {
          Thread.sleep(heartbeatIntervalMs.get());
        } catch (InterruptedException ex) {
          LOG.debug("Heartbeater interrupted", ex);
        }
      }
    }
  }
  
  private class CallbackHandlerThread extends Thread {
    public CallbackHandlerThread() {
      super("AMRM Callback Handler Thread");
    }
    
    public void run() {
      while (true) {
        if (!keepRunning) {
          return;
        }
        try {
          AllocateResponse response;
          if(savedException != null) {
            LOG.error("Stopping callback due to: ", savedException);
            handler.onError(savedException);
            return;
          }
          try {
            response = responseQueue.take();
          } catch (InterruptedException ex) {
            LOG.info("Interrupted while waiting for queue", ex);
            continue;
          }
          List<NodeReport> updatedNodes = response.getUpdatedNodes();
          if (!updatedNodes.isEmpty()) {
            handler.onNodesUpdated(updatedNodes);
          }

          List<ContainerStatus> completed =
              response.getCompletedContainersStatuses();
          if (!completed.isEmpty()) {
            handler.onContainersCompleted(completed);
          }

          List<Container> allocated = response.getAllocatedContainers();
          if (!allocated.isEmpty()) {
            handler.onContainersAllocated(allocated);
          }

          progress = handler.getProgress();
        } catch (Throwable ex) {
          handler.onError(ex);
          // re-throw exception to end the thread
          throw new YarnRuntimeException(ex);
        }
      }
    }
  }
}
