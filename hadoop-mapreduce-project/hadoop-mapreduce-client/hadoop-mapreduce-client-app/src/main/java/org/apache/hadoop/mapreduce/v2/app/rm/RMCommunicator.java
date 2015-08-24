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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster.RunningAppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.RMNotificationHandler;
import org.apache.hadoop.yarn.api.RMNotificationProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationType;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterNotificationAddressRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterNotificationAddressResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMNotificationInbox;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

/**
 * Registers/unregisters to RM and sends heartbeats to RM.
 */
public abstract class RMCommunicator extends AbstractService
    implements RMHeartbeatHandler {
  private static final Log LOG = LogFactory.getLog(RMCommunicator.class);
  private int rmPollInterval;//millis
  private int rmEventBasedPollInterval;
  private int configuredRmEventBasedPollInterval;
  private boolean rmNeedUpdate;
  private boolean rmEventBasedPollEnabled;
  private Object rmPollMonitor;
  protected ApplicationId applicationId;
  private final AtomicBoolean stopped;
  protected Thread allocatorThread;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;
  protected ApplicationMasterProtocol scheduler;
  private final ClientService clientService;
  private Resource maxContainerCapability;
  protected Map<ApplicationAccessType, String> applicationACLs;
  private volatile long lastHeartbeatTime;
  private ConcurrentLinkedQueue<Runnable> heartbeatCallbacks;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private final AppContext context;
  private Job job;
  // Has a signal (SIGTERM etc) been issued?
  protected volatile boolean isSignalled = false;
  private volatile boolean shouldUnregister = true;
  private boolean isApplicationMasterRegistered = false;

  private EnumSet<SchedulerResourceTypes> schedulerResourceTypes;

  private RMNotificationInbox inbox = null;

  public RMCommunicator(ClientService clientService, AppContext context) {
    super("RMCommunicator");
    this.clientService = clientService;
    this.context = context;
    this.eventHandler = context.getEventHandler();
    this.applicationId = context.getApplicationID();
    this.stopped = new AtomicBoolean(false);
    this.heartbeatCallbacks = new ConcurrentLinkedQueue<Runnable>();
    this.schedulerResourceTypes = EnumSet.of(SchedulerResourceTypes.MEMORY);
    this.rmPollMonitor = new Object();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    rmPollInterval =
        conf.getInt(MRJobConfig.MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS,
            MRJobConfig.DEFAULT_MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS);
    rmEventBasedPollEnabled =
        conf.getBoolean(MRJobConfig.MR_AM_TO_RM_EVENT_BASED_HEARTBEAT_ENABLED,
            MRJobConfig.DEFAULT_MR_AM_TO_RM_EVENT_BASED_HEARTBEAT_ENABLED);
    if (rmEventBasedPollEnabled) {
      configuredRmEventBasedPollInterval =
          conf.getInt(MRJobConfig.MR_AM_TO_RM_EVENT_BASED_HEARTBEAT_INTERVAL_MS,
              MRJobConfig.DEFAULT_MR_AM_TO_RM_EVENT_BASED_HEARTBEAT_INTERVAL_MS);
      rmEventBasedPollInterval = configuredRmEventBasedPollInterval;
      // check configuredRmEventBasedPollInterval and rmPollInterval
      if (configuredRmEventBasedPollInterval >= rmPollInterval) {
        rmEventBasedPollEnabled = false;
        LOG.warn("Event-based heartbeat is disabled because the event-based heartbeat interval "
            + configuredRmEventBasedPollInterval + " is equal to or larger than regular heartbeat interval "
            + rmPollInterval);
      } else {
        LOG.info("Enable and initialize event-based heartbeat interval to " +
            configuredRmEventBasedPollInterval + " ms");
      }
    }
    rmNeedUpdate = true;
  }

  @Override
  protected void serviceStart() throws Exception {
    scheduler= createSchedulerProxy();
    JobID id = TypeConverter.fromYarn(this.applicationId);
    JobId jobId = TypeConverter.toYarn(id);
    job = context.getJob(jobId);
    register();
    startAllocatorThread();
    super.serviceStart();
  }

  protected AppContext getContext() {
    return context;
  }

  protected Job getJob() {
    return job;
  }

  /**
   * Get the appProgress. Can be used only after this component is started.
   * @return the appProgress.
   */
  protected float getApplicationProgress() {
    // For now just a single job. In future when we have a DAG, we need an
    // aggregate progress.
    return this.job.getProgress();
  }

  protected void register() {
    //Register
    InetSocketAddress serviceAddr = null;
    if (clientService != null ) {
      serviceAddr = clientService.getBindAddress();
    }
    try {
      RegisterApplicationMasterRequest request =
        recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      if (serviceAddr != null) {
        request.setHost(serviceAddr.getHostName());
        request.setRpcPort(serviceAddr.getPort());
        request.setTrackingUrl(MRWebAppUtil
            .getAMWebappScheme(getConfig())
            + serviceAddr.getHostName() + ":" + clientService.getHttpPort());
      }
      RegisterApplicationMasterResponse response =
        scheduler.registerApplicationMaster(request);
      isApplicationMasterRegistered = true;
      maxContainerCapability = response.getMaximumResourceCapability();
      this.context.getClusterInfo().setMaxContainerCapability(
          maxContainerCapability);
      if (UserGroupInformation.isSecurityEnabled()) {
        setClientToAMToken(response.getClientToAMTokenMasterKey());        
      }
      this.applicationACLs = response.getApplicationACLs();
      LOG.info("maxContainerCapability: " + maxContainerCapability);
      String queue = response.getQueue();
      LOG.info("queue: " + queue);
      job.setQueueName(queue);
      this.schedulerResourceTypes.addAll(response.getSchedulerResourceTypes());

      // Init and start notification inbox
      if (getConfig().getBoolean(MRJobConfig.MR_RM_NOTIFICATION_ENABLED,
          MRJobConfig.DEFAULT_MR_RM_NOTIFICATION_ENABLED)) {
        initNotificationInbox();
      }
    } catch (Exception are) {
      LOG.error("Exception while registering", are);
      throw new YarnRuntimeException(are);
    }
  }

  private void initNotificationInbox() {
    RegisterNotificationAddressRequest request =
        recordFactory
            .newRecordInstance(RegisterNotificationAddressRequest.class);
    request.setHost(clientService.getBindAddress().getHostName());
    Configuration.IntegerRanges portsRange;
    portsRange =
        getConfig().getRange(MRJobConfig.MR_RM_NOTIFICATION_PROTOCOL_PORTS_POOL,
            MRJobConfig.DEFAULT_MR_RM_NOTIFICATION_PROTOCOL_PORTS_POOL);
    int port = -1;
    for (Integer aPort : portsRange) {
      if (isPortAvailable(aPort)) {
        port = aPort;
        break;
      }
    }
    if (port != -1) {
      request.setNotificationPort(port);
      try {
        RegisterNotificationAddressResponse response =
            scheduler.registerNotificationAddress(request);
        if (response.getIfSuccess()) {
          inbox = new RMNotificationInbox(RMNotificationProtocol.class,
              new InetSocketAddress(request.getHost(),
                  request.getNotificationPort()),
              getConfig(), new MRNotificationHandler());
          inbox.start();
          LOG.info("Initialized notification inbox with address: "
              + request.getHost() + ", " + request.getNotificationPort());
        } else {
          LOG.warn("Register notification address failed: "
              + response.getDiagnostics());
        }
      } catch (Exception e) {
        LOG.warn("Exception while initialize notification inbox: " + e);
      }
    } else {
      LOG.warn("Register notification address failed: no port can be bind.");
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

  public class MRNotificationHandler implements RMNotificationHandler {

    @Override
    public NotificationResponse handle(NotificationRequest notification) {
      if (notification.getNotificationType() == NotificationType.RM_CONTAINER_ALLOCATED) {
        notifyAllocatorThread();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received notification from RM: " + notification.getNotificationType());
        }
      }
      LOG.info("Received notification from RM: " + notification.getNotificationType());
      NotificationResponse response =
          RecordFactoryProvider.getRecordFactory(null)
              .newRecordInstance(NotificationResponse.class);
      response.setNotificationReceived(true);
      return response;
    }
  }

  private void setClientToAMToken(ByteBuffer clientToAMTokenMasterKey) {
    byte[] key = clientToAMTokenMasterKey.array();
    context.getClientToAMTokenSecretManager().setMasterKey(key);
  }

  protected void unregister() {
    try {
      doUnregistration();
    } catch(Exception are) {
      LOG.error("Exception while unregistering ", are);
      // if unregistration failed, isLastAMRetry needs to be recalculated
      // to see whether AM really has the chance to retry
      RunningAppContext raContext = (RunningAppContext) context;
      raContext.resetIsLastAMRetry();
    }
  }

  @VisibleForTesting
  protected void doUnregistration()
      throws YarnException, IOException, InterruptedException {
    FinalApplicationStatus finishState = FinalApplicationStatus.UNDEFINED;
    JobImpl jobImpl = (JobImpl)job;
    if (jobImpl.getInternalState() == JobStateInternal.SUCCEEDED) {
      finishState = FinalApplicationStatus.SUCCEEDED;
    } else if (jobImpl.getInternalState() == JobStateInternal.KILLED
        || (jobImpl.getInternalState() == JobStateInternal.RUNNING && isSignalled)) {
      finishState = FinalApplicationStatus.KILLED;
    } else if (jobImpl.getInternalState() == JobStateInternal.FAILED
        || jobImpl.getInternalState() == JobStateInternal.ERROR) {
      finishState = FinalApplicationStatus.FAILED;
    }
    StringBuffer sb = new StringBuffer();
    for (String s : job.getDiagnostics()) {
      sb.append(s).append("\n");
    }
    LOG.info("Setting job diagnostics to " + sb.toString());

    String historyUrl =
        MRWebAppUtil.getApplicationWebURLOnJHSWithScheme(getConfig(),
            context.getApplicationID());
    LOG.info("History url is " + historyUrl);
    FinishApplicationMasterRequest request =
        FinishApplicationMasterRequest.newInstance(finishState,
          sb.toString(), historyUrl);
    try {
      while (true) {
        FinishApplicationMasterResponse response =
            scheduler.finishApplicationMaster(request);
        if (response.getIsUnregistered()) {
          // When excepting ClientService, other services are already stopped,
          // it is safe to let clients know the final states. ClientService
          // should wait for some time so clients have enough time to know the
          // final states.
          RunningAppContext raContext = (RunningAppContext) context;
          raContext.markSuccessfulUnregistration();
          break;
        }
        LOG.info("Waiting for application to be successfully unregistered.");
        if (inbox != null) inbox.stop();
        Thread.sleep(rmPollInterval);
      }
    } catch (ApplicationMasterNotRegisteredException e) {
      // RM might have restarted or failed over and so lost the fact that AM had
      // registered before.
      register();
      doUnregistration();
    }
  }

  protected Resource getMaxContainerCapability() {
    return maxContainerCapability;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    if (allocatorThread != null) {
      allocatorThread.interrupt();
      try {
        allocatorThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("InterruptedException while stopping", ie);
      }
    }
    if (isApplicationMasterRegistered && shouldUnregister) {
      unregister();
    }
    super.serviceStop();
  }

  protected void startAllocatorThread() {
    allocatorThread = new Thread(new Runnable() {
      private void waitHeartbeatInterval(int minInterval, int maxInterval) throws InterruptedException {
        synchronized(rmPollMonitor) {
          long start = System.currentTimeMillis();
          long waitInterval = maxInterval;
          if(rmNeedUpdate == true) {
            rmNeedUpdate = false;
            waitInterval = minInterval;
          }
          while(waitInterval > 0) {
            rmPollMonitor.wait(waitInterval);
            if(rmNeedUpdate == true) {
              long timeElapse = System.currentTimeMillis() - start;
              rmNeedUpdate = false;
              if(timeElapse < minInterval) {
                waitInterval = minInterval - timeElapse;
              } else {
                waitInterval = 0;
              }
            } else {
              waitInterval = 0;
            }
          }
        }
      }
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            if(rmEventBasedPollEnabled == true) {
              waitHeartbeatInterval(rmEventBasedPollInterval, rmPollInterval);
            } else {
              Thread.sleep(rmPollInterval);
            }
            try {
              heartbeat();
            } catch (YarnRuntimeException e) {
              LOG.error("Error communicating with RM: " + e.getMessage() , e);
              return;
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
              continue;
              // TODO: for other exceptions
            }

            lastHeartbeatTime = context.getClock().getTime();
            executeHeartbeatCallbacks();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Allocated thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });
    allocatorThread.setName("RMCommunicator Allocator");
    allocatorThread.start();
  }

  protected ApplicationMasterProtocol createSchedulerProxy() {
    final Configuration conf = getConfig();

    try {
      return ClientRMProxy.createRMProxy(conf, ApplicationMasterProtocol.class);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  protected abstract void heartbeat() throws Exception;

  protected void notifyAllocatorThread() {
    if(rmEventBasedPollEnabled == true) {
      synchronized(rmPollMonitor) {
        rmNeedUpdate = true;
        rmPollMonitor.notify();
      }
    }
  }

  private void executeHeartbeatCallbacks() {
    Runnable callback = null;
    while ((callback = heartbeatCallbacks.poll()) != null) {
      callback.run();
    }
  }

  @Override
  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  @Override
  public void runOnNextHeartbeat(Runnable callback) {
    heartbeatCallbacks.add(callback);
  }

  public void setShouldUnregister(boolean shouldUnregister) {
    this.shouldUnregister = shouldUnregister;
    LOG.info("RMCommunicator notified that shouldUnregistered is: " 
        + shouldUnregister);
  }
  
  public void setSignalled(boolean isSignalled) {
    this.isSignalled = isSignalled;
    LOG.info("RMCommunicator notified that isSignalled is: " 
        + isSignalled);
  }

  protected void updateHeartbeatInterval(int nextHeartbeatInterval, boolean flag) {
    rmPollInterval = nextHeartbeatInterval;
    if (rmEventBasedPollEnabled) {
      if (flag)
        rmEventBasedPollInterval = nextHeartbeatInterval;
      else
        rmEventBasedPollInterval =
            configuredRmEventBasedPollInterval < nextHeartbeatInterval ?
                configuredRmEventBasedPollInterval : nextHeartbeatInterval;
    }
  }

  @VisibleForTesting
  protected boolean isApplicationMasterRegistered() {
    return isApplicationMasterRegistered;
  }

  public EnumSet<SchedulerResourceTypes> getSchedulerResourceTypes() {
    return schedulerResourceTypes;
  }
}
