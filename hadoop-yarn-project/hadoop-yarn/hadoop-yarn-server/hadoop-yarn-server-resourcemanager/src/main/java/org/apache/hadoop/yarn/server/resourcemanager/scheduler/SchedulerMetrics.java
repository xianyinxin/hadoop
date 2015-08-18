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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.metrics2.lib.Interns.info;


@InterfaceAudience.Private
@Metrics(context="yarn")
public class SchedulerMetrics {

  private static final AtomicBoolean isInitialized = new AtomicBoolean(false);

  private static final MetricsInfo RECORD_INFO =
      info("SchedulerMetrics", "Metrics for the Yarn Scheduler");
  private static volatile SchedulerMetrics INSTANCE = null;
  private static MetricsRegistry registry;

  private static YarnScheduler scheduler;
  private static QueueMetrics rootMetric;

  private final Timer timer= new Timer("Scheduler metrics updater", true);
  private static final long TIMER_START_DELAY_MS = 5000L;

  public enum SchedulerLoad {
    LIGHT, NORMAL, BUSY, HEAVY, UNKNOWN
  }
  private static SchedulerLoad schedulerLoad;

  private static final long UPDATE_INTERVAL = 1000L;
  private static final long LOOK_BACK_PERIOD = 10000L;
  private static final TimeUnit UNIT = TimeUnit.MILLISECONDS;

  public enum SchedulerEventOp {
    ADDED, HANDLED
  }

  @Metric("# of waiting scheduler events") MutableGaugeInt numWaitingEvents;
  @Metric("# of waiting node_add events") MutableGaugeInt numWaitingNodeAddEvents;
  @Metric("# of waiting node_remove events") MutableGaugeInt numWaitingNodeRemoveEvents;
  @Metric("# of waiting node_update events") MutableGaugeInt numWaitingNodeUpdateEvents;
  @Metric("# of waiting node_resource_update events") MutableGaugeInt numWaitingNodeResourceUpdateEvents;
  @Metric("# of waiting node_labels_update events") MutableGaugeInt numWaitingNodeLabelsUpdateEvents;
  @Metric("# of waiting app_add events") MutableGaugeInt numWaitingAppAddEvents;
  @Metric("# of waiting app_remove events") MutableGaugeInt numWaitingAppRemoveEvents;
  @Metric("# of waiting attempt_add events") MutableGaugeInt numWaitingAttemptAddEvents;
  @Metric("# of waiting attempt_remove events") MutableGaugeInt numWaitingAttemptRemoveEvents;
  @Metric("# of waiting container_expired events") MutableGaugeInt numWaitingContainerExpiredEvents;
  @Metric("# of waiting drop_reservation events") MutableGaugeInt numWaitingDropReservationEvents;
  @Metric("# of waiting preempt_container events") MutableGaugeInt numWaitingPreemptContainerEvents;
  @Metric("# of waiting kill_container events") MutableGaugeInt numWaitingKillContainerEvents;

  @Metric("Stat of waiting scheduler events") MutableStat numWaitingEventsStat;
  @Metric("Stat of waiting node_update events") MutableStat numWaitingNodeUpdateEventsStat;

  @Metric("Rate of scheduler events handled") MutableRate eventsHandlingRate;
  @Metric("Rate of node_update events handled") MutableRate nodeUpdateHandlingRate;

  @Metric("Rate of scheduler events added") MutableRate eventsAddingRate;
  @Metric("Rate of node update events added") MutableRate nodeUpdateAddingRate;

  @Metric("Rate of container allocation") MutableRate containerAllocationRate;
  // schedulingExecRate equals nodeUpdateEventsHandlingRate
  // if asynchronously scheduling (continuous scheduling in fair) is disabled.
  @Metric("Rate that scheduling be executed") MutableRate schedulingExecRate;
  @Metric("Latency of app allocate") MutableStat appAllocateDurationStat;
  @Metric("Latency of node update") MutableStat nodeUpdateDurationStat;
  @Metric("Duration of scheduling call") MutableStat schedulingDurationStat;

  // Stat of scheduling duration accumulation within a second, which can reflect the scheduler load.
  @Metric("Stat of scheduling duration accumulation within a second") MutableRate schedulingAccumulationStat;
  // Records the scheduling duration accumulation with a second.
  private AtomicLong schedulingAccumulation = new AtomicLong(0);
  // Counter that counts the times of scheduling method be called.
  private AtomicInteger schedulingExecCounter = new AtomicInteger(0);

  /**
   * The elements of the counter are for
   * 0: sum of all events
   * 1: NODE_ADDED
   * 2: NODE_REMOVED
   * 3: NODE_UPDATE
   * 4: NODE_RESOURCE_UPDATE
   * 5: NODE_LABELS_UPDATE
   * 6: APP_ADDED
   * 7: APP_REMOVED
   * 8: APP_ATTEMPT_ADDED
   * 9: APP_ATTEMPT_REMOVED
   * 10: CONTAINER_EXPIRED
   * 11: DROP_RESERVATION
   * 12: PREEMPT_CONTAINER
   * 13: KILL_CONTAINER
   */
  private static final int NUM_SCHEDULER_EVENTS_TYPES = SchedulerEventType.values().length;
  private final AtomicInteger[] eventsAddingCounter = new AtomicInteger[NUM_SCHEDULER_EVENTS_TYPES];
  private final AtomicInteger[] eventsTakingCounter = new AtomicInteger[NUM_SCHEDULER_EVENTS_TYPES];


  // SchedulerMetrics should be a singleton.
  private SchedulerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register("SchedulerMetrics", "Metrics for the Yarn Scheduler",
          this);
    }
    rootMetric = scheduler.getRootQueueMetrics();
    schedulerLoad = SchedulerLoad.NORMAL;
    for (int i = 0; i < NUM_SCHEDULER_EVENTS_TYPES; i++) {
      eventsAddingCounter[i] = new AtomicInteger(0);
      eventsTakingCounter[i] = new AtomicInteger(0);
    }
    startTimerTask();
  }

  private static final Log LOG = LogFactory.getLog(SchedulerMetrics.class);

  public synchronized static void initMetrics(YarnScheduler sched) {
    if (!isInitialized.get()) {
      scheduler = sched;
      if(INSTANCE == null){
        INSTANCE = new SchedulerMetrics();
        INSTANCE.enableMovingAvg();
        isInitialized.set(true);
      }
      LOG.info("SchedulerMetrics initialized.");
    } else {
      LOG.info("SchedulerMetrics has already been initialized.");
    }
  }

  public synchronized static void reInitMetrics(YarnScheduler sched) {
    destroyMetrics();
    initMetrics(sched);
    LOG.info("SchedulerMetrics reinitialized.");
  }

  public synchronized static void destroyMetrics() {
    isInitialized.set(false);
    scheduler = null;
    INSTANCE.stopTimerTask();
    INSTANCE = null;
  }

  public static SchedulerMetrics getMetrics() {
    if(!isInitialized.get()) {
      LOG.error("SchedulerMetrics hasn't been initialized, please initialize it first.");
    }
    return INSTANCE;
  }

  private synchronized void enableMovingAvg() {
    numWaitingEventsStat.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
    numWaitingNodeUpdateEventsStat.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);

    eventsHandlingRate.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
    nodeUpdateHandlingRate.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);

    eventsAddingRate.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
    nodeUpdateAddingRate.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);

    containerAllocationRate.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
    appAllocateDurationStat.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
    nodeUpdateDurationStat.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
    schedulingDurationStat.enableMovingAvg(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
  }

  public void handle(SchedulerEvent event, SchedulerEventOp opt) {
    switch (opt) {
      case ADDED:
        switch (event.getType()) {
          case NODE_ADDED:
            eventsAddingCounter[1].incrementAndGet();
            numWaitingNodeAddEvents.incr();
            break;
          case NODE_REMOVED:
            eventsAddingCounter[2].incrementAndGet();
            numWaitingNodeRemoveEvents.incr();
            break;
          case NODE_UPDATE:
            eventsAddingCounter[3].incrementAndGet();
            numWaitingNodeUpdateEvents.incr();
            break;
          case NODE_RESOURCE_UPDATE:
            eventsAddingCounter[4].incrementAndGet();
            numWaitingNodeResourceUpdateEvents.incr();
            break;
          case NODE_LABELS_UPDATE:
            eventsAddingCounter[5].incrementAndGet();
            numWaitingNodeLabelsUpdateEvents.incr();
            break;
          case APP_ADDED:
            eventsAddingCounter[6].incrementAndGet();
            numWaitingAppAddEvents.incr();
            break;
          case APP_REMOVED:
            eventsAddingCounter[7].incrementAndGet();
            numWaitingAppRemoveEvents.incr();
            break;
          case APP_ATTEMPT_ADDED:
            eventsAddingCounter[8].incrementAndGet();
            numWaitingAttemptAddEvents.incr();
            break;
          case APP_ATTEMPT_REMOVED:
            eventsAddingCounter[9].incrementAndGet();
            numWaitingAttemptRemoveEvents.incr();
            break;
          case CONTAINER_EXPIRED:
            eventsAddingCounter[10].incrementAndGet();
            numWaitingContainerExpiredEvents.incr();
            break;
          case DROP_RESERVATION:
            eventsAddingCounter[11].incrementAndGet();
            numWaitingDropReservationEvents.incr();
            break;
          case PREEMPT_CONTAINER:
            eventsAddingCounter[12].incrementAndGet();
            numWaitingPreemptContainerEvents.incr();
            break;
          case KILL_CONTAINER:
            eventsAddingCounter[13].incrementAndGet();
            numWaitingKillContainerEvents.incr();
            break;
          default:
            LOG.error("Unknown scheduler event type: " + event.getType());
        }
        eventsAddingCounter[0].incrementAndGet();
        numWaitingEvents.incr();
        break;
      case HANDLED:
        switch (event.getType()) {
          case NODE_ADDED:
            eventsTakingCounter[1].incrementAndGet();
            numWaitingNodeAddEvents.decr();
            break;
          case NODE_REMOVED:
            eventsTakingCounter[2].incrementAndGet();
            numWaitingNodeRemoveEvents.decr();
            break;
          case NODE_UPDATE:
            eventsTakingCounter[3].incrementAndGet();
            numWaitingNodeUpdateEvents.decr();
            break;
          case NODE_RESOURCE_UPDATE:
            eventsTakingCounter[4].incrementAndGet();
            numWaitingNodeResourceUpdateEvents.decr();
            break;
          case NODE_LABELS_UPDATE:
            eventsTakingCounter[5].incrementAndGet();
            numWaitingNodeLabelsUpdateEvents.decr();
            break;
          case APP_ADDED:
            eventsTakingCounter[6].incrementAndGet();
            numWaitingAppAddEvents.decr();
            break;
          case APP_REMOVED:
            eventsTakingCounter[7].incrementAndGet();
            numWaitingAppRemoveEvents.decr();
            break;
          case APP_ATTEMPT_ADDED:
            eventsTakingCounter[8].incrementAndGet();
            numWaitingAttemptAddEvents.decr();
            break;
          case APP_ATTEMPT_REMOVED:
            eventsTakingCounter[9].incrementAndGet();
            numWaitingAttemptRemoveEvents.decr();
            break;
          case CONTAINER_EXPIRED:
            eventsTakingCounter[10].incrementAndGet();
            numWaitingContainerExpiredEvents.decr();
            break;
          case DROP_RESERVATION:
            eventsTakingCounter[11].incrementAndGet();
            numWaitingDropReservationEvents.decr();
            break;
          case PREEMPT_CONTAINER:
            eventsTakingCounter[12].incrementAndGet();
            numWaitingPreemptContainerEvents.decr();
            break;
          case KILL_CONTAINER:
            eventsTakingCounter[13].incrementAndGet();
            numWaitingKillContainerEvents.decr();
            break;
          default:
            LOG.error("Unknown scheduler event type: " + event.getType());
        }
        eventsTakingCounter[0].incrementAndGet();
        numWaitingEvents.decr();
        break;
      default:
        LOG.error("Unknown scheduler event options: " + opt.toString());
    }
  }

  private void resetAllCounters() {
    for (int i = 0; i < NUM_SCHEDULER_EVENTS_TYPES; i++) {
      eventsAddingCounter[i].set(0);
      eventsTakingCounter[i].set(0);
    }
  }

  private void startTimerTask() {
    TimerTask metricsUpdateTask = new TimerTask() {
      long lastRecord = rootMetric.getAggregateAllocatedContainers();
      @Override
      public void run() {
        numWaitingEventsStat.add(numWaitingEvents.value());
        numWaitingNodeUpdateEventsStat.add(numWaitingNodeUpdateEvents.value());

        eventsHandlingRate.add(eventsTakingCounter[0].getAndSet(0));
        nodeUpdateHandlingRate.add(eventsTakingCounter[3].getAndSet(0));

        eventsAddingRate.add(eventsAddingCounter[0].getAndSet(0));
        nodeUpdateAddingRate.add(eventsAddingCounter[3].getAndSet(0));

        containerAllocationRate.add(rootMetric.getAggregateAllocatedContainers() - lastRecord);
        lastRecord = rootMetric.getAggregateAllocatedContainers();

        schedulingAccumulationStat.add(schedulingAccumulation.getAndSet(0));
        schedulingExecRate.add(schedulingExecCounter.getAndSet(0));

        evaluateLoad();
      }
    };
    timer.scheduleAtFixedRate(metricsUpdateTask,
        TIMER_START_DELAY_MS, getMetricUpdateIntervalMills());
  }

  private void stopTimerTask() {
    timer.cancel();
  }

  public void evaluateLoad() {
    // If there hasn't been stat data or num of samples is inadequate to evaluate the scheduler load
    if (getNumWaitingEventsStat() == null || getSchedulingAccumulationStat() == null ||
        getSchedulingAccumulationStat().numSamples() < 60) {
      schedulerLoad = SchedulerLoad.UNKNOWN;
      return;
    }

    // We use some statistic info to evaluate the load of the scheduler.
    final double curNumWaitingEvents = getNumWaitingEventsStat().movingAvg();
    final double curSchedulingAccumulation = getSchedulingAccumulationStat().movingAvg();
    final double schedulingCPURatio = curSchedulingAccumulation / getMetricUpdateIntervalMills();

    // For NORMAL and LIGHT scheduler, there should not be scheduler events
    // accumulation in the dispatch queue at the very most time.
    final int numActiveNodes = scheduler.getNumClusterNodes();
    if (curNumWaitingEvents / numActiveNodes < 0.01) {
      if (schedulingCPURatio < 0.1) {
        schedulerLoad = SchedulerLoad.LIGHT;
      } else if (schedulingCPURatio < 0.6) {
        schedulerLoad = SchedulerLoad.NORMAL;
      } else {
        schedulerLoad = SchedulerLoad.BUSY;
      }
    } else if (curNumWaitingEvents / numActiveNodes < 0.1) {
      schedulerLoad = SchedulerLoad.BUSY;
    } else {
      schedulerLoad = SchedulerLoad.HEAVY;
    }
  }

  // Waiting events info in scheduler dispatch queue
  public int getNumWaitingEvents() {
    return numWaitingEvents.value();
  }

  public int getNumWaitingNodeUpdateEvents() {
    return numWaitingNodeUpdateEvents.value();
  }

  // Stat of waiting events
  public MutableRate.StatInfo getNumWaitingEventsStat() {
    return numWaitingEventsStat.getHistStatInfo();
  }

  public MutableRate.StatInfo getNumWaitingNodeUpdateEventsStat() {
    return numWaitingNodeUpdateEventsStat.getHistStatInfo();
  }

  // Scheduler events handling
  public MutableRate.StatInfo getEventsHandlingStat() {
    return eventsHandlingRate.getHistStatInfo();
  }

  // Scheduler events adding
  public MutableRate.StatInfo getEventsAddingStat() {
    return eventsAddingRate.getHistStatInfo();
  }

  // Node Update events handling
  public MutableRate.StatInfo getNodeUpdateHandlingStat() {
    return nodeUpdateHandlingRate.getHistStatInfo();
  }

  // Node update events adding
  public MutableRate.StatInfo getNodeUpdateAddingStat() {
    return nodeUpdateAddingRate.getHistStatInfo();
  }

  // Container allocation
  public void updateContainerAllocationRate(long rate) {
    containerAllocationRate.add(rate);
  }

  public MutableRate.StatInfo getContainerAllocationStat() {
    return containerAllocationRate.getHistStatInfo();
  }

  // App allocate duration
  public void updateAppAllocateDurationStat(long duration) {
    appAllocateDurationStat.add(duration);
  }

  public MutableRate.StatInfo getAppAllocateDurationStat() {
    return appAllocateDurationStat.getHistStatInfo();
  }

  // Node update duration
  public void updateNodeUpdateDurationStat(long duration) {
    nodeUpdateDurationStat.add(duration);
  }

  public MutableRate.StatInfo getNodeUpdateDurationStat() {
    return nodeUpdateDurationStat.getHistStatInfo();
  }

  // Node update duration integral
  public MutableRate.StatInfo getSchedulingAccumulationStat() {
    return schedulingAccumulationStat.getHistStatInfo();
  }

  // Scheduling call duration
  public void updateSchedulingDurationStat(long duration) {
    schedulingDurationStat.add(duration);
    schedulingAccumulation.addAndGet(duration);
    schedulingExecCounter.incrementAndGet();
  }

  public MutableRate.StatInfo getSchedulingDurationStat() {
    return schedulingDurationStat.getHistStatInfo();
  }

  public MutableRate.StatInfo getSchedulingExecRate() {
    return schedulingExecRate.getHistStatInfo();
  }

  // Scheduler load info
  public SchedulerLoad getSchedulerLoad() {
    return schedulerLoad;
  }

  public long getMetricUpdateIntervalMills() {
    return UNIT.toMillis(UPDATE_INTERVAL);
  }
}
