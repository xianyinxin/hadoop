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

package org.apache.hadoop.metrics2.lib;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.SampleEWMA;
import org.apache.hadoop.metrics2.util.SampleStat;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.metrics2.lib.Interns.*;

/**
 * A mutable metric with stats.
 *
 * Useful for keeping throughput/latency stats.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableStat extends MutableMetric {
  private final MetricsInfo numInfo;
  private final MetricsInfo avgInfo;
  private final MetricsInfo stddevInfo;
  private final MetricsInfo iMinInfo;
  private final MetricsInfo iMaxInfo;
  private final MetricsInfo avgHistInfo;
  private final MetricsInfo minInfo;
  private final MetricsInfo maxInfo;
  private final MetricsInfo mAvgInfo;

  private final SampleStat intervalStat = new SampleStat();
  private final SampleStat prevStat = new SampleStat();
  private final SampleStat historicalStat = new SampleStat();
  private long numSamples = 0;
  private boolean extended = false;

  private static final long UPDATE_INTERVAL = 5000L;
  private static final long LOOK_BACK_PERIOD = 60000L;
  private static final TimeUnit UNIT = TimeUnit.MILLISECONDS;
  private boolean enableMovingAvg = false;

  private SampleEWMA ewma;

  private final StatInfo statInfo;
  private final StatInfo histStatInfo;

  private long lastNumSamples = 0;
  private long lastSampleValue = 0;

  /**
   * Construct a sample statistics metric
   * @param name        of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g. "Ops")
   * @param valueName   of the metric (e.g. "Time", "Latency")
   * @param extended    create extended stats (stddev, min/max etc.) by default.
   */
  public MutableStat(String name, String description,
                     String sampleName, String valueName, boolean extended) {
    String ucName = StringUtils.capitalize(name);
    String usName = StringUtils.capitalize(sampleName);
    String uvName = StringUtils.capitalize(valueName);
    String desc = StringUtils.uncapitalize(description);
    String lsName = StringUtils.uncapitalize(sampleName);
    String lvName = StringUtils.uncapitalize(valueName);
    ewma = new SampleEWMA(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
    numInfo = info(ucName +"Num"+ usName, "Number of "+ lsName +" for "+ desc);
    avgInfo = info(ucName +"Avg"+ uvName, "Average "+ lvName +" for "+ desc);
    stddevInfo = info(ucName +"Stdev"+ uvName,
                     "Standard deviation of "+ lvName +" for "+ desc);
    iMinInfo = info(ucName +"IMin"+ uvName,
                    "Interval min "+ lvName +" for "+ desc);
    iMaxInfo = info(ucName + "IMax"+ uvName,
                    "Interval max "+ lvName +" for "+ desc);
    avgHistInfo = info(ucName +"HistAvg"+ uvName,
                        "Historical average "+ lvName +" for "+ desc);
    minInfo = info(ucName +"Min"+ uvName, "Min "+ lvName +" for "+ desc);
    maxInfo = info(ucName +"Max"+ uvName, "Max "+ lvName +" for "+ desc);
    mAvgInfo = info(ucName +"mAvg"+ uvName, "Moving average "+ lvName +" for "+ desc);
    this.extended = extended;
    statInfo = new StatInfo();
    histStatInfo = new StatInfo();
  }

  /**
   * Construct a snapshot stat metric with extended stat off by default
   * @param name        of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g. "Ops")
   * @param valueName   of the metric (e.g. "Time", "Latency")
   */
  public MutableStat(String name, String description,
                     String sampleName, String valueName) {
    this(name, description, sampleName, valueName, false);
  }

  /**
   * Set whether to display the extended stats (stdev, min/max etc.) or not
   * @param extended enable/disable displaying extended stats
   */
  public synchronized void setExtended(boolean extended) {
    this.extended = extended;
  }

  /**
   * Enable moving average calculation which adopts the default params.
   */
  public void enableMovingAvg() {
    this.enableMovingAvg = true;
    if (ewma == null) {
      ewma = new SampleEWMA(UPDATE_INTERVAL, LOOK_BACK_PERIOD, UNIT);
    }
  }

  /**
   * Enable moving average with user's params.
   * @param updateInterval of the moving average calculation
   * @param lookBackPeriod that the moving average was calculated over
   * @param unit of time
   */
  public void enableMovingAvg(final long updateInterval,
                              final long lookBackPeriod, final TimeUnit unit) {
    this.enableMovingAvg = true;
    ewma = new SampleEWMA(updateInterval, lookBackPeriod, unit);
  }

  /**
   * Stop displaying moving average.
   */
  public void disableMovingAvg() {
    this.enableMovingAvg = false;
  }

  /**
   * Add a number of samples and their sum to the running stat
   * @param numSamples  number of samples
   * @param sum of the samples
   */
  public synchronized void add(long numSamples, long sum) {
    intervalStat.add(numSamples, sum);
    historicalStat.add(numSamples, sum);
    lastNumSamples = numSamples;
    lastSampleValue = sum;
    ewma.update(((double) sum) / numSamples);
    setChanged();
  }

  /**
   * Add a snapshot to the metric
   * @param value of the metric
   */
  public synchronized void add(long value) {
    intervalStat.add(value);
    historicalStat.add(value);
    lastNumSamples = 1;
    lastSampleValue = value;
    ewma.update(value);
    setChanged();
  }

  @Override
  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      numSamples += intervalStat.numSamples();
      builder.addCounter(numInfo, numSamples)
             .addGauge(avgInfo, lastStat().mean());
      if (extended) {
        builder.addGauge(stddevInfo, lastStat().stddev())
               .addGauge(iMinInfo, lastStat().min())
               .addGauge(iMaxInfo, lastStat().max())
               .addGauge(avgHistInfo, historicalStat.mean())
               .addGauge(minInfo, historicalStat.min())
               .addGauge(maxInfo, historicalStat.max());
      }
      if (enableMovingAvg) {
        builder.addGauge(mAvgInfo, ewma.getCurrentValue());
      }
      if (changed()) {
        if (numSamples > 0) {
          intervalStat.copyTo(prevStat);
          intervalStat.reset();
        }
        clearChanged();
      }
    }
  }

  private SampleStat lastStat() {
    return changed() ? intervalStat : prevStat;
  }

  /**
   * Reset the all time stat of the metric
   */
  public void resetHistoricalStat() {
    historicalStat.reset();
  }

  /**
   * An information class that wraps the statistic info of a SchedulerMutableRate, including
   * the all time average, standard deviation, max, min, last value and moving average.
   */
  public static class StatInfo {
    private double avg;
    private double stddev;
    private double max;
    private double min;
    private double lastValue;
    private long lastNumSamples;
    private double mAvg;
    private long numSamples;

    public StatInfo() {}

    public StatInfo(double avg, double stddev, double max, double min,
                    double lastValue, long lastNumSamples, long numSamples, double mAvg) {
      this.avg = avg;
      this.stddev = stddev;
      this.max = max;
      this.min = min;
      this.lastValue = lastValue;
      this.lastNumSamples = lastNumSamples;
      this.numSamples = numSamples;
      this.mAvg = mAvg;
    }

    // private setters so that others can not modify the values
    private void setAvg(double avg) {
      this.avg = avg;
    }
    private void setStddev(double stddev) {
      this.stddev = stddev;
    }
    private void setMax(double max) {
      this.max = max;
    }
    private void setMin(double min) {
      this.min = min;
    }
    private void setLastValue(double value) {
      this.lastValue = value;
    }
    private void setLastNumSamples(long lastNumSamples) {
      this.lastNumSamples = lastNumSamples;
    }
    private void setNumSamples(long numSamples) {
      this.numSamples = numSamples;
    }
    private void setMovingAvg(double mAvg) {
      this.mAvg = mAvg;
    }

    public double avg() {
      return avg;
    }
    public double stddev() {
      return stddev;
    }
    public double max() {
      return max;
    }
    public double min() {
      return min;
    }
    public double lastValue() {
      return this.lastValue;
    }
    public long lastNumSamples() {
      return this.lastNumSamples;
    }
    public long numSamples() {
      return this.numSamples;
    }
    public double movingAvg() {
      return this.mAvg;
    }
  }

  /**
   * Light-weighed method gather the last stat info.
   * @return an object that contains stat info, and null if there's no stat info.
   */
  public synchronized StatInfo getLastStatInfo() {
    if (lastNumSamples == 0)
      return null;
    statInfo.setAvg(lastStat().mean());
    statInfo.setStddev(lastStat().stddev());
    statInfo.setMax(lastStat().max());
    statInfo.setMin(lastStat().min());
    statInfo.setLastValue(lastSampleValue);
    statInfo.setLastNumSamples(lastNumSamples);
    statInfo.setNumSamples(lastStat().numSamples());
    statInfo.setMovingAvg(ewma.getCurrentValue());
    return statInfo;
  }

  /**
   * Light-weighed method to gather the all time stat info.
   * @return an object that contains stat info, and null if there's no stat info.
   */
  public synchronized StatInfo getHistStatInfo() {
    if (lastNumSamples == 0)
      return null;
    histStatInfo.setAvg(historicalStat.mean());
    histStatInfo.setStddev(historicalStat.stddev());
    histStatInfo.setMax(historicalStat.max());
    histStatInfo.setMin(historicalStat.min());
    histStatInfo.setLastValue(lastSampleValue);
    histStatInfo.setLastNumSamples(lastNumSamples);
    histStatInfo.setNumSamples(historicalStat.numSamples());
    histStatInfo.setMovingAvg(ewma.getCurrentValue());
    return histStatInfo;
  }

  @Override
  public String toString() {
    return lastStat().toString();
  }
}
