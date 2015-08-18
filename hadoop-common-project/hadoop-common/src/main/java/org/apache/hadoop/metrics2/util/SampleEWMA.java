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

package org.apache.hadoop.metrics2.util;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.hadoop.classification.InterfaceAudience;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.exp;

/**
 * Helper to compute exponentially weighted moving average(EWMA), which will generate
 * an exponentially moving average over a period of time.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
@InterfaceAudience.Private
public class SampleEWMA {
  private AtomicDouble sample;
  private static final long DEFAULT_INTERVAL_MS = 5000L;
  private static final long MILLISECONDS_PER_MINUTE = 60000L;
  private static final long DEFAULT_PERIOD_MS = 60000L;  // 1 minute
  private final long intervalMs;
  private final double periodMin;
  private final double alpha;

  private volatile double mAvg = 0.0;
  private volatile boolean initialized = false;

  private AtomicLong lastTick;

  public SampleEWMA() {
    this(DEFAULT_INTERVAL_MS, DEFAULT_PERIOD_MS, TimeUnit.MILLISECONDS);
  }

  public SampleEWMA(long interval, long period, TimeUnit unit) {
    this.intervalMs = unit.toMillis(interval);
    this.periodMin = unit.toSeconds(period) / 60.0;
    this.alpha = 1 - exp(- ((double) intervalMs) / MILLISECONDS_PER_MINUTE / periodMin);

    sample = new AtomicDouble(0.0);
    lastTick = new AtomicLong(System.currentTimeMillis());
  }

  private void tick() {
    final double instantValue = sample.get();
    mAvg += (alpha * (instantValue - mAvg));
  }

  public void update(double value) {
    if (!initialized) {
      mAvg = value;
      initialized = true;
      lastTick.set(System.currentTimeMillis());
      return;
    }
    final long oldTick = lastTick.get();
    final long newTick = System.currentTimeMillis();
    final long duration = newTick - oldTick;
    sample.getAndSet(value);
    if (duration >= intervalMs) {
      final long newIntervalStartTick = newTick - duration % intervalMs;
      if (lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
        final long requiredTicks = duration / intervalMs;
        for (long i = 0; i < requiredTicks; i++)
          tick();
      }
    }
  }

  public double getCurrentValue() {
    return mAvg;
  }
}
