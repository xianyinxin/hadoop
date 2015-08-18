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

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.exp;
import static org.junit.Assert.assertEquals;

public class TestSampleEWMA {
  private static final double EPSILON = 1e-8;

  @Test
  public void TestSimple() throws Exception {
    final long intervalMs = 1000;
    final long periodMs = 300000;
    final long milliSecondsPerMinute = 60000;
    SampleEWMA ewma = new SampleEWMA(intervalMs, periodMs, TimeUnit.MILLISECONDS);
    final double alpha =
        1 - exp(-((double)intervalMs) / TimeUnit.MILLISECONDS.toMinutes(periodMs) / milliSecondsPerMinute);
    double expected = 0.0, actual = 0.0;

    assertEquals("rate", 0.0, ewma.getCurrentValue(), EPSILON);

    ewma.update(20);
    assertEquals("rate", 20.0, ewma.getCurrentValue(), EPSILON);

    Thread.sleep(intervalMs);
    expected = 20.0;
    assertEquals("rate", expected, ewma.getCurrentValue(), EPSILON);

    ewma.update(30);
    expected = 30.0 * alpha + (1 - alpha) * expected;
    actual = ewma.getCurrentValue();
    assertEquals("rate", expected, actual, EPSILON);

    ewma.update(25);
    assertEquals("rate", expected, actual, EPSILON);

    Thread.sleep(intervalMs);
    ewma.update(36);
    expected = 36.0 * alpha + (1 - alpha) * expected;
    assertEquals("rate", expected, ewma.getCurrentValue(), EPSILON);

    Thread.sleep(2 * intervalMs);
    ewma.update(27);
    expected = 27.0 * alpha + (1 - alpha) * expected;
    expected = 27.0 * alpha + (1 - alpha) * expected;
    assertEquals("rate", expected, ewma.getCurrentValue(), EPSILON);
  }
}
