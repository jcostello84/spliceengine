/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.metrics;

/**
 * @author Scott Fines
 *         Date: 5/13/14
 */
public abstract class SimpleTimer implements Timer,TimeView {
		protected final TimeMeasure timeMeasure;

		private long numEvents = 0l;

		public SimpleTimer(TimeMeasure timeMeasure) {
				this.timeMeasure = timeMeasure;
		}

		@Override public void startTiming() { timeMeasure.startTime(); }

		@Override public void stopTiming() { timeMeasure.stopTime(); }

		@Override
		public void tick(long numEvents) {
			timeMeasure.stopTime();
				this.numEvents+=numEvents;
		}

		@Override
		public long getNumEvents() {
				return numEvents;
		}

		@Override
		public TimeView getTime() {
				return this;
		}
}