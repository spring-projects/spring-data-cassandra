/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.observability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.cassandra.observability.CassandraObservation.Events;
import org.springframework.data.cassandra.observability.CassandraObservation.HighCardinalityKeyNames;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;

import io.micrometer.observation.Observation;
import io.micrometer.observation.Observation.Event;

/**
 * Trace implementation of the {@link RequestTracker}.
 *
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @since 4.0
 */
public enum ObservationRequestTracker implements RequestTracker {

	/**
	 * Singleton instance of the {@link RequestTracker} to complete {@link Observation}s.
	 */
	INSTANCE;

	private static final Log log = LogFactory.getLog(ObservationRequestTracker.class);

	@Override
	public void onSuccess(Request request, long latencyNanos, DriverExecutionProfile executionProfile, Node node,
			String requestLogPrefix) {

		if (request instanceof CassandraObservationSupplier) {

			Observation observation = ((CassandraObservationSupplier) request).getObservation();

			if (log.isDebugEnabled()) {
				log.debug("Closing observation [" + observation + "]");
			}

			observation.stop();
		}
	}

	@Override
	public void onError(Request request, Throwable error, long latencyNanos, DriverExecutionProfile executionProfile,
			@Nullable Node node, String requestLogPrefix) {

		if (request instanceof CassandraObservationSupplier) {

			Observation observation = ((CassandraObservationSupplier) request).getObservation();
			observation.error(error);

			if (log.isDebugEnabled()) {
				log.debug("Closing observation [" + observation + "]");
			}

			observation.stop();
		}
	}

	@Override
	public void onNodeError(Request request, Throwable error, long latencyNanos, DriverExecutionProfile executionProfile,
			Node node, String requestLogPrefix) {

		if (request instanceof CassandraObservationSupplier) {

			Observation observation = ((CassandraObservationSupplier) request).getObservation();

			((CassandraObservationContext) observation.getContext()).setNode(node);

			observation.highCardinalityKeyValue(
					String.format(HighCardinalityKeyNames.NODE_ERROR_TAG.asString(), node.getEndPoint()), error.toString());
			observation.event(Event.of(Events.NODE_ERROR.getValue()));

			if (log.isDebugEnabled()) {
				log.debug("Marking node error for [" + observation + "]");
			}
		}
	}

	@Override
	public void onNodeSuccess(Request request, long latencyNanos, DriverExecutionProfile executionProfile, Node node,
			String requestLogPrefix) {

		if (request instanceof CassandraObservationSupplier) {

			Observation observation = ((CassandraObservationSupplier) request).getObservation();

			((CassandraObservationContext) observation.getContext()).setNode(node);

			observation.event(Event.of(Events.NODE_SUCCESS.getValue()));

			if (log.isDebugEnabled()) {
				log.debug("Marking node success for [" + observation + "]");
			}
		}
	}

	@Override
	public void close() throws Exception {

	}

}
