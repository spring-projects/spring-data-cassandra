/*
 * Copyright 2024-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.observation.Observation;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;

/**
 * Unit tests for {@link ObservationRequestTracker}.
 *
 * @author Mark Paluch
 */
class ObservationRequestTrackerUnitTests {

	@Test // GH-1541
	void shouldStopObservation() {

		Request request = mockRequest(null);

		ObservationRequestTracker.INSTANCE.onSuccess(request, 0, null, null, "");

		verify(((CassandraObservationSupplier) request).getObservation()).stop();
	}

	@Test // GH-1541
	void shouldAssociateNodeWithContext() {

		CassandraObservationContext context = new CassandraObservationContext(null, "foo", false, "foo", "foo", "bar");

		Request request = mockRequest(context);
		InternalDriverContext driverContext = mock(InternalDriverContext.class, Answers.RETURNS_MOCKS);

		DefaultNode node = new DefaultNode(new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 1234)),
				driverContext);
		ObservationRequestTracker.INSTANCE.onNodeSuccess(request, 0, null, node, "");

		assertThat(context.getNode()).isEqualTo(node);
	}

	@Test // GH-1541
	void noOpObservationShouldNotAssociateContext() {

		CassandraObservationContext context = new CassandraObservationContext(null, "foo", false, "foo", "foo", "bar");
		Request request = mockRequest(context, observation -> {
			when(observation.isNoop()).thenReturn(true);
		});
		InternalDriverContext driverContext = mock(InternalDriverContext.class, Answers.RETURNS_MOCKS);

		DefaultNode node = new DefaultNode(new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 1234)),
				driverContext);
		ObservationRequestTracker.INSTANCE.onNodeSuccess(request, 0, null, node, "");

		assertThat(context.getNode()).isNull();
	}

	@Test // GH-1541
	void observationWithOtherContextShouldNotAssociateContext() {

		Request request = mockRequest(mock(Observation.Context.class));
		InternalDriverContext driverContext = mock(InternalDriverContext.class, Answers.RETURNS_MOCKS);

		DefaultNode node = new DefaultNode(new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 1234)),
				driverContext);

		assertThatNoException().isThrownBy(() -> {
			ObservationRequestTracker.INSTANCE.onNodeSuccess(request, 0, null, node, "");
		});
	}

	private static Request mockRequest(@Nullable Observation.Context context) {
		return mockRequest(context, observation -> {});
	}

	private static Request mockRequest(@Nullable Observation.Context context,
			Consumer<Observation> observationCustomizer) {

		Request request = mock(Request.class, withSettings().extraInterfaces(CassandraObservationSupplier.class));

		Observation observation = mock(Observation.class);
		CassandraObservationSupplier supplier = (CassandraObservationSupplier) request;
		when(supplier.getObservation()).thenReturn(observation);
		when(observation.getContext()).thenReturn(context);

		observationCustomizer.accept(observation);

		return request;
	}
}
