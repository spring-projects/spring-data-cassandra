/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping.event;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.With;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Date;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.core.Ordered;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.mapping.context.PersistentEntities;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link ReactiveAuditingEntityCallback}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveAuditingEntityCallbackUnitTests {

	private ReactiveIsNewAwareAuditingHandler handler;
	private ReactiveAuditingEntityCallback callback;

	@BeforeEach
	void setUp() {

		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.getPersistentEntity(Sample.class);

		handler = spy(
				new ReactiveIsNewAwareAuditingHandler(new PersistentEntities(Collections.singletonList(mappingContext))));

		doAnswer(invocation -> Mono.just(invocation.getArgument(0))).when(handler).markCreated(any());
		doAnswer(invocation -> Mono.just(invocation.getArgument(0))).when(handler).markModified(any());

		callback = new ReactiveAuditingEntityCallback(() -> handler);
	}

	@Test // DATACASS-4
	void rejectsNullAuditingHandler() {
		assertThatIllegalArgumentException().isThrownBy(() -> new AuditingEntityCallback(null));
	}

	@Test // DATACASS-4
	void triggersCreationMarkForObjectWithEmptyId() {

		Sample sample = new Sample();
		callback.onBeforeConvert(sample, CqlIdentifier.fromCql("foo"));

		verify(handler, times(1)).markCreated(sample);
		verify(handler, times(0)).markModified(any());
	}

	@Test // DATACASS-4
	void triggersModificationMarkForObjectWithSetId() {

		Sample sample = new Sample();
		sample.id = "id";
		callback.onBeforeConvert(sample, CqlIdentifier.fromCql("foo"));

		verify(handler, times(0)).markCreated(any());
		verify(handler, times(1)).markModified(sample);
	}

	@Test // DATACASS-4
	void hasExplicitOrder() {

		assertThat(callback).isInstanceOf(Ordered.class);
		assertThat(callback.getOrder()).isEqualTo(100);
	}

	@Test // DATACASS-4, DATACASS-784
	void propagatesChangedInstanceToEvent() {

		ImmutableSample sample = new ImmutableSample();

		ImmutableSample newSample = new ImmutableSample();
		ReactiveIsNewAwareAuditingHandler handler = mock(ReactiveIsNewAwareAuditingHandler.class);
		doReturn(Mono.just(newSample)).when(handler).markAudited(eq(sample));

		ReactiveAuditingEntityCallback listener = new ReactiveAuditingEntityCallback(() -> handler);
		Object result = listener.onBeforeConvert(sample, CqlIdentifier.fromCql("foo")).block();

		assertThat(result).isSameAs(newSample);
	}

	private static class Sample {

		@Id private String id;
		@CreatedDate Date created;
		@LastModifiedDate Date modified;
	}

	@Value
	@With
	@AllArgsConstructor
	@NoArgsConstructor(force = true)
	static class ImmutableSample {

		@Id String id;
		@CreatedDate Date created;
		@LastModifiedDate Date modified;
	}
}
