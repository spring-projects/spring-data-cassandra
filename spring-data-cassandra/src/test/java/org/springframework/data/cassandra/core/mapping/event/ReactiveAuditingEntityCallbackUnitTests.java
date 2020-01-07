/*
 * Copyright 2019-2020 the original author or authors.
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
import lombok.experimental.Wither;

import java.util.Collections;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalAnswers;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.core.Ordered;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.mapping.context.PersistentEntities;

/**
 * Unit tests for {@link ReactiveAuditingEntityCallback}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveAuditingEntityCallbackUnitTests {

	IsNewAwareAuditingHandler handler;
	ReactiveAuditingEntityCallback callback;

	@Before
	public void setUp() {

		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.getPersistentEntity(Sample.class);

		handler = spy(new IsNewAwareAuditingHandler(new PersistentEntities(Collections.singletonList(mappingContext))));

		doAnswer(AdditionalAnswers.returnsArgAt(0)).when(handler).markCreated(any());
		doAnswer(AdditionalAnswers.returnsArgAt(0)).when(handler).markModified(any());

		callback = new ReactiveAuditingEntityCallback(() -> handler);
	}

	@Test // DATACASS-4
	public void rejectsNullAuditingHandler() {
		assertThatIllegalArgumentException().isThrownBy(() -> new AuditingEntityCallback(null));
	}

	@Test // DATACASS-4
	public void triggersCreationMarkForObjectWithEmptyId() {

		Sample sample = new Sample();
		callback.onBeforeConvert(sample, CqlIdentifier.of("foo"));

		verify(handler, times(1)).markCreated(sample);
		verify(handler, times(0)).markModified(any());
	}

	@Test // DATACASS-4
	public void triggersModificationMarkForObjectWithSetId() {

		Sample sample = new Sample();
		sample.id = "id";
		callback.onBeforeConvert(sample, CqlIdentifier.of("foo"));

		verify(handler, times(0)).markCreated(any());
		verify(handler, times(1)).markModified(sample);
	}

	@Test // DATACASS-4
	public void hasExplicitOrder() {

		assertThat(callback).isInstanceOf(Ordered.class);
		assertThat(callback.getOrder()).isEqualTo(100);
	}

	@Test // DATACASS-4
	public void propagatesChangedInstanceToEvent() {

		ImmutableSample sample = new ImmutableSample();

		ImmutableSample newSample = new ImmutableSample();
		IsNewAwareAuditingHandler handler = mock(IsNewAwareAuditingHandler.class);
		doReturn(newSample).when(handler).markAudited(eq(sample));

		ReactiveAuditingEntityCallback listener = new ReactiveAuditingEntityCallback(() -> handler);
		Object result = listener.onBeforeConvert(sample, CqlIdentifier.of("foo")).block();

		assertThat(result).isSameAs(newSample);
	}

	static class Sample {

		@Id String id;
		@CreatedDate Date created;
		@LastModifiedDate Date modified;
	}

	@Value
	@Wither
	@AllArgsConstructor
	@NoArgsConstructor(force = true)
	static class ImmutableSample {

		@Id String id;
		@CreatedDate Date created;
		@LastModifiedDate Date modified;
	}
}
