/*
 * Copyright 2022-present the original author or authors.
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
package org.springframework.data.cassandra.config;

import static org.mockito.Mockito.*;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.datastax.oss.driver.api.core.session.Session;

/**
 * Unit tests for {@link SchemaUtils}.
 *
 * @author Ammar Khaku
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SchemaUtilsUnitTests {

	@Mock Session session;

	@Test // GH-990, GH-1253
	void shouldSuspendSchemaRefresh() {

		when(session.isSchemaMetadataEnabled()).thenReturn(true);
		when(session.setSchemaMetadataEnabled(true)).thenReturn(CompletableFuture.completedFuture(null));

		SchemaUtils.withSuspendedAsyncSchemaRefresh(session, () -> {});

		verify(session).setSchemaMetadataEnabled(false);
		verify(session).setSchemaMetadataEnabled(null);
	}

	@Test // GH-990, GH-1253
	void shouldRetainSchemaRefreshWhenSchemaMetadataDisabled() {

		when(session.isSchemaMetadataEnabled()).thenReturn(false);

		SchemaUtils.withSuspendedAsyncSchemaRefresh(session, () -> {});

		verify(session, never()).setSchemaMetadataEnabled(anyBoolean());
	}

	@Test // GH-990, GH-1253
	void shouldRefreshSchemaWhenSchemaMetadataEnabled() {

		when(session.isSchemaMetadataEnabled()).thenReturn(true);
		when(session.refreshSchemaAsync()).thenReturn(CompletableFuture.completedFuture(null));

		SchemaUtils.withAsyncSchemaRefresh(session, () -> {});

		verify(session, never()).setSchemaMetadataEnabled(anyBoolean());
	}

	@Test // GH-990, GH-1253
	void shouldNotRefreshSchemaWhenSchemaMetadataDisabled() {

		when(session.isSchemaMetadataEnabled()).thenReturn(false);

		SchemaUtils.withAsyncSchemaRefresh(session, () -> {});

		verify(session, never()).setSchemaMetadataEnabled(anyBoolean());
		verify(session, never()).refreshSchemaAsync();
	}

}
