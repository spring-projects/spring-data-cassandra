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

package org.springframework.data.cassandra.config;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.session.Session;

/**
 * Test suite of unit tests testing the contract and functionality of the {@link SchemaRefreshUtils} class.
 */
@ExtendWith(MockitoExtension.class)
class SchemaRefreshUtilsUnitTests {
	@Mock Session session;

	@Test
	void withDisabledSchemaRevert() throws Exception {
		when(session.isSchemaMetadataEnabled()).thenReturn(true);
		SchemaRefreshUtils.withDisabledSchema(session, () -> {});
		verify(session).setSchemaMetadataEnabled(false);
		verify(session).setSchemaMetadataEnabled(null);
	}

	@Test
	void withDisabledSchemaDisabledPreviously() throws Exception {
		when(session.isSchemaMetadataEnabled()).thenReturn(false);
		SchemaRefreshUtils.withDisabledSchema(session, () -> {});
		verify(session).setSchemaMetadataEnabled(false);
		verify(session).setSchemaMetadataEnabled(null);
	}

	@Test
	void withDisabledSchemaDisabledProgrammaticallyPreviously() throws Exception {
		when(session.isSchemaMetadataEnabled()).thenReturn(false).thenReturn(true);
		SchemaRefreshUtils.withDisabledSchema(session, () -> {});
		verify(session, times(2)).setSchemaMetadataEnabled(false);
		verify(session).setSchemaMetadataEnabled(null);
	}

	@Test
	void withDisabledSchemaEnabledProgrammaticallyPreviously() throws Exception {
		when(session.isSchemaMetadataEnabled()).thenReturn(true).thenReturn(false);
		SchemaRefreshUtils.withDisabledSchema(session, () -> {});
		verify(session).setSchemaMetadataEnabled(true);
		verify(session).setSchemaMetadataEnabled(null);
	}
}
