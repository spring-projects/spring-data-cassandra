/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;

/**
 * Unit tests for {@link SimpleUserTypeResolver}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleUserTypeResolverUnitTests {

	@Mock Cluster cluster;
	@Mock Metadata metadata;
	@Mock KeyspaceMetadata keyspaceMetadata;

	@Before
	public void before() {
		when(cluster.getMetadata()).thenReturn(metadata);
		when(metadata.getKeyspace(anyString())).thenReturn(keyspaceMetadata);
	}

	@Test // DATACASS-720
	public void shouldQuoteCaseSensitiveKeyspaceName() {

		SimpleUserTypeResolver resolver = new SimpleUserTypeResolver(cluster, "MyKeyspace");

		resolver.resolveType(CqlIdentifier.of("user_type"));

		verify(metadata).getKeyspace("\"MyKeyspace\"");
	}
}
