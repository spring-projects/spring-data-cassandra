/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;

/**
 * Unit tests for {@link SimpleTupleTypeFactory}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleTupleTypeFactoryUnitTests {

	@Mock Cluster cluster;
	@Mock Metadata metadata;

	@Test // DATACASS-523
	public void shouldCreateTupleTypes() {

		when(this.cluster.getMetadata()).thenReturn(this.metadata);

		new SimpleTupleTypeFactory(this.cluster).create(DataType.varchar());

		verify(this.metadata).newTupleType(Collections.singletonList(DataType.varchar()));
	}
}
