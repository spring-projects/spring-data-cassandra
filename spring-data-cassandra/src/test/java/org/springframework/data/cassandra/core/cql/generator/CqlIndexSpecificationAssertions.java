/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import org.springframework.data.cassandra.core.cql.keyspace.IndexDescriptor;

import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * @author David Webb
 * @author Matthew T. Adams
 * @author Antoine Toulme
 */
public class CqlIndexSpecificationAssertions {

	/**
	 * Assert the existence of an index using the index name.
	 *
	 * @param expected
	 * @param keyspace
	 * @param session
	 */
	public static void assertIndex(IndexDescriptor expected, String keyspace, Session session) {
		TableMetadata tableMetadata = session.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase())
				.getTable(expected.getTableName().toCql());

		IndexMetadata indexMetadata = tableMetadata.getIndex(expected.getName().toCql());

		assertThat(indexMetadata).isNotNull();
		assertThat(indexMetadata.getName()).isEqualTo(expected.getName().toCql());
	}

	/**
	 * Assert the absence of an index using the index name.
	 *
	 * @param expected
	 * @param keyspace
	 * @param session
	 */
	public static void assertNoIndex(IndexDescriptor expected, String keyspace, Session session) {
		TableMetadata tableMetadata = session.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase())
				.getTable(expected.getTableName().toCql());

		IndexMetadata indexMetadata = tableMetadata.getIndex(expected.getName().toCql());

		assertThat(indexMetadata).isNull();
	}
}
