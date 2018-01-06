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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.mapping.MapIdFactory.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Integration tests for {@link org.springframework.data.cassandra.core.CassandraTemplate} using {@link MapId}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraTemplateMapIdProxyDelegateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraOperations operations;

	@Before
	public void before() {

		operations = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(SinglePkc.class, operations);
		SchemaTestUtils.potentiallyCreateTableFor(MultiPkc.class, operations);

		SchemaTestUtils.truncate(SinglePkc.class, operations);
		SchemaTestUtils.truncate(MultiPkc.class, operations);
	}

	@Test
	public void testSinglePkc() {

		// insert
		SinglePkc inserted = new SinglePkc(uuid());
		inserted.setValue(uuid());
		operations.insert(inserted);

		// select
		SinglePkcId id = id(SinglePkcId.class).key(inserted.getKey());
		SinglePkc selected = operations.selectOneById(id, SinglePkc.class);
		assertThat(inserted).isNotSameAs(selected);
		assertThat(selected.getKey()).isEqualTo(inserted.getKey());
		assertThat(selected.getValue()).isEqualTo(inserted.getValue());

		// update
		selected.setValue(uuid());
		operations.update(selected);

		SinglePkc updated = operations.selectOneById(id, SinglePkc.class);
		assertThat(selected.getValue()).isEqualTo(updated.getValue());

		// delete
		operations.delete(selected);
		assertThat(operations.selectOneById(id, SinglePkc.class)).isNull();
	}

	public interface SinglePkcId {
		SinglePkcId key(String key);

		String key();
	}

	@Table
	public static class SinglePkc {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String key;

		@Column String value;

		public SinglePkc(String key) {
			setKey(key);
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	@Test
	public void testMultiPkc() {

		// insert
		MultiPkc inserted = new MultiPkc(uuid(), uuid());
		inserted.setValue(uuid());
		operations.insert(inserted);

		// select
		MultiPkcId id = id(MultiPkcId.class).key0(inserted.getKey0()).key1(inserted.getKey1());
		MultiPkc selected = operations.selectOneById(id, MultiPkc.class);
		assertThat(selected.getKey0()).isEqualTo(inserted.getKey0());
		assertThat(selected.getKey1()).isEqualTo(inserted.getKey1());
		assertThat(selected.getValue()).isEqualTo(inserted.getValue());

		// update
		selected.setValue(uuid());
		operations.update(selected);

		MultiPkc updated = operations.selectOneById(id, MultiPkc.class);
		assertThat(updated).isNotSameAs(selected);
		assertThat(selected.getValue()).isEqualTo(updated.getValue());

		// delete
		operations.delete(selected);
		assertThat(operations.selectOneById(id, MultiPkc.class)).isNull();
	}

	public interface MultiPkcId {
		MultiPkcId key0(String key0);

		String key0();

		MultiPkcId key1(String key1);

		String key1();
	}

	@Table
	public static class MultiPkc {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String key0;

		@PrimaryKeyColumn(ordinal = 1) String key1;

		@Column String value;

		public MultiPkc(String key0, String key1) {
			setKey0(key0);
			setKey1(key1);
		}

		public String getKey0() {
			return key0;
		}

		public void setKey0(String key0) {
			this.key0 = key0;
		}

		public String getKey1() {
			return key1;
		}

		public void setKey1(String key1) {
			this.key1 = key1;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}
}
