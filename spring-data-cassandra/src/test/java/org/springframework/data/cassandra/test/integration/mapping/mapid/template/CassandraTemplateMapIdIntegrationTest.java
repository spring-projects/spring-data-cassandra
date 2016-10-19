/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.data.cassandra.test.integration.mapping.mapid.template;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.repository.support.BasicMapId.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;

/**
 * Integration tests for {@link org.springframework.data.cassandra.core.CassandraTemplate} with {@link MapId}.
 *
 * @author Matthew T. Adams
 */
public class CassandraTemplateMapIdIntegrationTest extends AbstractKeyspaceCreatingIntegrationTest {

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
		SinglePkc saved = operations.insert(inserted);
		assertThat(inserted).isSameAs(saved);

		// select
		MapId id = id("key", saved.getKey());
		SinglePkc selected = operations.selectOneById(id, SinglePkc.class);
		assertThat(saved).isNotSameAs(selected);
		assertThat(selected.getKey()).isEqualTo(saved.getKey());
		assertThat(selected.getValue()).isEqualTo(saved.getValue());

		// update
		selected.setValue(uuid());
		SinglePkc updated = operations.update(selected);
		assertThat(selected).isSameAs(updated);

		selected = operations.selectOneById(id, SinglePkc.class);
		assertThat(updated).isNotSameAs(selected);
		assertThat(selected.getValue()).isEqualTo(updated.getValue());

		// delete
		operations.delete(selected);
		assertThat(operations.selectOneById(id, SinglePkc.class)).isNull();
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
		MultiPkc saved = operations.insert(inserted);
		assertThat(inserted).isSameAs(saved);

		// select
		MapId id = id("key0", saved.getKey0()).with("key1", saved.getKey1());
		MultiPkc selected = operations.selectOneById(id, MultiPkc.class);
		assertThat(saved).isNotSameAs(selected);
		assertThat(selected.getKey0()).isEqualTo(saved.getKey0());
		assertThat(selected.getKey1()).isEqualTo(saved.getKey1());
		assertThat(selected.getValue()).isEqualTo(saved.getValue());

		// update
		selected.setValue(uuid());
		MultiPkc updated = operations.update(selected);
		assertThat(selected).isSameAs(updated);

		selected = operations.selectOneById(id, MultiPkc.class);
		assertThat(updated).isNotSameAs(selected);
		assertThat(selected.getValue()).isEqualTo(updated.getValue());

		// delete
		operations.delete(selected);
		assertThat(operations.selectOneById(id, MultiPkc.class)).isNull();
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
