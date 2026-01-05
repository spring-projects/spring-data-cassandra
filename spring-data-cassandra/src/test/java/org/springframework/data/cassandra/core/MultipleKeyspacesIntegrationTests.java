/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.query.Criteria.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for {@link CassandraTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Tomasz Lelek
 */
class MultipleKeyspacesIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraTemplate template;

	@BeforeEach
	void setUp() {

		session.execute("DROP KEYSPACE IF EXISTS multiplekeyspaces_it;");
		session.execute(
				"CREATE KEYSPACE multiplekeyspaces_it WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.setUserTypeResolver(new SimpleUserTypeResolver(session));
		converter.afterPropertiesSet();

		template = new CassandraTemplate(new CqlTemplate(session), converter);

		SchemaTestUtils.createTableAndTypes(DefaultKs.class, template);
		SchemaTestUtils.createTableAndTypes(OtherKs.class, template);
		SchemaTestUtils.truncate(DefaultKs.class, template);
		SchemaTestUtils.truncate(OtherKs.class, template);
	}

	@Test // GH-921
	void shouldInsertAndSelect() {

		DefaultKs defaultKs = new DefaultKs("foo1", "bar", new DefaultKsUdt("baz"));
		OtherKs otherKs = new OtherKs("foo2", "bar", new OtherKsUdt("baz"));

		template.insert(defaultKs);
		template.insert(otherKs);

		assertThat(template.select(Query.query(where("id").is("foo1")), DefaultKs.class)).hasSize(1);
		assertThat(template.select(Query.query(where("id").is("foo2")), DefaultKs.class)).isEmpty();

		assertThat(template.select(Query.query(where("id").is("foo1")), OtherKs.class)).isEmpty();
		assertThat(template.select(Query.query(where("id").is("foo2")), OtherKs.class)).hasSize(1);
	}

	@Table
	static class DefaultKs {

		@Id String id;
		String value;
		DefaultKsUdt my_udt;

		public DefaultKs(String id, String value, DefaultKsUdt my_udt) {
			this.id = id;
			this.value = value;
			this.my_udt = my_udt;
		}
	}

	@UserDefinedType
	static class DefaultKsUdt {

		String value;

		public DefaultKsUdt(String value) {
			this.value = value;
		}
	}

	@Table(keyspace = "multiplekeyspaces_it")
	static class OtherKs {

		@Id String id;
		String value;
		OtherKsUdt my_udt;

		public OtherKs(String id, String value, OtherKsUdt my_udt) {
			this.id = id;
			this.value = value;
			this.my_udt = my_udt;
		}
	}

	@UserDefinedType(keyspace = "multiplekeyspaces_it")
	static class OtherKsUdt {

		String value;

		public OtherKsUdt(String value) {
			this.value = value;
		}
	}

}
