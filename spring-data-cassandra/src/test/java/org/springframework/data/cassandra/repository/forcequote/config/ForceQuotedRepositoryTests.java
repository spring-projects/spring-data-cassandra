/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.forcequote.config;

import static org.assertj.core.api.Assertions.*;

import org.springframework.data.cassandra.core.CassandraOperations;

/**
 * Tests to be executed using Java/XML configuration.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
class ForceQuotedRepositoryTests {

	ImplicitRepository implicitRepository;
	ImplicitPropertiesRepository implicitPropertiesRepository;
	ExplicitRepository explicitRepository;
	ExplicitPropertiesRepository explicitPropertiesRepository;
	CassandraOperations cassandraTemplate;

	public void before() {
		cassandraTemplate.truncate(Implicit.class);
	}

	private String query(String columnName, String tableName, String keyColumnName, String key) {
		return cassandraTemplate.getCqlOperations().queryForObject(
				String.format("select %s from %s where %s = '%s'", columnName, tableName, keyColumnName, key), String.class);
	}

	public void testImplicit() {
		Implicit entity = new Implicit();
		String key = entity.getPrimaryKey();

		Implicit s = implicitRepository.save(entity);
		assertThat(entity).isSameAs(s);

		Implicit f = implicitRepository.findById(key).get();
		assertThat(entity).isNotSameAs(f);

		String stringValue = query("stringvalue", "\"Implicit\"", "primarykey", f.getPrimaryKey());
		assertThat(stringValue).isEqualTo(f.getStringValue());

		implicitRepository.deleteById(key);

		assertThat(implicitRepository.findById(key)).isNotPresent();
	}

	public void testExplicit(String tableName) {
		Explicit entity = new Explicit();
		String key = entity.getPrimaryKey();

		Explicit s = explicitRepository.save(entity);
		assertThat(entity).isSameAs(s);

		Explicit f = explicitRepository.findById(key).get();
		assertThat(entity).isNotSameAs(f);

		String stringValue = query("stringvalue", String.format("\"%s\"", tableName), "primarykey", f.getPrimaryKey());
		assertThat(stringValue).isEqualTo(f.getStringValue());

		explicitRepository.deleteById(key);

		assertThat(explicitRepository.findById(key)).isNotPresent();
	}

	public void testImplicitProperties() {
		ImplicitProperties entity = new ImplicitProperties();
		String key = entity.getPrimaryKey();

		ImplicitProperties s = implicitPropertiesRepository.save(entity);
		assertThat(entity).isSameAs(s);

		ImplicitProperties f = implicitPropertiesRepository.findById(key).get();
		assertThat(entity).isNotSameAs(f);

		String stringValue = query("\"stringValue\"", "implicitproperties", "\"primaryKey\"", f.getPrimaryKey());
		assertThat(stringValue).isEqualTo(f.getStringValue());

		implicitPropertiesRepository.deleteById(key);

		assertThat(implicitPropertiesRepository.findById(key)).isNotPresent();
	}

	public void testExplicitProperties(String stringValueColumnName, String primaryKeyColumnName) {
		ExplicitProperties entity = new ExplicitProperties();
		String key = entity.getPrimaryKey();

		ExplicitProperties s = explicitPropertiesRepository.save(entity);
		assertThat(entity).isSameAs(s);

		ExplicitProperties f = explicitPropertiesRepository.findById(key).get();
		assertThat(entity).isNotSameAs(f);

		String stringValue = query(String.format("\"%s\"", stringValueColumnName), "explicitproperties",
				String.format("\"%s\"", primaryKeyColumnName), f.getPrimaryKey());
		assertThat(stringValue).isEqualTo(f.getStringValue());

		implicitPropertiesRepository.deleteById(key);

		assertThat(implicitPropertiesRepository.findById(key)).isNotPresent();
	}
}
