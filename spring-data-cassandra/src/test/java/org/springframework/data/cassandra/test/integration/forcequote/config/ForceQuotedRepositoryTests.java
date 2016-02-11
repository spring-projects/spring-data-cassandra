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
package org.springframework.data.cassandra.test.integration.forcequote.config;

import static org.junit.Assert.*;

import org.springframework.data.cassandra.core.CassandraOperations;

/**
 * Tests to be executed using Java/XML configuration.
 *
 * @author Matthew T. Adams
 */
public class ForceQuotedRepositoryTests {

	ImplicitRepository implicitRepository;
	ImplicitPropertiesRepository implicitPropertiesRepository;
	ExplicitRepository explicitRepository;
	ExplicitPropertiesRepository explicitPropertiesRepository;
	CassandraOperations cassandraTemplate;

	public void before() {
		cassandraTemplate.deleteAll(Implicit.class);
	}

	public String query(String columnName, String tableName, String keyColumnName, String key) {
		return cassandraTemplate.queryForObject(
				String.format("select %s from %s where %s = '%s'", columnName, tableName, keyColumnName, key), String.class);
	}

	public void testImplicit() {
		Implicit entity = new Implicit();
		String key = entity.getPrimaryKey();

		Implicit s = implicitRepository.save(entity);
		assertSame(s, entity);

		Implicit f = implicitRepository.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query("stringvalue", "\"Implicit\"", "primarykey", f.getPrimaryKey());
		assertEquals(f.getStringValue(), stringValue);

		implicitRepository.delete(key);

		assertNull(implicitRepository.findOne(key));
	}

	public void testExplicit(String tableName) {
		Explicit entity = new Explicit();
		String key = entity.getPrimaryKey();

		Explicit s = explicitRepository.save(entity);
		assertSame(s, entity);

		Explicit f = explicitRepository.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query("stringvalue", String.format("\"%s\"", tableName), "primarykey", f.getPrimaryKey());
		assertEquals(f.getStringValue(), stringValue);

		explicitRepository.delete(key);

		assertNull(explicitRepository.findOne(key));
	}

	public void testImplicitProperties() {
		ImplicitProperties entity = new ImplicitProperties();
		String key = entity.getPrimaryKey();

		ImplicitProperties s = implicitPropertiesRepository.save(entity);
		assertSame(s, entity);

		ImplicitProperties f = implicitPropertiesRepository.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query("\"stringValue\"", "implicitproperties", "\"primaryKey\"", f.getPrimaryKey());
		assertEquals(f.getStringValue(), stringValue);

		implicitPropertiesRepository.delete(key);

		assertNull(implicitPropertiesRepository.findOne(key));
	}

	public void testExplicitProperties(String stringValueColumnName, String primaryKeyColumnName) {
		ExplicitProperties entity = new ExplicitProperties();
		String key = entity.getPrimaryKey();

		ExplicitProperties s = explicitPropertiesRepository.save(entity);
		assertSame(s, entity);

		ExplicitProperties f = explicitPropertiesRepository.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query(String.format("\"%s\"", stringValueColumnName), "explicitproperties",
				String.format("\"%s\"", primaryKeyColumnName), f.getPrimaryKey());
		assertEquals(f.getStringValue(), stringValue);

		implicitPropertiesRepository.delete(key);

		assertNull(implicitPropertiesRepository.findOne(key));
	}
}
