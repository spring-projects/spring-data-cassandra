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
package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import static org.junit.Assert.*;

import java.util.UUID;

import org.springframework.data.cassandra.core.CassandraTemplate;

/**
 * @author Matthew T. Adams
 */
public class ForceQuotedCompositePrimaryKeyRepositoryTests {

	ImplicitRepository implicitRepository;
	ExplicitRepository explicitRepository;
	CassandraTemplate cassandraTemplate;

	public void before() {
		cassandraTemplate.deleteAll(Implicit.class);
	}

	public String query(String columnName, String tableName, String keyZeroColumnName, String keyZero,
			String keyOneColumnName, String keyOne) {

		return cassandraTemplate.queryForObject(String.format("select %s from %s where %s = '%s' and %s = '%s'", columnName,
				tableName, keyZeroColumnName, keyZero, keyOneColumnName, keyOne), String.class);
	}

	public void testImplicit() {

		ImplicitKey key = new ImplicitKey(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Implicit entity = new Implicit(key);

		// insert
		Implicit s = implicitRepository.save(entity);
		assertSame(s, entity);

		// select
		Implicit f = implicitRepository.findOne(key);
		assertNotSame(f, entity);
		String stringValue = query("stringvalue", "\"Implicit\"", "\"keyZero\"", f.getPrimaryKey().getKeyZero(),
				"\"keyOne\"", f.getPrimaryKey().getKeyOne());
		assertEquals(f.getStringValue(), stringValue);

		// update
		f.setStringValue(f.getStringValue() + "X");
		Implicit u = implicitRepository.save(f);
		assertSame(u, f);
		f = implicitRepository.findOne(u.getPrimaryKey());
		assertNotSame(f, u);
		assertEquals(u.getStringValue(), f.getStringValue());

		// delete
		implicitRepository.delete(key);
		assertNull(implicitRepository.findOne(key));
	}

	public void testExplicit(String tableName, String stringValueColumnName, String keyZeroColumnName,
			String keyOneColumnName) {
		ExplicitKey key = new ExplicitKey(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Explicit entity = new Explicit(key);

		// insert
		Explicit s = explicitRepository.save(entity);
		assertSame(s, entity);

		// select
		Explicit f = explicitRepository.findOne(key);
		assertNotSame(f, entity);
		String stringValue = query(stringValueColumnName, tableName, keyZeroColumnName, f.getPrimaryKey().getKeyZero(),
				keyOneColumnName, f.getPrimaryKey().getKeyOne());
		assertEquals(f.getStringValue(), stringValue);

		// update
		f.setStringValue(f.getStringValue() + "X");
		Explicit u = explicitRepository.save(f);
		assertSame(u, f);
		f = explicitRepository.findOne(u.getPrimaryKey());
		assertNotSame(f, u);
		assertEquals(u.getStringValue(), f.getStringValue());

		// delete
		explicitRepository.delete(key);
		assertNull(explicitRepository.findOne(key));
	}
}
