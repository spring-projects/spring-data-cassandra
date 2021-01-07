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
package org.springframework.data.cassandra.repository.forcequote.compositeprimarykey;

import static org.assertj.core.api.Assertions.*;

import java.util.UUID;

import org.springframework.data.cassandra.core.CassandraTemplate;

/**
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
class ForceQuotedCompositePrimaryKeyRepositoryTests {

	ImplicitRepository implicitRepository;
	ExplicitRepository explicitRepository;
	CassandraTemplate cassandraTemplate;

	public void before() {
		cassandraTemplate.truncate(Implicit.class);
	}

	private String query(String columnName, String tableName, String keyZeroColumnName, String keyZero,
			String keyOneColumnName, String keyOne) {

		return cassandraTemplate.getCqlOperations()
				.queryForObject(String.format("select %s from %s where %s = '%s' and %s = '%s'", columnName, tableName,
						keyZeroColumnName, keyZero, keyOneColumnName, keyOne), String.class);
	}

	public void testImplicit() {

		ImplicitKey key = new ImplicitKey(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Implicit entity = new Implicit(key);

		// insert
		Implicit s = implicitRepository.save(entity);
		assertThat(entity).isSameAs(s);

		// select
		Implicit f = implicitRepository.findById(key).get();
		assertThat(entity).isNotSameAs(f);
		String stringValue = query("stringvalue", "\"Implicit\"", "\"keyZero\"", f.getPrimaryKey().getKeyZero(),
				"\"keyOne\"", f.getPrimaryKey().getKeyOne());
		assertThat(stringValue).isEqualTo(f.getStringValue());

		// update
		f.setStringValue(f.getStringValue() + "X");
		Implicit u = implicitRepository.save(f);
		assertThat(f).isSameAs(u);
		f = implicitRepository.findById(u.getPrimaryKey()).get();
		assertThat(u).isNotSameAs(f);
		assertThat(f.getStringValue()).isEqualTo(u.getStringValue());

		// delete
		implicitRepository.deleteById(key);
		assertThat(implicitRepository.findById(key)).isNotPresent();
	}

	public void testExplicit(String tableName, String stringValueColumnName, String keyZeroColumnName,
			String keyOneColumnName) {
		ExplicitKey key = new ExplicitKey(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Explicit entity = new Explicit(key);

		// insert
		Explicit s = explicitRepository.save(entity);
		assertThat(entity).isSameAs(s);

		// select
		Explicit f = explicitRepository.findById(key).get();
		assertThat(entity).isNotSameAs(f);
		String stringValue = query(stringValueColumnName, tableName, keyZeroColumnName, f.getPrimaryKey().getKeyZero(),
				keyOneColumnName, f.getPrimaryKey().getKeyOne());
		assertThat(stringValue).isEqualTo(f.getStringValue());

		// update
		f.setStringValue(f.getStringValue() + "X");
		Explicit u = explicitRepository.save(f);
		assertThat(f).isSameAs(u);
		f = explicitRepository.findById(u.getPrimaryKey()).get();
		assertThat(u).isNotSameAs(f);
		assertThat(f.getStringValue()).isEqualTo(u.getStringValue());

		// delete
		explicitRepository.deleteById(key);
		assertThat(explicitRepository.findById(key)).isNotPresent();
	}
}
