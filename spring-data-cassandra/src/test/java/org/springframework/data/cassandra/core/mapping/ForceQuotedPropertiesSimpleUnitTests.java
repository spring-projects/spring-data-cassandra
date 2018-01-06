/*
 * Copyright 2016-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.cql.CqlIdentifier.*;

import java.io.Serializable;

import org.junit.Test;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;

/**
 * Unit tests for {@link CassandraMappingContext}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class ForceQuotedPropertiesSimpleUnitTests {

	public static final String EXPLICIT_PRIMARY_KEY_NAME = "ThePrimaryKey";
	public static final String EXPLICIT_COLUMN_NAME = "AnotherColumn";
	public static final String EXPLICIT_KEY_0 = "TheFirstKeyField";
	public static final String EXPLICIT_KEY_1 = "TheSecondKeyField";

	CassandraMappingContext context = new CassandraMappingContext();

	@Test
	public void testImplicit() {

		CassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(Implicit.class);

		CassandraPersistentProperty primaryKey = entity.getRequiredPersistentProperty("primaryKey");
		CassandraPersistentProperty aString = entity.getRequiredPersistentProperty("aString");

		assertThat(primaryKey.getColumnName().toCql()).isEqualTo("\"primaryKey\"");
		assertThat(aString.getColumnName().toCql()).isEqualTo("\"aString\"");
	}

	@Table
	public static class Implicit {

		@PrimaryKey(forceQuote = true) String primaryKey;

		@Column(forceQuote = true) String aString;
	}

	@Test
	public void testDefault() {

		CassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(Default.class);

		CassandraPersistentProperty primaryKey = entity.getRequiredPersistentProperty("primaryKey");
		CassandraPersistentProperty aString = entity.getRequiredPersistentProperty("aString");

		assertThat(primaryKey.getColumnName().toCql()).isEqualTo("primarykey");
		assertThat(aString.getColumnName().toCql()).isEqualTo("astring");
	}

	@Table
	public static class Default {

		@PrimaryKey String primaryKey;

		@Column String aString;
	}

	@Test
	public void testExplicit() {

		CassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(Explicit.class);

		CassandraPersistentProperty primaryKey = entity.getRequiredPersistentProperty("primaryKey");
		CassandraPersistentProperty aString = entity.getRequiredPersistentProperty("aString");

		assertThat(primaryKey.getColumnName().toCql()).isEqualTo("\"" + EXPLICIT_PRIMARY_KEY_NAME + "\"");
		assertThat(aString.getColumnName().toCql()).isEqualTo("\"" + EXPLICIT_COLUMN_NAME + "\"");
	}

	@Table
	public static class Explicit {

		@PrimaryKey(value = EXPLICIT_PRIMARY_KEY_NAME, forceQuote = true) String primaryKey;

		@Column(value = EXPLICIT_COLUMN_NAME, forceQuote = true) String aString;
	}

	@Test
	public void testImplicitComposite() {

		CassandraPersistentEntity<?> key = context.getRequiredPersistentEntity(ImplicitKey.class);

		CassandraPersistentProperty stringZero = key.getRequiredPersistentProperty("stringZero");
		CassandraPersistentProperty stringOne = key.getRequiredPersistentProperty("stringOne");

		assertThat(stringZero.getColumnName().toCql()).isEqualTo("\"stringZero\"");
		assertThat(stringZero.getColumnName()).isEqualTo(quoted("stringZero"));
		assertThat(stringOne.getColumnName().toCql()).isEqualTo("\"stringOne\"");
		assertThat(stringOne.getColumnName()).isEqualTo(quoted("stringOne"));
	}

	@PrimaryKeyClass
	public static class ImplicitKey implements Serializable {

		private static final long serialVersionUID = -1956747638065267667L;

		@PrimaryKeyColumn(ordinal = 0, forceQuote = true, type = PrimaryKeyType.PARTITIONED) String stringZero;

		@PrimaryKeyColumn(ordinal = 1, forceQuote = true) String stringOne;
	}

	@Table
	public static class ImplicitComposite {

		@PrimaryKey(forceQuote = true) ImplicitKey primaryKey;

		@Column(forceQuote = true) String aString;
	}

	@Test
	public void testDefaultComposite() {

		CassandraPersistentEntity<?> key = context.getRequiredPersistentEntity(DefaultKey.class);

		CassandraPersistentProperty stringZero = key.getRequiredPersistentProperty("stringZero");
		CassandraPersistentProperty stringOne = key.getRequiredPersistentProperty("stringOne");

		assertThat(stringZero.getColumnName()).isEqualTo(CqlIdentifier.of("stringZero"));
		assertThat(stringOne.getColumnName()).isEqualTo(CqlIdentifier.of("stringOne"));
		assertThat(stringZero.getColumnName().toCql()).isEqualTo("stringzero");
		assertThat(stringOne.getColumnName().toCql()).isEqualTo("stringone");
	}

	@PrimaryKeyClass
	public static class DefaultKey implements Serializable {

		private static final long serialVersionUID = -1956747638065267667L;

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String stringZero;

		@PrimaryKeyColumn(ordinal = 1) String stringOne;
	}

	@Table
	public static class DefaultComposite {

		@PrimaryKey DefaultKey primaryKey;

		@Column String aString;
	}

	@Test
	public void testExplicitComposite() {

		CassandraPersistentEntity<?> key = context.getRequiredPersistentEntity(ExplicitKey.class);

		CassandraPersistentProperty stringZero = key.getRequiredPersistentProperty("stringZero");
		CassandraPersistentProperty stringOne = key.getRequiredPersistentProperty("stringOne");

		assertThat(stringZero.getColumnName()) //
				.isEqualTo(CqlIdentifier.of("TheFirstKeyField", true)) //
				.isNotEqualTo(CqlIdentifier.of("TheFirstKeyField"));

		assertThat(stringZero.getColumnName().toCql()).isEqualTo("\"" + EXPLICIT_KEY_0 + "\"");
		assertThat(stringOne.getColumnName().toCql()).isEqualTo("\"" + EXPLICIT_KEY_1 + "\"");
	}

	@PrimaryKeyClass
	public static class ExplicitKey implements Serializable {

		private static final long serialVersionUID = -1956747638065267667L;

		@PrimaryKeyColumn(ordinal = 0, name = EXPLICIT_KEY_0, forceQuote = true,
				type = PrimaryKeyType.PARTITIONED) String stringZero;

		@PrimaryKeyColumn(ordinal = 1, name = EXPLICIT_KEY_1, forceQuote = true) String stringOne;
	}

	@Table
	public static class ExplicitComposite {

		@PrimaryKey(forceQuote = true) ExplicitKey primaryKey;

		@Column(forceQuote = true) String aString;
	}
}
