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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;
import static org.springframework.data.cassandra.core.cql.generator.AlterUserTypeCqlGenerator.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.keyspace.AlterUserTypeSpecification;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.util.Version;

import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Integration tests for {@link AlterUserTypeCqlGenerator}.
 *
 * @author Mark Paluch
 */
class AlterUserTypeCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private static final Version CASSANDRA_3_10 = Version.parse("3.10");
	private static final Version CASSANDRA_3_0_10 = Version.parse("3.0.10");

	private Version cassandraVersion;

	@BeforeEach
	void setUp() throws Exception {

		cassandraVersion = CassandraVersion.get(session);

		session.execute("DROP TYPE IF EXISTS address;");
		session.execute("CREATE TYPE address (zip text, state text);");
	}

	@Test // DATACASS-172
	void alterTypeShouldAddField() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address")//
				.add("street", DataTypes.TEXT);

		session.execute(toCql(spec));
	}

	@Test // DATACASS-172, DATACASS-429
	void alterTypeShouldAlterField() {

		assumeTrue(cassandraVersion.isLessThan(CASSANDRA_3_10) && cassandraVersion.isLessThan(CASSANDRA_3_0_10));

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address")//
				.alter("zip", DataTypes.TEXT);

		session.execute(toCql(spec));
	}

	@Test // DATACASS-172
	void alterTypeShouldRenameField() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address")//
				.rename("zip", "zap");

		session.execute(toCql(spec));
	}

	@Test // DATACASS-172
	void alterTypeShouldRenameFields() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address")//
				.rename("zip", "zap") //
				.rename("state", "county");

		session.execute(toCql(spec));
	}

	@Test // DATACASS-172
	void generationFailsIfNameIsNotSet() {
		assertThatNullPointerException().isThrownBy(() -> toCql(AlterUserTypeSpecification.alterType(null)));
	}

	@Test // DATACASS-172
	void generationFailsWithoutFields() {
		assertThatIllegalArgumentException().isThrownBy(() -> toCql(AlterUserTypeSpecification.alterType("hello")));
	}
}
