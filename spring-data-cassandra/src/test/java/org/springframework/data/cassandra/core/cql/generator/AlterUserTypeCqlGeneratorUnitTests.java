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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.cql.keyspace.AlterUserTypeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.SpecificationBuilder;

import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link AlterUserTypeCqlGenerator}.
 *
 * @author Mark Paluch
 */
class AlterUserTypeCqlGeneratorUnitTests {

	@Test // DATACASS-172
	void alterTypeShouldAddField() {

		AlterUserTypeSpecification spec = SpecificationBuilder.alterType("address") //
				.add("zip", DataTypes.TEXT);

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("ALTER TYPE address ADD zip text;");
	}

	@Test // DATACASS-172
	void alterTypeShouldAlterField() {

		AlterUserTypeSpecification spec = SpecificationBuilder.alterType("address") //
				.alter("zip", DataTypes.TEXT);

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("ALTER TYPE address ALTER zip TYPE text;");
	}

	@Test // DATACASS-172
	void alterTypeShouldRenameField() {

		AlterUserTypeSpecification spec = SpecificationBuilder.alterType("address") //
				.rename("zip", "zap");

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("ALTER TYPE address RENAME zip TO zap;");
	}

	@Test // DATACASS-172
	void alterTypeShouldRenameFields() {

		AlterUserTypeSpecification spec = SpecificationBuilder.alterType("address") //
				.rename("zip", "zap") //
				.rename("city", "county");

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("ALTER TYPE address RENAME zip TO zap AND city TO county;");
	}


	@Test // DATACASS-172
	void generationFailsWithoutFields() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> AlterUserTypeCqlGenerator.toCql(AlterUserTypeSpecification.alterType("hello")));
	}
}
