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
import static org.springframework.data.cassandra.core.cql.generator.AlterUserTypeCqlGenerator.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.keyspace.AlterUserTypeSpecification;

import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link AlterUserTypeCqlGenerator}.
 *
 * @author Mark Paluch
 */
class AlterUserTypeCqlGeneratorUnitTests {

	@Test // DATACASS-172
	void alterTypeShouldAddField() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address") //
				.add("zip", DataTypes.TEXT);

		assertThat(toCql(spec)).isEqualTo("ALTER TYPE address ADD zip text;");
	}

	@Test // DATACASS-172
	void alterTypeShouldAlterField() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address") //
				.alter("zip", DataTypes.TEXT);

		assertThat(toCql(spec)).isEqualTo("ALTER TYPE address ALTER zip TYPE text;");
	}

	@Test // DATACASS-172
	void alterTypeShouldRenameField() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address") //
				.rename("zip", "zap");

		assertThat(toCql(spec)).isEqualTo("ALTER TYPE address RENAME zip TO zap;");
	}

	@Test // DATACASS-172
	void alterTypeShouldRenameFields() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address") //
				.rename("zip", "zap") //
				.rename("city", "county");

		assertThat(toCql(spec)).isEqualTo("ALTER TYPE address RENAME zip TO zap AND city TO county;");
	}


	@Test // DATACASS-172
	void generationFailsWithoutFields() {
		assertThatIllegalArgumentException().isThrownBy(() -> toCql(AlterUserTypeSpecification.alterType("hello")));
	}
}
