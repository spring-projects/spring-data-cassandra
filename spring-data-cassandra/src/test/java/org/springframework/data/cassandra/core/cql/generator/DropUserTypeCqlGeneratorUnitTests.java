/*
 * Copyright 2016-2025 the original author or authors.
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
import static org.springframework.data.cassandra.core.cql.generator.DropUserTypeCqlGenerator.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.cql.keyspace.DropUserTypeSpecification;

/**
 * Unit tests for {@link DropUserTypeCqlGenerator}.
 *
 * @author Mark Paluch
 */
class DropUserTypeCqlGeneratorUnitTests {

	@Test // DATACASS-172
	void shouldDropUserType() {

		DropUserTypeSpecification spec = DropUserTypeSpecification.dropType("address");

		assertThat(toCql(spec)).isEqualTo("DROP TYPE address;");
	}

	@Test // DATACASS-172
	void shouldDropUserTypeIfExists() {

		DropUserTypeSpecification spec = DropUserTypeSpecification.dropType("address").ifExists();

		assertThat(toCql(spec)).isEqualTo("DROP TYPE IF EXISTS address;");
	}
}
