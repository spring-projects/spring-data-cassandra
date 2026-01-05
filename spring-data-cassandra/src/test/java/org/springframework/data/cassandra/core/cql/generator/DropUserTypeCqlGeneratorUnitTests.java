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

import org.springframework.data.cassandra.core.cql.keyspace.DropUserTypeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.SpecificationBuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link DropUserTypeCqlGenerator}.
 *
 * @author Mark Paluch
 */
class DropUserTypeCqlGeneratorUnitTests {

	@Test // DATACASS-172
	void shouldDropUserType() {

		DropUserTypeSpecification spec = SpecificationBuilder.dropType("address");

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("DROP TYPE address;");
	}

	@Test // GH-921
	void shouldConsiderKeyspace() {

		DropUserTypeSpecification spec = SpecificationBuilder.dropType(CqlIdentifier.fromCql("foo"),
				CqlIdentifier.fromCql("bar"));

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("DROP TYPE foo.bar;");
	}

	@Test // DATACASS-172
	void shouldDropUserTypeIfExists() {

		DropUserTypeSpecification spec = SpecificationBuilder.dropType("address").ifExists();

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("DROP TYPE IF EXISTS address;");
	}
}
