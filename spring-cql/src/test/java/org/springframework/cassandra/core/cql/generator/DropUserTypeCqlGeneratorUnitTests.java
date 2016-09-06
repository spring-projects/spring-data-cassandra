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
package org.springframework.cassandra.core.cql.generator;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.cassandra.core.cql.generator.DropUserTypeCqlGenerator.*;

import org.junit.Test;
import org.springframework.cassandra.core.keyspace.DropUserTypeSpecification;

/**
 * Unit tests for {@link DropUserTypeCqlGenerator}.
 * 
 * @author Mark Paluch
 */
public class DropUserTypeCqlGeneratorUnitTests {

	@Test
	public void shouldDropUserType() throws Exception {

		DropUserTypeSpecification spec = DropUserTypeSpecification.dropType("address");

		assertThat(toCql(spec), is(equalTo("DROP TYPE address;")));
	}

	@Test
	public void shouldDropUserTypeIfExists() throws Exception {

		DropUserTypeSpecification spec = DropUserTypeSpecification.dropType("address").ifExists();

		assertThat(toCql(spec), is(equalTo("DROP TYPE IF EXISTS address;")));
	}
}
