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
package org.springframework.data.cassandra.core.cql.keyspace;

import static org.assertj.core.api.Assertions.*;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CreateTableSpecification}.
 *
 * @author Seungho Kang
 */
class CreateTableSpecificationTest {

	@Test // GH-1584
	void shouldSupportStandardTableOptions() {

		CreateTableSpecification specification = CreateTableSpecification.createTable(CqlIdentifier.fromCql("person"))
				.with(TableOption.COMPACT_STORAGE)
				.with(TableOption.CDC, true)
				.with(TableOption.READ_REPAIR, "BLOCKING");

		assertThat(specification.getOptions()).containsEntry("COMPACT STORAGE", null)
				.containsEntry("cdc", true)
				.containsEntry("read_repair", "'BLOCKING'");
	}

	@Test // GH-1584
	void shouldUseRawValueWhenEscapingIsDisabled() {

		CreateTableSpecification specification = CreateTableSpecification.createTable(CqlIdentifier.fromCql("person"))
				.with("max_index_interval", 128, false, false);

		assertThat(specification.getOptions()).containsEntry("max_index_interval", 128);
	}

	@Test // GH-1584
	void shouldEscapeStringValueWhenEscapeIsEnabled() {

		CreateTableSpecification specification = CreateTableSpecification.createTable(CqlIdentifier.fromCql("person"))
				.with("read_repair", "BLOCKING", true, true);

		assertThat(specification.getOptions()).containsEntry("read_repair", "'BLOCKING'");
	}

	@Test // GH-1584
	void shouldPreserveEscapedStringWhenProvidedByCaller() {

		CreateTableSpecification specification = CreateTableSpecification.createTable(CqlIdentifier.fromCql("person"))
				.with("read_repair", "'BLOCKING'", false, false);

		assertThat(specification.getOptions()).containsEntry("read_repair", "'BLOCKING'");
	}

}
