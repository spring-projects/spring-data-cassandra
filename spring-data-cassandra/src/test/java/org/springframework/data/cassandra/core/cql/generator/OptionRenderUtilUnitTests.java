/*
 * Copyright 2026-present the original author or authors.
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

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.cql.keyspace.DefaultOption;
import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;

/**
 * Unit tests for {@link OptionRenderUtil}.
 */
class OptionRenderUtilUnitTests {

	@Test // GH-58
	void shouldEscapeMapOptionName() {

		Option option = new DefaultOption("key' : 'value", String.class, true, false, true);

		assertThat(OptionRenderUtil.render(Map.of(option, "value"))).isEqualTo("{ 'key'' : ''value' : 'value' }");
	}

	@Test // GH-58
	void shouldValidateValues() {
		assertThatIllegalArgumentException().isThrownBy(() -> OptionRenderUtil.render(Map.of(TableOption.CDC, "hello")));
		assertThatIllegalArgumentException()
				.isThrownBy(() -> OptionRenderUtil.render(Map.of(TableOption.CACHING, "not a map")));
	}

}
