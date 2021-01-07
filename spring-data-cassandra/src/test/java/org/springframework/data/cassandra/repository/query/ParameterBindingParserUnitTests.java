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
package org.springframework.data.cassandra.repository.query;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.repository.query.BindingContext.ParameterBinding;
import org.springframework.data.cassandra.repository.query.StringBasedQuery.ParameterBindingParser;

/**
 * Unit tests for {@link ParameterBindingParser}.
 *
 * @author Mark Paluch
 */
class ParameterBindingParserUnitTests {

	@Test // DATACASS-117
	void parseWithoutParameters() {

		String query = "SELECT * FROM hello_world";
		List<ParameterBinding> bindings = new ArrayList<>();

		String transformed = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				bindings);

		assertThat(transformed).isEqualTo(query);
		assertThat(bindings).isEmpty();
	}

	@Test // DATACASS-117
	void parseWithStaticParameters() {

		String query = "SELECT * FROM hello_world WHERE a = 1 AND b = {'list'} AND c = {'key':'value'}";
		List<ParameterBinding> bindings = new ArrayList<>();

		String transformed = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				bindings);

		assertThat(transformed).isEqualTo(query);
		assertThat(bindings).isEmpty();
	}

	@Test // DATACASS-117
	void parseWithPositionalParameters() {

		String query = "SELECT * FROM hello_world WHERE a = ?0 and b = ?13";
		List<ParameterBinding> bindings = new ArrayList<>();

		String transformed = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				bindings);

		assertThat(transformed).isEqualTo("SELECT * FROM hello_world WHERE a = ?_param_? and b = ?_param_?");
		assertThat(bindings).hasSize(2);

		assertThat(bindings.get(0).getParameterIndex()).isEqualTo(0);
		assertThat(bindings.get(1).getParameterIndex()).isEqualTo(13);
	}

	@Test // DATACASS-117
	void parseWithNamedParameters() {

		String query = "SELECT * FROM hello_world WHERE a = :hello and b = :world";
		List<ParameterBinding> bindings = new ArrayList<>();

		String transformed = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				bindings);

		assertThat(transformed).isEqualTo("SELECT * FROM hello_world WHERE a = ?_param_? and b = ?_param_?");
		assertThat(bindings).hasSize(2);
	}

	@Test // DATACASS-117
	void parseWithIndexExpressionParameters() {

		String query = "SELECT * FROM hello_world WHERE a = ?#{[0]} and b = ?#{[2]}";
		List<ParameterBinding> bindings = new ArrayList<>();

		String transformed = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				bindings);

		assertThat(transformed).isEqualTo("SELECT * FROM hello_world WHERE a = ?_param_? and b = ?_param_?");
		assertThat(bindings).hasSize(2);
	}

	@Test // DATACASS-117
	void parseWithNameExpressionParameters() {

		String query = "SELECT * FROM hello_world WHERE a = :#{#a} and b = :#{#b}";
		List<ParameterBinding> bindings = new ArrayList<>();

		String transformed = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				bindings);

		assertThat(transformed).isEqualTo("SELECT * FROM hello_world WHERE a = ?_param_? and b = ?_param_?");
		assertThat(bindings).hasSize(2);
	}

	@Test // DATACASS-117
	void parseWithMixedParameters() {

		String query = "SELECT * FROM hello_world WHERE (a = ?1 and b = :name) and c = (:#{#a}) and (d = ?#{[1]})";
		List<ParameterBinding> bindings = new ArrayList<>();

		String transformed = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				bindings);

		assertThat(transformed).isEqualTo(
				"SELECT * FROM hello_world WHERE (a = ?_param_? and b = ?_param_?) and c = (?_param_?) and (d = ?_param_?)");
		assertThat(bindings).hasSize(4);
	}
}
