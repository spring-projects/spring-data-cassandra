/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.example;

import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.NamingStrategy;

class NamingStrategyConfiguration {

	public void configurationMethod() {

		// tag::method[]
		CassandraMappingContext context = new CassandraMappingContext();

		// default naming strategy
		context.setNamingStrategy(NamingStrategy.INSTANCE);

		// snake_case converted to upper case (SNAKE_CASE)
		context.setNamingStrategy(NamingStrategy.SNAKE_CASE.transform(String::toUpperCase));

		// end::method[]
	}
}
