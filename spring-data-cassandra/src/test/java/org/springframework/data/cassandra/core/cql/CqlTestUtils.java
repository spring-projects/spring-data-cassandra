/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class CqlTestUtils {

	/**
	 * Transforms the {@link Map} of {@link CqlIdentifier} to a {@link Map} keyed by {@link Option} for easier option
	 * assertion.
	 *
	 * @param options
	 * @return
	 */
	public static Map<Option, Object> toOptions(Map<CqlIdentifier, Object> options) {

		Map<Option, Object> result = new LinkedHashMap<>();

		record SimpleOption(String name) implements Option {

			@Override
			public Class<?> getType() {
				return Object.class;
			}

			@Override
			public String getName() {
				return name;
			}

			@Override
			public boolean takesValue() {
				return false;
			}

			@Override
			public boolean escapesValue() {
				return false;
			}

			@Override
			public boolean quotesValue() {
				return false;
			}

			@Override
			public boolean requiresValue() {
				return false;
			}

			@Override
			public void checkValue(Object value) {

			}

			@Override
			public boolean isCoerceable(Object value) {
				return false;
			}

			@Override
			public String toString(@Nullable Object value) {
				return "";
			}

			@Override
			public boolean equals(Object object) {

				if (!(object instanceof Option that)) {
					return false;
				}

				return ObjectUtils.nullSafeEquals(name, that.getName());
			}

		}
		options.forEach((k, v) -> {

			Option option = TableOption.findByName(k.asInternal());

			if (option == null) {
				option = new SimpleOption(k.asInternal());
			}

			result.put(option, v);

		});

		return result;
	}
}
