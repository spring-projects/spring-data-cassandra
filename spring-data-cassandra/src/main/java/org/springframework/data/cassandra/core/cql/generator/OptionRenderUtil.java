/*
 * Copyright 2017-present the original author or authors.
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

import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.core.cql.keyspace.CqlStringUtils;
import org.springframework.data.cassandra.core.cql.keyspace.Option;

/**
 * Utility to render {@link Option}s.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class OptionRenderUtil {

	static String render(Map<?, @Nullable Object> valueMap) {

		if (valueMap.isEmpty()) {
			return "";
		}

		StringBuilder cql = new StringBuilder(valueMap.size() * 2 * 16);
		// else option value is a non-empty map

		// append { 'name' : 'value', ... }
		cql.append("{ ");
		boolean mapFirst = true;
		for (Map.Entry<?, @Nullable Object> entry : valueMap.entrySet()) {
			if (mapFirst) {
				mapFirst = false;
			} else {
				cql.append(", ");
			}

			Object option = entry.getKey();
			Object entryValue = entry.getValue();
			if (option instanceof Option knownOption) {

				cql.append(CqlStringUtils.toCqlLiteral(knownOption.getName())); // entries in map keys are always quoted
				cql.append(" : ");

				knownOption.checkValue(entryValue);

				cql.append(CqlStringUtils.toString(entryValue, knownOption.escapesValue(), knownOption.quotesValue()));
			}
			else {
				cql.append(CqlStringUtils.toCqlLiteral(option)); // entries in map keys are always quoted
				cql.append(" : ");
				cql.append(CqlStringUtils.toCqlLiteral(entryValue));
			}
		}
		cql.append(" }");

		return cql.toString();
	}

}
