/*
 * Copyright 2013-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.generator;

import static org.springframework.data.cassandra.core.cql.CqlStringUtils.*;

import java.util.Map;

import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOptionsSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.Option;

/**
 * Base class that contains behavior common to CQL generation for table operations.
 *
 * @author Matthew T. Adams
 * @param T The subtype of this class for which this is a CQL generator.
 */
public abstract class KeyspaceOptionsCqlGenerator<T extends KeyspaceOptionsSpecification<T>>
		extends KeyspaceNameCqlGenerator<KeyspaceOptionsSpecification<T>> {

	public KeyspaceOptionsCqlGenerator(KeyspaceOptionsSpecification<T> specification) {
		super(specification);
	}

	@SuppressWarnings("unchecked")
	protected T spec() {
		return (T) getSpecification();
	}

	void optionValueMap(Map<Option, Object> valueMap, StringBuilder cql) {

		if (valueMap.isEmpty()) {
			return;
		}
		// else option value is a non-empty map

		// append { 'name' : 'value', ... }
		cql.append("{ ");
		boolean mapFirst = true;
		for (Map.Entry<Option, Object> entry : valueMap.entrySet()) {
			if (mapFirst) {
				mapFirst = false;
			} else {
				cql.append(", ");
			}

			Option option = entry.getKey();
			cql.append(singleQuote(option.getName())); // entries in map keys are always quoted
			cql.append(" : ");
			Object entryValue = entry.getValue();
			entryValue = entryValue == null ? "" : entryValue.toString();
			if (option.escapesValue()) {
				entryValue = escapeSingle(entryValue);
			}
			if (option.quotesValue()) {
				entryValue = singleQuote(entryValue);
			}
			cql.append(entryValue);
		}
		cql.append(" }");

		return;
	}
}
