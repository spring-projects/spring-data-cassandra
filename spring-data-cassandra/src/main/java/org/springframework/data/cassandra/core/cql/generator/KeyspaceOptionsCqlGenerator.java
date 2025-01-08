/*
 * Copyright 2013-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.generator;

import java.util.Map;

import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOptionsSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.Option;

/**
 * Base class that contains behavior common to CQL generation for table operations.
 *
 * @author Matthew T. Adams
 * @param <T> the subtype of this class for which this is a CQL generator.
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
		cql.append(OptionRenderUtil.render(valueMap));
	}
}
