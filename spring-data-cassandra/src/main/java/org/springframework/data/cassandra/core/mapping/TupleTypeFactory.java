/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.util.Arrays;
import java.util.List;

import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;

/**
 * Factory to create {@link TupleType} given {@link DataType tuple element types}. Primarily internal use.
 *
 * @author Mark Paluch
 * @since 2.1
 * @see SimpleTupleTypeFactory
 * @see CodecRegistryTupleTypeFactory
 * @deprecated since 3.0
 */
@FunctionalInterface
@Deprecated
public interface TupleTypeFactory {

	/**
	 * Create a {@link TupleType} representing the given {@link DataType tuple element types}.
	 *
	 * @param types must not be {@literal null} and not contain {@literal null} elements.
	 * @return the {@link TupleType} representing the given {@link DataType tuple element types}.
	 */
	default TupleType create(DataType... types) {

		Assert.notNull(types, "DataType must not be null");
		Assert.noNullElements(types, "DataType must not contain null elements");

		return create(Arrays.asList(types));
	}

	/**
	 * Create a {@link TupleType} representing the given {@link DataType tuple element types}.
	 *
	 * @param types must not be {@literal null}.
	 * @return the {@link TupleType} representing the given {@link DataType tuple element types}.
	 */
	TupleType create(List<DataType> types);

}
