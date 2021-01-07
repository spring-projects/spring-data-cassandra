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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;

/**
 * {@link CodecRegistry}-based {@link TupleTypeFactory} using {@link DefaultTupleType} to create tuple types.
 * {@link TupleType tuple types}.
 *
 * @author Mark Paluch
 * @since 3.0
 */

public enum SimpleTupleTypeFactory implements TupleTypeFactory {

	/**
	 * Default {@link SimpleTupleTypeFactory} using newest protocol versions and the default {@link CodecRegistry}.
	 */
	DEFAULT;

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.TupleTypeFactory#create(com.datastax.oss.driver.api.core.type.DataType[])
	 */
	@Override
	public TupleType create(DataType... types) {
		return create(Arrays.asList(types));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.TupleTypeFactory#create(java.util.List)
	 */
	@Override
	public TupleType create(List<DataType> types) {
		return new DefaultTupleType(types);
	}
}
