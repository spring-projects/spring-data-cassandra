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

import java.util.List;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * {@link CodecRegistry}-based {@link TupleTypeFactory} using
 * {@link TupleType#of(ProtocolVersion, CodecRegistry, DataType...)} to create tuple types. {@link TupleType tuple
 * types}.
 *
 * @author Mark Paluch
 * @since 2.1
 * @deprecated since 3.0, use {@link SimpleTupleTypeFactory} instead.
 */
@Deprecated
public class CodecRegistryTupleTypeFactory implements TupleTypeFactory {

	/**
	 * Default {@link CodecRegistryTupleTypeFactory} using default protocol versions and the default
	 * {@link CodecRegistry}.
	 */
	public static final CodecRegistryTupleTypeFactory DEFAULT = new CodecRegistryTupleTypeFactory();

	/**
	 * Creates a new {@link CodecRegistryTupleTypeFactory} using newest protocol version and the default
	 * {@link CodecRegistry}.
	 */
	private CodecRegistryTupleTypeFactory() {
		this(ProtocolVersion.DEFAULT, CodecRegistry.DEFAULT);
	}

	/**
	 * Creates a new {@link CodecRegistryTupleTypeFactory} given {@link ProtocolVersion} and {@link CodecRegistry}.
	 *
	 * @param protocolVersion must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 */
	@Deprecated
	public CodecRegistryTupleTypeFactory(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.TupleTypeFactory#create(com.datastax.oss.driver.api.core.type.DataType[])
	 */
	@Override
	public TupleType create(DataType... types) {
		return SimpleTupleTypeFactory.DEFAULT.create(types);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.TupleTypeFactory#create(java.util.List)
	 */
	@Override
	public TupleType create(List<DataType> types) {
		return SimpleTupleTypeFactory.DEFAULT.create(types);
	}
}
