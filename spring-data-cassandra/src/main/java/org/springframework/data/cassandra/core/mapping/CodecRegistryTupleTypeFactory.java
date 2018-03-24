/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.util.List;

import org.springframework.util.Assert;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;

/**
 * {@link CodecRegistry}-based {@link TupleTypeFactory} using
 * {@link TupleType#of(ProtocolVersion, CodecRegistry, DataType...)} to create tuple types. {@link TupleType tuple
 * types}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public class CodecRegistryTupleTypeFactory implements TupleTypeFactory {

	/**
	 * Default {@link CodecRegistryTupleTypeFactory} using newest protocol versions and the default {@link CodecRegistry}.
	 */
	public static final CodecRegistryTupleTypeFactory DEFAULT = new CodecRegistryTupleTypeFactory();

	private final CodecRegistry codecRegistry;

	private final ProtocolVersion protocolVersion;

	/**
	 * Creates a new {@link CodecRegistryTupleTypeFactory} using newest protocol version and the default
	 * {@link CodecRegistry}.
	 */
	private CodecRegistryTupleTypeFactory() {
		this(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE);
	}

	/**
	 * Creates a new {@link CodecRegistryTupleTypeFactory} given {@link ProtocolVersion} and {@link CodecRegistry}.
	 *
	 * @param protocolVersion must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 */
	public CodecRegistryTupleTypeFactory(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {

		Assert.notNull(protocolVersion, "ProtocolVersion must not be null");
		Assert.notNull(codecRegistry, "CodecRegistry must not be null");

		this.protocolVersion = protocolVersion;
		this.codecRegistry = codecRegistry;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.TupleTypeFactory#create(com.datastax.driver.core.DataType[])
	 */
	@Override
	public TupleType create(DataType... types) {
		return TupleType.of(this.protocolVersion, this.codecRegistry, types);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.TupleTypeFactory#create(java.util.List)
	 */
	@Override
	public TupleType create(List<DataType> types) {
		return create(types.toArray(new DataType[types.size()]));
	}
}
