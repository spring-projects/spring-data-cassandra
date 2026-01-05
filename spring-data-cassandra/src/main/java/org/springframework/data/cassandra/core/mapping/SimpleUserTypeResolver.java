/*
 * Copyright 2016-present the original author or authors.
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

import java.util.function.Supplier;

import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Default implementation of {@link UserTypeResolver} that resolves a {@link UserDefinedType} by its name from
 * {@link Metadata}.
 *
 * @author Mark Paluch
 * @since 1.5
 */
public class SimpleUserTypeResolver implements UserTypeResolver {

	private final Supplier<Metadata> metadataSupplier;

	private final Supplier<CqlIdentifier> keyspaceName;

	/**
	 * Create a new {@link SimpleUserTypeResolver}.
	 *
	 * @param session must not be {@literal null}.
	 * @since 3.0
	 */
	public SimpleUserTypeResolver(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		this.metadataSupplier = session::getMetadata;
		this.keyspaceName = Lazy.of(() -> session.getKeyspace().orElse(CqlIdentifier.fromCql("system")));
	}

	/**
	 * Create a new {@link SimpleUserTypeResolver}.
	 *
	 * @param session must not be {@literal null}.
	 * @param keyspaceName must not be {@literal null}.
	 * @since 3.0
	 */
	public SimpleUserTypeResolver(CqlSession session, CqlIdentifier keyspaceName) {

		Assert.notNull(session, "Session must not be null");
		Assert.notNull(keyspaceName, "Keyspace must not be null");

		this.metadataSupplier = session::getMetadata;
		this.keyspaceName = Lazy.of(keyspaceName);
	}

	/**
	 * Create a new {@link SimpleUserTypeResolver}.
	 *
	 * @param metadataSupplier must not be {@literal null}.
	 * @param keyspaceName must not be {@literal null}.
	 * @since 3.2.2
	 */
	public SimpleUserTypeResolver(Supplier<Metadata> metadataSupplier, CqlIdentifier keyspaceName) {

		Assert.notNull(metadataSupplier, "Metadata supplier must not be null");
		Assert.notNull(keyspaceName, "Keyspace must not be null");

		this.metadataSupplier = metadataSupplier;
		this.keyspaceName = Lazy.of(keyspaceName);
	}

	@Nullable
	@Override
	public UserDefinedType resolveType(CqlIdentifier typeName) {
		return metadataSupplier.get().getKeyspace(keyspaceName.get()) //
				.flatMap(it -> it.getUserDefinedType(typeName)) //
				.orElse(null);
	}

	@Nullable
	@Override
	public UserDefinedType resolveType(CqlIdentifier keyspace, CqlIdentifier typeName) {
		return metadataSupplier.get().getKeyspace(keyspace) //
				.flatMap(it -> it.getUserDefinedType(typeName)) //
				.orElse(null);
	}
}
