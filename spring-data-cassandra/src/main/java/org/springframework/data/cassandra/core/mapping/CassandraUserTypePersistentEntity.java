/*
 * Copyright 2016-2018 the original author or authors.
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

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.UserType;

/**
 * {@link org.springframework.data.mapping.PersistentEntity} for a mapped user-defined type (UDT). A mapped UDT consists
 * of a set of fields. Each field requires a data type that can be either a simple Cassandra type or an UDT.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see UserDefinedType
 */
public class CassandraUserTypePersistentEntity<T> extends BasicCassandraPersistentEntity<T> {

	private final UserTypeResolver resolver;

	private final Object lock = new Object();

	private volatile @Nullable UserType userType;

	/**
	 * Create a new {@link CassandraUserTypePersistentEntity}.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 * @param resolver must not be {@literal null}.
	 */
	public CassandraUserTypePersistentEntity(TypeInformation<T> typeInformation,
			CassandraPersistentEntityMetadataVerifier verifier, UserTypeResolver resolver) {

		super(typeInformation, verifier);

		Assert.notNull(resolver, "UserTypeResolver must not be null");

		this.resolver = resolver;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity#determineTableName()
	 */
	@Override
	protected CqlIdentifier determineTableName() {

		UserDefinedType annotation = findAnnotation(UserDefinedType.class);

		if (annotation != null) {
			return determineName(annotation.value(), annotation.forceQuote());
		}

		return super.determineTableName();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity#isUserDefinedType()
	 */
	@Override
	public boolean isUserDefinedType() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity#getUserType()
	 */
	@Override
	public UserType getUserType() {

		if (userType == null) {
			synchronized (lock) {
				if (userType == null) {

					CqlIdentifier identifier = determineTableName();
					UserType userType = resolver.resolveType(identifier);

					if (userType == null) {
						throw new MappingException(String.format("User type [%s] not found", identifier));
					}

					this.userType = userType;
				}
			}
		}

		return userType;
	}
}
