/*
 * Copyright 2016-2021 the original author or authors.
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
import java.util.Collection;

import org.springframework.data.mapping.MappingException;
import org.springframework.util.Assert;

/**
 * Composite {@link CassandraPersistentEntityMetadataVerifier} to verify persistent entities and primary key classes.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see BasicCassandraPersistentEntityMetadataVerifier
 * @see PrimaryKeyClassEntityMetadataVerifier
 */
public class CompositeCassandraPersistentEntityMetadataVerifier implements CassandraPersistentEntityMetadataVerifier {

	private Collection<CassandraPersistentEntityMetadataVerifier> verifiers;

	/**
	 * Create a new {@link CompositeCassandraPersistentEntityMetadataVerifier} using default entity and primary key
	 * verifiers.
	 *
	 * @see BasicCassandraPersistentEntityMetadataVerifier
	 * @see PrimaryKeyClassEntityMetadataVerifier
	 */
	public CompositeCassandraPersistentEntityMetadataVerifier() {
		this(Arrays.asList(new PrimaryKeyClassEntityMetadataVerifier(),
				new BasicCassandraPersistentEntityMetadataVerifier()));
	}

	/**
	 * Create a new {@link CompositeCassandraPersistentEntityMetadataVerifier} for the given {@code verifiers}
	 *
	 * @param verifiers must not be {@literal null}.
	 */
	private CompositeCassandraPersistentEntityMetadataVerifier(
			Collection<CassandraPersistentEntityMetadataVerifier> verifiers) {

		Assert.notNull(verifiers, "Verifiers must not be null");

		this.verifiers = verifiers;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentEntityMetadataVerifier#verify(org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity)
	 */
	@Override
	public void verify(CassandraPersistentEntity<?> entity) throws MappingException {
		verifiers.forEach(verifier -> verifier.verify(entity));
	}
}
