/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.springframework.data.annotation.Persistent;
import org.springframework.data.mapping.model.MappingException;
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
	 * Creates a new {@link CompositeCassandraPersistentEntityMetadataVerifier} using default entity and primary key
	 * verifiers.
	 *
	 * @see BasicCassandraPersistentEntityMetadataVerifier
	 * @see PrimaryKeyClassEntityMetadataVerifier
	 */
	public CompositeCassandraPersistentEntityMetadataVerifier() {
		this(Arrays.asList(new PersistentAnnotationVerifier(), new PrimaryKeyClassEntityMetadataVerifier(),
			new BasicCassandraPersistentEntityMetadataVerifier()));
	}

	/**
	 * Creates a new {@link CompositeCassandraPersistentEntityMetadataVerifier} for the given {@code verifiers}
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
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntityMetadataVerifier#verify(org.springframework.data.cassandra.mapping.CassandraPersistentEntity)
	 */
	@Override
	public void verify(CassandraPersistentEntity<?> entity) throws MappingException {
		for (CassandraPersistentEntityMetadataVerifier verifier : verifiers) {
			verifier.verify(entity);
		}
	}

	/**
	 * {@link CassandraPersistentEntityMetadataVerifier} implementation that requires classes to be annotated with
	 * {@link Persistent}, {@link Table} or {@link PrimaryKeyClass}.
	 *
	 * @author Mark Paluch
	 */
	private static class PersistentAnnotationVerifier implements CassandraPersistentEntityMetadataVerifier {

		@Override
		public void verify(CassandraPersistentEntity<?> entity) throws MappingException {
			if (entity.getType().isInterface()) {
				return;
			}

			// Ensure entity is either a @Table/@Persistent or a @PrimaryKey
			if (entity.findAnnotation(Persistent.class) == null) {
				throw new VerifierMappingExceptions(entity, Collections.singletonList(new MappingException(
					String.format("Cassandra entities must be annotated with either @%s, @%s, or @%s",
						Persistent.class.getSimpleName(), Table.class.getSimpleName(), PrimaryKeyClass.class.getSimpleName()))));
			}
		}
	}
}
