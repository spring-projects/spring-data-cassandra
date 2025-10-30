/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import java.lang.reflect.Method;
import java.util.Locale;

import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.Query.Idempotency;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * Cassandra specific implementation of {@link QueryMethod}.
 *
 * @author Matthew Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author John Blum
 */
public class CassandraQueryMethod extends QueryMethod {

	private final Method method;

	private final MappingContext<? extends CassandraPersistentEntity<?>, ? extends CassandraPersistentProperty> mappingContext;

	private final @Nullable Query query;

	private final @Nullable Consistency consistency;

	private @Nullable CassandraEntityMetadata<?> entityMetadata;

	/**
	 * Create a new {@link CassandraQueryMethod} from the given {@link Method}.
	 *
	 * @param method must not be {@literal null}.
	 * @param repositoryMetadata must not be {@literal null}.
	 * @param projectionFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public CassandraQueryMethod(Method method, RepositoryMetadata repositoryMetadata, ProjectionFactory projectionFactory,
			MappingContext<? extends CassandraPersistentEntity<?>, ? extends CassandraPersistentProperty> mappingContext) {

		super(method, repositoryMetadata, projectionFactory, CassandraParameters::new);

		Assert.notNull(mappingContext, "MappingContext must not be null");

		verify(method, repositoryMetadata);

		this.method = method;
		this.mappingContext = mappingContext;
		this.query = AnnotatedElementUtils.findMergedAnnotation(method, Query.class);
		this.consistency = AnnotatedElementUtils.findMergedAnnotation(method, Consistency.class);
	}

	/**
	 * Validates that this query is not a page query.
	 */
	@SuppressWarnings("unused")
	public void verify(Method method, RepositoryMetadata metadata) {

		if (isPageQuery()) {
			throw new InvalidDataAccessApiUsageException("Page queries are not supported; Use a Slice query");
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public CassandraEntityMetadata<?> getEntityInformation() {

		if (this.entityMetadata == null) {

			Class<?> returnedObjectType = getReturnedObjectType();
			Class<?> domainClass = getDomainClass();

			if (ClassUtils.isPrimitiveOrWrapper(returnedObjectType)) {
				this.entityMetadata = new SimpleCassandraEntityMetadata<>((Class<Object>) domainClass,
						this.mappingContext.getRequiredPersistentEntity(domainClass));

			} else {

				CassandraPersistentEntity<?> returnedEntity = this.mappingContext.getPersistentEntity(returnedObjectType);
				CassandraPersistentEntity<?> managedEntity = this.mappingContext.getRequiredPersistentEntity(domainClass);

				returnedEntity = returnedEntity == null || returnedEntity.getType().isInterface() ? managedEntity
						: returnedEntity;

				this.entityMetadata = new SimpleCassandraEntityMetadata<>((Class<Object>) returnedEntity.getType(),
						managedEntity);
			}
		}

		return this.entityMetadata;
	}

	@Override
	public CassandraParameters getParameters() {
		return (CassandraParameters) super.getParameters();
	}

	/**
	 * Returns whether the method has an annotated query.
	 */
	public boolean hasAnnotatedQuery() {
		return this.query != null && StringUtils.hasText(this.query.value());
	}

	/**
	 * Returns the query string declared in a {@link Query} annotation or {@literal null} if neither the annotation found
	 * nor the attribute was specified.
	 *
	 * @return the query string or {@literal null} if no query string present.
	 */
	public @Nullable String getAnnotatedQuery() {
		return this.query != null ? this.query.value() : null;
	}

	/**
	 * @return whether the method has an annotated {@link ConsistencyLevel}.
	 * @since 2.0
	 */
	public boolean hasConsistencyLevel() {
		return this.consistency != null;
	}

	/**
	 * Returns the {@link ConsistencyLevel} in a {@link Query} annotation or throws {@link IllegalStateException} if the
	 * annotation was not found.
	 *
	 * @return the {@link ConsistencyLevel}.
	 * @throws IllegalStateException if the required annotation was not found.
	 */
	public ConsistencyLevel getRequiredAnnotatedConsistencyLevel() throws IllegalStateException {

		if (this.consistency == null) {
			throw new IllegalStateException("No @Consistency annotation found");
		}

		return this.consistency.value();
	}

	/**
	 * Returns the required query string declared in a {@link Query} annotation or throws {@link IllegalStateException} if
	 * neither the annotation found nor the attribute was specified.
	 *
	 * @return the query string.
	 * @throws IllegalStateException in case query method has no annotated query.
	 */
	public String getRequiredAnnotatedQuery() {

		if (this.query == null) {
			throw new IllegalStateException("Query method " + this + " has no annotated query");
		}

		return this.query.value();
	}

	/**
	 * Returns the {@link Query} annotation that is applied to the method.
	 */
	@Nullable
	Query getQueryAnnotation() {
		return this.query;
	}

	/**
	 * Returns the {@link Query} annotation that is applied to the method.
	 *
	 * @return the required query annotation.
	 */
	Query getRequiredQueryAnnotation() {

		Query query = getQueryAnnotation();
		if (query == null) {
			throw new IllegalStateException("No @Consistency annotation found");
		}

		return query;
	}

	@Override
	protected Class<?> getDomainClass() {
		return super.getDomainClass();
	}

	/**
	 * @return the return type for this {@link QueryMethod}.
	 */
	public TypeInformation<?> getReturnType() {
		return TypeInformation.fromReturnTypeOf(this.method);
	}

	/**
	 * @return {@literal true} if the method returns a {@link ResultSet}.
	 */
	public boolean isResultSetQuery() {

		TypeInformation<?> actualType = getReturnType().getActualType();

		return actualType != null && ResultSet.class.isAssignableFrom(actualType.getType());
	}

	/**
	 * @return Query {@link Idempotency}. Defaults to {@link Idempotency#IDEMPOTENT} for {@code SELECT} queries.
	 */
	Idempotency getIdempotency() {

		if (this.query == null) {
			return Idempotency.UNDEFINED;
		}

		if (this.query.idempotent() != Idempotency.UNDEFINED) {
			return this.query.idempotent();
		}

		String cql = getAnnotatedQuery();
		if (StringUtils.hasText(cql)) {

			if (cql.trim().toUpperCase(Locale.ENGLISH).startsWith("SELECT ")) {
				return Idempotency.IDEMPOTENT;
			}
		}

		return Idempotency.UNDEFINED;
	}
}
