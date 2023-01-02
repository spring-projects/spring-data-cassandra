/*
 * Copyright 2013-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.lang.annotation.Annotation;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.BiFunction;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Cassandra specific {@link BasicPersistentEntity} implementation that adds Cassandra specific metadata.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 */
public class BasicCassandraPersistentEntity<T> extends BasicPersistentEntity<T, CassandraPersistentProperty>
		implements CassandraPersistentEntity<T>, ApplicationContextAware {

	private static final CassandraPersistentEntityMetadataVerifier DEFAULT_VERIFIER = new CompositeCassandraPersistentEntityMetadataVerifier();

	private final CqlIdentifierGenerator namingAccessor = new CqlIdentifierGenerator();

	private Boolean forceQuote;

	private CassandraPersistentEntityMetadataVerifier verifier = DEFAULT_VERIFIER;

	private CqlIdentifier tableName;

	private @Nullable StandardEvaluationContext spelContext;

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} given {@link TypeInformation}.
	 *
	 * @param typeInformation must not be {@literal null}.
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation) {
		this(typeInformation, DEFAULT_VERIFIER);
	}

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation,
			CassandraPersistentEntityMetadataVerifier verifier) {

		super(typeInformation, CassandraPersistentPropertyComparator.INSTANCE);

		setVerifier(verifier);
	}

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 * @param comparator must not be {@literal null}.
	 * @since 2.1
	 */
	protected BasicCassandraPersistentEntity(TypeInformation<T> typeInformation,
			CassandraPersistentEntityMetadataVerifier verifier, Comparator<CassandraPersistentProperty> comparator) {

		super(typeInformation, comparator);

		setVerifier(verifier);
	}

	protected CqlIdentifier determineTableName() {
		return determineTableName(NamingStrategy::getTableName, findAnnotation(Table.class));
	}

	CqlIdentifier determineTableName(
			BiFunction<NamingStrategy, CassandraPersistentEntity<?>, String> defaultNameGenerator,
			@Nullable Annotation annotation) {

		if (annotation != null) {
			return this.namingAccessor.generate((String) AnnotationUtils.getValue(annotation),
					(Boolean) AnnotationUtils.getValue(annotation, "forceQuote"), defaultNameGenerator, this, this.spelContext);
		}

		return this.namingAccessor.generate(null, forceQuote != null ? forceQuote : false, defaultNameGenerator, this,
				this.spelContext);
	}

	@Override
	public void addAssociation(Association<CassandraPersistentProperty> association) {
		throw new UnsupportedCassandraOperationException("Cassandra does not support associations");
	}

	@Override
	public void doWithAssociations(AssociationHandler<CassandraPersistentProperty> handler) {}

	@Override
	public boolean isCompositePrimaryKey() {
		return isAnnotationPresent(PrimaryKeyClass.class);
	}

	@Override
	public void verify() throws MappingException {

		super.verify();

		this.verifier.verify(this);

		if (this.tableName == null) {
			setTableName(determineTableName());
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {

		Assert.notNull(context, "ApplicationContext must not be null");

		spelContext = new StandardEvaluationContext();
		spelContext.addPropertyAccessor(new BeanFactoryAccessor());
		spelContext.setBeanResolver(new BeanFactoryResolver(context));
		spelContext.setRootObject(context);
	}

	@Override
	public void setForceQuote(boolean forceQuote) {

		boolean changed = !Boolean.valueOf(forceQuote).equals(this.forceQuote);

		this.forceQuote = forceQuote;

		if (changed) {
			setTableName(CqlIdentifierGenerator.createIdentifier(getTableName().asInternal(), forceQuote));
		}
	}

	@Override
	public void setTableName(CqlIdentifier tableName) {

		Assert.notNull(tableName, "CqlIdentifier must not be null");

		this.tableName = tableName;
	}

	/**
	 * Set the {@link NamingStrategy} to use.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 * @since 3.0
	 */
	public void setNamingStrategy(NamingStrategy namingStrategy) {
		this.namingAccessor.setNamingStrategy(namingStrategy);
	}

	@Override
	public CqlIdentifier getTableName() {
		return Optional.ofNullable(this.tableName).orElseGet(this::determineTableName);
	}

	/**
	 * @param verifier The verifier to set.
	 */
	public void setVerifier(CassandraPersistentEntityMetadataVerifier verifier) {
		this.verifier = verifier;
	}

	/**
	 * @return the verifier.
	 */
	@SuppressWarnings("unused")
	public CassandraPersistentEntityMetadataVerifier getVerifier() {
		return this.verifier;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public boolean isUserDefinedType() {
		return false;
	}
}
