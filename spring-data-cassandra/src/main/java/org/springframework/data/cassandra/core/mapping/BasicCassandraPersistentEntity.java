/*
 * Copyright 2013-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import static org.springframework.data.cql.core.CqlIdentifier.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.cassandra.util.SpelUtils;
import org.springframework.data.cql.core.CqlIdentifier;
import org.springframework.data.cql.support.exception.UnsupportedCassandraOperationException;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.UserType;

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

	private static final Optional<Comparator<CassandraPersistentProperty>> PROPERTY_COMPARATOR = Optional
			.of(CassandraPersistentPropertyComparator.INSTANCE);

	private CassandraPersistentEntityMetadataVerifier verifier = DEFAULT_VERIFIER;

	private MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private ApplicationContext context;

	private StandardEvaluationContext spelContext;

	private Optional<Boolean> forceQuote = Optional.empty();

	private Optional<CqlIdentifier> tableName = Optional.empty();

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} given {@link TypeInformation}.
	 *
	 * @param typeInformation must not be {@literal null}.
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation) {
		this(typeInformation, null, DEFAULT_VERIFIER);
	}

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 *
	 * @param typeInformation
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation,
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {
		this(typeInformation, mappingContext, DEFAULT_VERIFIER);
	}

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 *
	 * @param typeInformation
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation,
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext,
			CassandraPersistentEntityMetadataVerifier verifier) {

		// FIXME: Constructor with comparator, no optionality here
		super(typeInformation, PROPERTY_COMPARATOR);

		this.mappingContext = mappingContext;

		setVerifier(verifier);
	}

	protected CqlIdentifier determineTableName() {

		Optional<Table> tableAnnotation = findAnnotation(Table.class);

		return tableAnnotation //
				.map(annotation -> determineName(annotation.value(), annotation.forceQuote())) //
				.orElseGet(this::determineDefaultName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.BasicPersistentEntity#addAssociation(org.springframework.data.mapping.Association)
	 */
	@Override
	public void addAssociation(Association<CassandraPersistentProperty> association) {
		throw new UnsupportedCassandraOperationException("Cassandra does not support associations");
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.BasicPersistentEntity#doWithAssociations(org.springframework.data.mapping.AssociationHandler)
	 */
	@Override
	public void doWithAssociations(AssociationHandler<CassandraPersistentProperty> handler) {
		throw new UnsupportedCassandraOperationException("Cassandra does not support associations");
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#isCompositePrimaryKey()
	 */
	@Override
	public boolean isCompositePrimaryKey() {
		return findAnnotation(PrimaryKeyClass.class).isPresent();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#getCompositePrimaryKeyProperties()
	 */
	@Override
	public List<CassandraPersistentProperty> getCompositePrimaryKeyProperties() {

		List<CassandraPersistentProperty> properties = new ArrayList<>();

		Assert.state(isCompositePrimaryKey(),
				String.format("[%s] does not represent a composite primary key class", this.getType().getName()));

		addCompositePrimaryKeyProperties(this, properties);

		return properties;
	}

	protected void addCompositePrimaryKeyProperties(CassandraPersistentEntity<?> compositePrimaryKeyEntity,
			final List<CassandraPersistentProperty> properties) {

		compositePrimaryKeyEntity.getPersistentProperties().forEach(property -> {

			if (property.isCompositePrimaryKey()) {
				addCompositePrimaryKeyProperties(property.getCompositePrimaryKeyEntity(), properties);
			} else {
				properties.add(property);
			}

		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.BasicPersistentEntity#verify()
	 */
	@Override
	public void verify() throws MappingException {
		super.verify();

		if (verifier != null) {
			verifier.verify(this);
		}

		if (!tableName.isPresent()) {
			setTableName(determineTableName());
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {

		Assert.notNull(context, "ApplicationContext must not be null");

		this.context = context;
		spelContext = new StandardEvaluationContext();
		spelContext.addPropertyAccessor(new BeanFactoryAccessor());
		spelContext.setBeanResolver(new BeanFactoryResolver(context));
		spelContext.setRootObject(context);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#getApplicationContext()
	 */
	@Override
	public ApplicationContext getApplicationContext() {
		return context;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#setForceQuote(boolean)
	 */
	@Override
	public void setForceQuote(boolean forceQuote) {

		boolean changed = !this.forceQuote.isPresent() || this.forceQuote.filter(v -> v != forceQuote).isPresent();

		this.forceQuote = Optional.of(forceQuote);

		if (changed) {
			setTableName(cqlId(getTableName().getUnquoted(), forceQuote));
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#getMappingContext()
	 */
	@Override
	public MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#setTableName(org.springframework.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public void setTableName(CqlIdentifier tableName) {

		Assert.notNull(tableName, "CqlIdentifier must not be null");
		this.tableName = Optional.of(tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#getTableName()
	 */
	@Override
	public CqlIdentifier getTableName() {
		return tableName.orElseGet(this::determineTableName);
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
	public CassandraPersistentEntityMetadataVerifier getVerifier() {
		return verifier;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#isUserDefinedType()
	 */
	@Override
	public boolean isUserDefinedType() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntity#getUserType()
	 */
	@Override
	public UserType getUserType() {
		return null;
	}

	protected CqlIdentifier determineDefaultName() {
		return cqlId(getType().getSimpleName(), false);
	}

	protected CqlIdentifier determineName(String value, boolean forceQuote) {

		if (!StringUtils.hasText(value)) {
			return cqlId(getType().getSimpleName(), forceQuote);
		}

		return cqlId(spelContext == null ? value : SpelUtils.evaluate(value, spelContext), forceQuote);
	}
}
