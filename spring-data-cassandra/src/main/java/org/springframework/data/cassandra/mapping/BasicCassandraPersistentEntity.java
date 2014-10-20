/*
 * Copyright 2013-2014 the original author or authors
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
package org.springframework.data.cassandra.mapping;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.support.exception.UnsupportedCassandraOperationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.cassandra.util.SpelUtils;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Cassandra specific {@link BasicPersistentEntity} implementation that adds Cassandra specific metadata.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class BasicCassandraPersistentEntity<T> extends BasicPersistentEntity<T, CassandraPersistentProperty> implements
		CassandraPersistentEntity<T>, ApplicationContextAware {

	protected static final CassandraPersistentEntityMetadataVerifier DEFAULT_VERIFIER = new BasicCassandraPersistentEntityMetadataVerifier();

	protected CqlIdentifier tableName;
	protected CassandraMappingContext mappingContext;
	protected StandardEvaluationContext spelContext;
	protected CassandraPersistentEntityMetadataVerifier verifier = DEFAULT_VERIFIER;
	protected ApplicationContext context;
	protected Boolean forceQuote;

	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation) {
		this(typeInformation, null);
	}

	/**
	 * Creates a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 * 
	 * @param typeInformation
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation, CassandraMappingContext mappingContext) {
		this(typeInformation, mappingContext, DEFAULT_VERIFIER);

	}

	/**
	 * Creates a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 * 
	 * @param typeInformation
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation, CassandraMappingContext mappingContext,
			CassandraPersistentEntityMetadataVerifier verifier) {

		super(typeInformation, CassandraPersistentPropertyComparator.IT);

		this.mappingContext = mappingContext;

		setVerifier(verifier);
	}

	protected CqlIdentifier determineTableName() {

		Table anno = getType().getAnnotation(Table.class);

		if (anno == null || !StringUtils.hasText(anno.value())) {
			return cqlId(getType().getSimpleName(), anno == null ? false : anno.forceQuote());
		}

		return cqlId(spelContext == null ? anno.value() : SpelUtils.evaluate(anno.value(), spelContext), anno.forceQuote());
	}

	@Override
	public void addAssociation(Association<CassandraPersistentProperty> association) {
		throw new UnsupportedCassandraOperationException("Cassandra does not support associations");
	}

	@Override
	public void doWithAssociations(AssociationHandler<CassandraPersistentProperty> handler) {
		throw new UnsupportedCassandraOperationException("Cassandra does not support associations");
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {

		Assert.notNull(context);

		this.context = context;
		spelContext = new StandardEvaluationContext();
		spelContext.addPropertyAccessor(new BeanFactoryAccessor());
		spelContext.setBeanResolver(new BeanFactoryResolver(context));
		spelContext.setRootObject(context);
	}

	@Override
	public CqlIdentifier getTableName() {

		if (tableName != null) {
			return tableName;
		}

		return tableName = determineTableName();
	}

	@Override
	public void setTableName(CqlIdentifier tableName) {

		Assert.notNull(tableName);
		this.tableName = tableName;
	}

	@Override
	public void setForceQuote(boolean forceQuote) {

		if (this.forceQuote != null && this.forceQuote == forceQuote) {
			return;
		} else {
			this.forceQuote = forceQuote;
		}

		setTableName(cqlId(tableName.getUnquoted(), forceQuote));
	}

	@Override
	public CassandraMappingContext getMappingContext() {
		return mappingContext;
	}

	@Override
	public boolean isCompositePrimaryKey() {
		return getType().isAnnotationPresent(PrimaryKeyClass.class);
	}

	@Override
	public List<CassandraPersistentProperty> getCompositePrimaryKeyProperties() {

		final List<CassandraPersistentProperty> properties = new ArrayList<CassandraPersistentProperty>();

		if (!isCompositePrimaryKey()) {
			throw new IllegalStateException(String.format("[%s] does not represent a composite primary key class", this
					.getType().getName()));
		}

		addCompositePrimaryKeyProperties(this, properties);

		return properties;
	}

	protected void addCompositePrimaryKeyProperties(CassandraPersistentEntity<?> compositePrimaryKeyEntity,
			final List<CassandraPersistentProperty> properties) {

		compositePrimaryKeyEntity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty p) {

				if (p.isCompositePrimaryKey()) {
					addCompositePrimaryKeyProperties(p.getCompositePrimaryKeyEntity(), properties);
				} else {
					properties.add(p);
				}
			}
		});
	}

	@Override
	public void verify() throws MappingException {
		super.verify();
		if (verifier != null) {
			verifier.verify(this);
		}
	}

	/**
	 * @return Returns the verifier.
	 */
	public CassandraPersistentEntityMetadataVerifier getVerifier() {
		return verifier;
	}

	/**
	 * @param verifier The verifier to set.
	 */
	public void setVerifier(CassandraPersistentEntityMetadataVerifier verifier) {
		this.verifier = verifier;
	}

	@Override
	public ApplicationContext getApplicationContext() {
		return context;
	}

    @Override
    public String toString() {
        return "Entity [" + tableName +"]";
    }
}
