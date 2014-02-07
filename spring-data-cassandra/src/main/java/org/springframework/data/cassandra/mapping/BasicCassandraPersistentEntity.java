/*
 * Copyright 2011-2012 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.cassandra.support.exception.UnsupportedCassandraOperationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.cassandra.util.CassandraNamingUtils;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
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

	protected String tableName;
	protected CassandraMappingContext mappingContext;
	protected final SpelExpressionParser spelParser;
	protected final StandardEvaluationContext spelContext;

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

		super(typeInformation, CassandraPersistentPropertyComparator.IT);

		this.spelParser = new SpelExpressionParser();
		this.spelContext = new StandardEvaluationContext();
		this.mappingContext = mappingContext;

		determineTableName();
	}

	protected void determineTableName() {
		Table anno = getType().getAnnotation(Table.class);

		this.tableName = anno != null && StringUtils.hasText(anno.value()) ? anno.value() : CassandraNamingUtils
				.getPreferredTableName(getType());
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
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		spelContext.addPropertyAccessor(new BeanFactoryAccessor());
		spelContext.setBeanResolver(new BeanFactoryResolver(applicationContext));
		spelContext.setRootObject(applicationContext);
	}

	@Override
	public String getTableName() {
		Expression expression = spelParser.parseExpression(tableName, ParserContext.TEMPLATE_EXPRESSION);
		return expression.getValue(spelContext, String.class);
	}

	@Override
	public void setTableName(String tableName) {
		Assert.hasText(tableName);
		this.tableName = tableName;
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
}
