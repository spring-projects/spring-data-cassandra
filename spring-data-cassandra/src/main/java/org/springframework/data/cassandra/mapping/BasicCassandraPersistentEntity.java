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

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.cassandra.exception.UnsupportedCassandraOperationException;
import org.springframework.data.cassandra.util.CassandraNamingUtils;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.StringUtils;

/**
 * Cassandra specific {@link BasicPersistentEntity} implementation that adds Cassandra specific metadata such as the
 * table name.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class BasicCassandraPersistentEntity<T> extends BasicPersistentEntity<T, CassandraPersistentProperty> implements
		CassandraPersistentEntity<T>, ApplicationContextAware {

	private String table;
	private final SpelExpressionParser spelParser;
	private final StandardEvaluationContext spelContext;
	private final Class<T> type;

	/**
	 * Creates a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 * 
	 * @param typeInformation
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation) {

		super(typeInformation, DefaultCassandraPersistentPropertyColumnComparator.IT);

		this.spelParser = new SpelExpressionParser();
		this.spelContext = new StandardEvaluationContext();

		this.type = typeInformation.getType();

		determineTableName();
	}

	protected void determineTableName() {
		Table anno = type.getAnnotation(Table.class);

		this.table = anno != null && StringUtils.hasText(anno.value()) ? anno.value() : CassandraNamingUtils
				.getPreferredTableName(type);
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
		Expression expression = spelParser.parseExpression(table, ParserContext.TEMPLATE_EXPRESSION);
		return expression.getValue(spelContext, String.class);
	}
}
