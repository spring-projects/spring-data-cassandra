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

import java.util.Comparator;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.cassandra.util.CassandraNamingUtils;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.StringUtils;

/**
 * Cassandra specific {@link BasicPersistentEntity} implementation that adds Cassandra specific meta-data such as the
 * table name.
 * 
 * @author Alex Shvid
 */
public class BasicCassandraPersistentEntity<T> extends BasicPersistentEntity<T, CassandraPersistentProperty> implements
		CassandraPersistentEntity<T>, ApplicationContextAware {

	private final String table;
	private final SpelExpressionParser parser;
	private final StandardEvaluationContext context;

	/**
	 * Creates a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entities simple type name.
	 * 
	 * @param typeInformation
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation) {

		super(typeInformation, CassandraPersistentPropertyComparator.INSTANCE);

		this.parser = new SpelExpressionParser();
		this.context = new StandardEvaluationContext();

		Class<?> rawType = typeInformation.getType();
		String fallback = CassandraNamingUtils.getPreferredTableName(rawType);

		if (rawType.isAnnotationPresent(Table.class)) {
			Table d = rawType.getAnnotation(Table.class);
			this.table = StringUtils.hasText(d.name()) ? d.name() : fallback;
		} else {
			this.table = fallback;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		context.addPropertyAccessor(new BeanFactoryAccessor());
		context.setBeanResolver(new BeanFactoryResolver(applicationContext));
		context.setRootObject(applicationContext);
	}

	/**
	 * Returns the table the entity shall be persisted to.
	 * 
	 * @return
	 */
	public String getTable() {
		Expression expression = parser.parseExpression(table, ParserContext.TEMPLATE_EXPRESSION);
		return expression.getValue(context, String.class);
	}

	/**
	 * {@link Comparator} implementation inspecting the {@link CassandraPersistentProperty}'s order.
	 * 
	 * @author Alex Shvid
	 */
	static enum CassandraPersistentPropertyComparator implements Comparator<CassandraPersistentProperty> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		public int compare(CassandraPersistentProperty o1, CassandraPersistentProperty o2) {

			return o1.getColumnName().compareTo(o2.getColumnName());

		}
	}

}
