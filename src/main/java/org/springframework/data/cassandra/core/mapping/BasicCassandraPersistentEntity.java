/*
 * Copyright 2011-2013 the original author or authors.
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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * CassandraDB specific {@link CassandraPersistentEntity} implementation that adds Cassandra specific meta-data such as the
 * collection name and the like.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 */
public class BasicCassandraPersistentEntity<T> extends BasicPersistentEntity<T, CassandraPersistentProperty> implements
		CassandraPersistentEntity<T>, ApplicationContextAware {

	private static final String AMBIGUOUS_FIELD_MAPPING = "Ambiguous field mapping detected! Both %s and %s map to the same field name %s! Disambiguate using @Field annotation!";
	private String columnFamily = "";
	private final StandardEvaluationContext context;

	/**
	 * Creates a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the
	 * collection name to the entities simple type name.
	 * 
	 * @param typeInformation
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation) {

		super(typeInformation, CassandraPersistentPropertyComparator.INSTANCE);

		this.context = new StandardEvaluationContext();

		Class<?> rawType = typeInformation.getType();

		if (rawType.isAnnotationPresent(ColumnFamily.class)) {
			ColumnFamily cf = rawType.getAnnotation(ColumnFamily.class);
			this.columnFamily = cf.columnFamilyName();
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

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.BasicPersistentEntity#verify()
	 */
	@Override
	public void verify() {

		AssertFieldNameUniquenessHandler handler = new AssertFieldNameUniquenessHandler();

		doWithProperties(handler);
		doWithAssociations(handler);
	}

	/**
	 * {@link Comparator} implementation inspecting the {@link CassandraPersistentProperty}'s order.
	 * 
	 * @author Oliver Gierke
	 */
	static enum CassandraPersistentPropertyComparator implements Comparator<CassandraPersistentProperty> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		public int compare(CassandraPersistentProperty o1, CassandraPersistentProperty o2) {

			if (o1.getFieldOrder() == Integer.MAX_VALUE) {
				return 1;
			}

			if (o2.getFieldOrder() == Integer.MAX_VALUE) {
				return -1;
			}

			return o1.getFieldOrder() - o2.getFieldOrder();
		}
	}

	/**
	 * Handler to collect {@link CassandraPersistentProperty} instances and check that each of them is mapped to a distinct
	 * field name.
	 * 
	 * @author Oliver Gierke
	 */
	private static class AssertFieldNameUniquenessHandler implements PropertyHandler<CassandraPersistentProperty>,
			AssociationHandler<CassandraPersistentProperty> {

		private final Map<String, CassandraPersistentProperty> properties = new HashMap<String, CassandraPersistentProperty>();

		public void doWithPersistentProperty(CassandraPersistentProperty persistentProperty) {
			assertUniqueness(persistentProperty);
		}

		public void doWithAssociation(Association<CassandraPersistentProperty> association) {
			assertUniqueness(association.getInverse());
		}

		private void assertUniqueness(CassandraPersistentProperty property) {

			String fieldName = property.getFieldName();
			CassandraPersistentProperty existingProperty = properties.get(fieldName);

			if (existingProperty != null) {
				throw new MappingException(String.format(AMBIGUOUS_FIELD_MAPPING, property.toString(),
						existingProperty.toString(), fieldName));
			}

			properties.put(fieldName, property);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity#getColumnFamily()
	 */
	public String getColumnFamily() {
		
		return this.columnFamily;
	}
}
