/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mapping.PersistentProperty;

/**
 * Cassandra specific {@link org.springframework.data.mapping.PersistentProperty} implementation.
 * 
 * @author Brian O'Neill
 */
public interface CassandraPersistentProperty extends PersistentProperty<CassandraPersistentProperty> {

	/**
	 * Returns the name of the column that the property is persisted to.
	 * 
	 * @return
	 */
	String getColumnName();
	
	/**
	 * Returns the order of the field if defined. Will return -1 if undefined.
	 * 
	 * @return
	 */
	int getFieldOrder();
	
	/**
	 * Returns the name of the field a property is persisted to.
	 * 
	 * @return
	 */
	String getFieldName();

	/**
	 * Simple {@link Converter} implementation to transform a {@link CassandraPersistentProperty} into its column name.
	 * 
	 * @author Brian O'Neill
	 */
	public enum PropertyToColumnNameConverter implements Converter<CassandraPersistentProperty, String> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		public String convert(CassandraPersistentProperty source) {
			return source.getColumnName();
		}
	}
}
