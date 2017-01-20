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
package org.springframework.data.cassandra.mapping;

import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.MutablePersistentEntity;

import com.datastax.driver.core.UserType;

/**
 * Cassandra specific {@link PersistentEntity} abstraction.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
// TODO: Not extend MutablePersistentEntity but rather PersistentEntity.
public interface CassandraPersistentEntity<T>
		extends MutablePersistentEntity<T, CassandraPersistentProperty>, ApplicationContextAware {

	/**
	 * Returns whether this entity represents a composite primary key.
	 */
	boolean isCompositePrimaryKey();

	// TODO: return rather a Stream, rename to "getPrimaryKeyProperties"
	List<CassandraPersistentProperty> getCompositePrimaryKeyProperties();

	/**
	 * Returns the table name to which the entity shall be persisted.
	 */
	CqlIdentifier getTableName();

	/**
	 * Sets the CQL table name.
	 * 
	 * @param tableName must not be {@literal null}.
	 */
	void setTableName(CqlIdentifier tableName);

	/**
	 * Sets whether to enforce quoting when using the {@link #getTableName()} in CQL.
	 * 
	 * @param forceQuote {@literal true} to enforce quoting; {@literal false} to disable enforced quoting usage.
	 */
	void setForceQuote(boolean forceQuote);

	/**
	 * @return {@literal true} if the type is a mapped user defined type
	 * @since 1.5
	 * @see UserDefinedType
	 */
	boolean isUserDefinedType();

	/**
	 * @return the CQL {@link UserType} if the type is a mapped user defined type, otherwise {@literal null}.
	 * @since 1.5
	 * @see UserDefinedType
	 */
	UserType getUserType();

	// TODO: Review if that's required or it can be handled in a different way
	CassandraMappingContext getMappingContext();

	ApplicationContext getApplicationContext();
}
