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
package org.springframework.data.cassandra.config;

import java.util.Collection;

import org.springframework.cassandra.config.CassandraCqlSessionFactoryBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

public class CassandraSessionFactoryBean extends CassandraCqlSessionFactoryBean {

	protected SchemaAction schemaAction = SchemaAction.NONE;
	protected CassandraAdminTemplate admin;
	protected CassandraConverter converter;
	protected CassandraMappingContext mappingContext;

	@Override
	public void afterPropertiesSet() throws Exception {

		super.afterPropertiesSet();

		Assert.notNull(converter);

		admin = new CassandraAdminTemplate(session, converter);

		performSchemaAction();
	}

	protected void performSchemaAction() {

		boolean dropTables = false;
		boolean dropUnused = false;

		switch (schemaAction) {

			case NONE:
				return;

			case RECREATE_DROP_UNUSED:
				dropUnused = true;
				// don't break!
			case RECREATE:
				dropTables = true;
				// don't break!
			case CREATE:
				createTables(dropTables, dropUnused);
		}
	}

	protected void createTables(boolean dropTables, boolean dropUnused) {

		Metadata md = session.getCluster().getMetadata();
		KeyspaceMetadata kmd = md.getKeyspace(keyspaceName);

		// TODO: fix this with KeyspaceIdentifier
		if (kmd == null) { // try lower-cased keyspace name
			kmd = md.getKeyspace(keyspaceName.toLowerCase());
		}

		if (kmd == null) {
			throw new IllegalStateException(String.format("keyspace [%s] does not exist", keyspaceName));
		}

		for (TableMetadata table : kmd.getTables()) {
			if (dropTables) {
				if (dropUnused || mappingContext.usesTable(table)) {
					admin.dropTable(cqlId(table.getName()));
				}
			}
		}

		Collection<? extends CassandraPersistentEntity<?>> entities = converter.getMappingContext()
				.getNonPrimaryKeyEntities();

		for (CassandraPersistentEntity<?> entity : entities) {
			admin.createTable(false, entity.getTableName(), entity.getType(), null); // TODO: allow spec of table options
		}
	}

	public SchemaAction getSchemaAction() {
		return schemaAction;
	}

	public void setSchemaAction(SchemaAction schemaAction) {
		Assert.notNull(schemaAction);
		this.schemaAction = schemaAction;
	}

	public CassandraConverter getConverter() {
		return converter;
	}

	public void setConverter(CassandraConverter converter) {
		Assert.notNull(converter);
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
	}
}
