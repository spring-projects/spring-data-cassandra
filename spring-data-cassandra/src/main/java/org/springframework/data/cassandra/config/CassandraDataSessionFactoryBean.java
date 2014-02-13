package org.springframework.data.cassandra.config;

import java.util.Collection;

import org.springframework.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

public class CassandraDataSessionFactoryBean extends CassandraSessionFactoryBean {

	protected SchemaAction schemaAction;
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

		if (kmd == null) { // try lower-cased keyspace name
			kmd = md.getKeyspace(keyspaceName.toLowerCase());
		}

		if (kmd == null) {
			throw new IllegalStateException(String.format("keyspace [%s] does not exist", keyspaceName));
		}

		for (TableMetadata table : kmd.getTables()) {
			if (dropTables) {
				if (dropUnused || mappingContext.usesTable(table)) {
					admin.dropTable(table.getName());
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
