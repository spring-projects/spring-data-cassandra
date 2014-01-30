package org.springframework.data.cassandra.config;

import java.util.Collection;

import org.springframework.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

public class CassandraDataSessionFactoryBean extends CassandraSessionFactoryBean {

	protected SchemaAction schemaAction;
	protected CassandraAdminTemplate admin;
	protected CassandraConverter converter;
	protected CassandraMappingContext mappingContext;
	protected Mapping mapping;
	protected ClassLoader entityClassLoader = getClass().getClassLoader();

	@Override
	public void afterPropertiesSet() throws Exception {

		super.afterPropertiesSet();

		Assert.notNull(converter);

		admin = new CassandraAdminTemplate(session, converter);

		mapping = mapping == null ? new Mapping() : mapping;

		processMappingOverrides();
		performSchemaAction();
	}

	protected void processMappingOverrides() throws ClassNotFoundException {

		if (mapping == null) {
			return;
		}

		for (EntityMapping entityMapping : mapping.getEntityMappings()) {

			if (entityMapping == null) {
				continue;
			}

			String entityClassName = entityMapping.getEntityClassName();
			Class<?> entityClass = Class.forName(entityClassName, false, entityClassLoader);

			CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

			if (entity == null) {
				throw new IllegalStateException(String.format("unknown persistent entity class name [%s]", entityClassName));
			}

			String tableName = entityMapping.getTableName();
			if (!StringUtils.hasText(tableName)) {
				continue;
			}

			entity.setTableName(tableName);
		}
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

		Collection<? extends CassandraPersistentEntity<?>> entities = converter.getMappingContext().getPersistentEntities();

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
		this.mappingContext = converter.getCassandraMappingContext();
	}

	public Mapping getMapping() {
		return mapping;
	}

	public void setMapping(Mapping mapping) {
		Assert.notNull(mapping);
		this.mapping = mapping;
	}

	public ClassLoader getEntityClassLoader() {
		return entityClassLoader;
	}

	public void setEntityClassLoader(ClassLoader entityClassLoader) {
		Assert.notNull(entityClassLoader);
		this.entityClassLoader = entityClassLoader;
	}
}
