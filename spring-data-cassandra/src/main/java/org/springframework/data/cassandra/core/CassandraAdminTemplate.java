package org.springframework.data.cassandra.core;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.util.CqlUtils;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Default implementation of {@link CassandraAdminOperations}.
 */
public class CassandraAdminTemplate extends CassandraTemplate implements CassandraAdminOperations {

	private static final Logger log = LoggerFactory.getLogger(CassandraAdminTemplate.class);

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param keyspace must not be {@literal null}.
	 */
	public CassandraAdminTemplate(Session session, CassandraConverter converter) {
		super(session, converter);
	}

	@Override
	public void createTable(boolean ifNotExists, final String tableName, Class<?> entityClass,
			Map<String, Object> optionsByName) {

		final CassandraPersistentEntity<?> entity = getCassandraMappingContext().getPersistentEntity(entityClass);

		execute(new SessionCallback<Object>() {
			@Override
			public Object doInSession(Session s) throws DataAccessException {

				String cql = new CreateTableCqlGenerator(getCassandraMappingContext().getCreateTableSpecificationFor(entity))
						.toCql();

				s.execute(cql);
				return null;
			}
		});
	}

	@Override
	public void alterTable(String tableName, Class<?> entityClass, boolean dropRemovedAttributeColumns) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public void replaceTable(String tableName, Class<?> entityClass, Map<String, Object> optionsByName) {

		dropTable(tableName);
		createTable(false, tableName, entityClass, optionsByName);
	}

	/**
	 * Create a list of query operations to alter the table for the given entity
	 * 
	 * @param entityClass
	 * @param tableName
	 */
	protected void doAlterTable(Class<?> entityClass, String keyspace, String tableName) {

		CassandraPersistentEntity<?> entity = getCassandraMappingContext().getPersistentEntity(entityClass);

		Assert.notNull(entity);

		final TableMetadata tableMetadata = getTableMetadata(keyspace, tableName);
		final List<String> queryList = CqlUtils.alterTable(tableName, entity, tableMetadata);

		execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

				for (String q : queryList) {
					log.info(q);
					s.execute(q);
				}

				return null;
			}
		});
	}

	public void dropTable(Class<?> entityClass) {
		dropTable(determineTableName(entityClass));
	}

	@Override
	public void dropTable(String tableName) {

		log.info("Dropping table => " + tableName);

		execute(DropTableSpecification.dropTable(tableName));
	}

	@Override
	public TableMetadata getTableMetadata(final String keyspace, final String tableName) {

		Assert.notNull(tableName);

		return execute(new SessionCallback<TableMetadata>() {
			@Override
			public TableMetadata doInSession(Session s) {
				return s.getCluster().getMetadata().getKeyspace(keyspace).getTable(tableName);
			}
		});
	}

	/**
	 * @param entityClass
	 * @return
	 */
	@Override
	public String determineTableName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = getCassandraMappingContext().getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ entityClass.getName());
		}
		return entity.getTableName();
	}
}
