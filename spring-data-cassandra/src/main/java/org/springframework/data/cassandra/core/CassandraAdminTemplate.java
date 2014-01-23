package org.springframework.data.cassandra.core;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.support.CassandraAccessor;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.cassandra.support.exception.CassandraTableExistsException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.util.CqlUtils;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Default implementation of {@link CassandraAdminOperations}.
 */
public class CassandraAdminTemplate extends CassandraAccessor implements CassandraAdminOperations {

	private static final Logger log = LoggerFactory.getLogger(CassandraAdminTemplate.class);

	private Session session;
	private CassandraConverter converter;
	private CassandraMappingContext mappingContext;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param keyspace must not be {@literal null}.
	 */
	public CassandraAdminTemplate(Session session) {
		setSession(session);
	}

	public void setCassandraConverter(CassandraConverter converter) {
		Assert.notNull(converter);
		this.converter = converter;
		setMappingContext(this.converter.getCassandraMappingContext());
	}

	protected void setMappingContext(CassandraMappingContext mappingContext) {
		Assert.notNull(mappingContext);
		this.mappingContext = mappingContext;
	}

	@Override
	public boolean createTable(boolean ifNotExists, final String tableName, Class<?> entityClass,
			Map<String, Object> optionsByName) {

		try {
			final CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

			execute(new SessionCallback<Object>() {
				@Override
				public Object doInSession(Session s) throws DataAccessException {

					String cql = new CreateTableCqlGenerator(mappingContext.getCreateTableSpecificationFor(entity)).toCql();

					log.info("CREATE TABLE CQL -> " + cql);

					s.execute(cql);
					return null;
				}
			});
			return true;

		} catch (CassandraTableExistsException ctex) {
			return !ifNotExists;
		} catch (RuntimeException x) {
			throw tryToConvert(x);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#alterTable(java.lang.String, java.lang.Class, boolean)
	 */
	@Override
	public void alterTable(String tableName, Class<?> entityClass, boolean dropRemovedAttributeColumns) {
		// TODO Auto-generated method stub
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#replaceTable(java.lang.String, java.lang.Class)
	 */
	@Override
	public void replaceTable(String tableName, Class<?> entityClass, Map<String, Object> optionsByName) {
		// TODO
	}

	/**
	 * Create a list of query operations to alter the table for the given entity
	 * 
	 * @param entityClass
	 * @param tableName
	 */
	protected void doAlterTable(Class<?> entityClass, String keyspace, String tableName) {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

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

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropTable(java.lang.Class)
	 */
	public void dropTable(Class<?> entityClass) {

		final String tableName = determineTableName(entityClass);

		dropTable(tableName);

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropTable(java.lang.String)
	 */
	@Override
	public void dropTable(String tableName) {

		log.info("Dropping table => " + tableName);

		final String q = CqlUtils.dropTable(tableName);
		log.info(q);

		execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {

				return s.execute(q);

			}

		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#getTableMetadata(java.lang.Class)
	 */
	@Override
	public TableMetadata getTableMetadata(final String keyspace, final String tableName) {

		Assert.notNull(tableName);

		return execute(new SessionCallback<TableMetadata>() {

			@Override
			public TableMetadata doInSession(Session s) throws DataAccessException {

				return s.getCluster().getMetadata().getKeyspace(keyspace).getTable(tableName);
			}
		});
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T execute(SessionCallback<T> callback) {

		Assert.notNull(callback);

		try {
			return callback.doInSession(session);
		} catch (RuntimeException x) {
			throw tryToConvert(x);
		}
	}

	protected RuntimeException tryToConvert(RuntimeException x) {
		RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(x);
		return resolved == null ? x : resolved;
	}

	/**
	 * @param entityClass
	 * @return
	 */
	public String determineTableName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ entityClass.getName());
		}
		return entity.getTableName();
	}

}
