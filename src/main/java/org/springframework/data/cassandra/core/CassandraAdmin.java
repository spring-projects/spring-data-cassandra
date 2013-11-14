package org.springframework.data.cassandra.core;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.util.CqlUtils;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 *
 */
public class CassandraAdmin implements CassandraAdminOperations {

	private static Logger log = LoggerFactory.getLogger(CassandraAdmin.class);

	private final Keyspace keyspace;
	private final Session session;
	private final CassandraConverter cassandraConverter;
	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	private ClassLoader beanClassLoader;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param keyspace must not be {@literal null}.
	 */
	public CassandraAdmin(Keyspace keyspace) {
		this.keyspace = keyspace;
		this.session = keyspace.getSession();
		this.cassandraConverter = keyspace.getCassandraConverter();
		this.mappingContext = this.cassandraConverter.getMappingContext();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#createTable(boolean, java.lang.String, java.lang.Class, java.util.Map)
	 */
	@Override
	public void createTable(boolean ifNotExists, final String tableName, Class<?> entityClass,
			Map<String, Object> optionsByName) {

		try {

			final CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

			execute(new SessionCallback<Object>() {

				public Object doInSession(Session s) throws DataAccessException {

					String cql = CqlUtils.createTable(tableName, entity);

					log.info("CREATE TABLE CQL -> " + cql);

					s.execute(cql);

					return null;

				}
			});

		} catch (LinkageError e) {
			e.printStackTrace();
		} finally {
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
	public void replaceTable(String tableName, Class<?> entityClass) {
		// TODO Auto-generated method stub

	}

	/**
	 * Create a list of query operations to alter the table for the given entity
	 * 
	 * @param entityClass
	 * @param tableName
	 */
	protected void doAlterTable(Class<?> entityClass, String tableName) {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		Assert.notNull(entity);

		final TableMetadata tableMetadata = getTableMetadata(entityClass, tableName);

		final List<String> queryList = CqlUtils.alterTable(tableName, entity, tableMetadata);

		execute(new SessionCallback<Object>() {

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
	@Override
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
	public TableMetadata getTableMetadata(Class<?> entityClass, String tableName) {

		/*
		 * Determine the table name if not provided
		 */
		if (tableName == null) {
			tableName = determineTableName(entityClass);
		}

		Assert.notNull(tableName);

		final String metadataTableName = tableName;

		return execute(new SessionCallback<TableMetadata>() {

			public TableMetadata doInSession(Session s) throws DataAccessException {

				log.info("Keyspace => " + keyspace.getKeyspace());

				return s.getCluster().getMetadata().getKeyspace(keyspace.getKeyspace()).getTable(metadataTableName);

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

		} catch (DataAccessException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	private RuntimeException potentiallyConvertRuntimeException(RuntimeException ex) {
		RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
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
		return entity.getTable();
	}
}
