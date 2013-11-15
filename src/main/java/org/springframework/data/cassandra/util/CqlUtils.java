package org.springframework.data.cassandra.util;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.ConsistencyLevel;
import org.springframework.data.cassandra.core.ConsistencyLevelResolver;
import org.springframework.data.cassandra.core.QueryOptions;
import org.springframework.data.cassandra.core.RetryPolicy;
import org.springframework.data.cassandra.core.RetryPolicyResolver;
import org.springframework.data.cassandra.exception.EntityWriterException;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.PropertyHandler;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * 
 * Utilties to convert Cassandra Annotated objects to Queries and CQL.
 * 
 * @author Alex Shvid
 * @author David Webb
 * 
 */
public abstract class CqlUtils {

	private static Logger log = LoggerFactory.getLogger(CqlUtils.class);

	/**
	 * Generates the CQL String to create a table in Cassandra
	 * 
	 * @param tableName
	 * @param entity
	 * @return The CQL that can be passed to session.execute()
	 */
	public static String createTable(String tableName, final CassandraPersistentEntity<?> entity) {

		final StringBuilder str = new StringBuilder();
		str.append("CREATE TABLE ");
		str.append(tableName);
		str.append('(');

		final List<String> ids = new ArrayList<String>();
		final List<String> idColumns = new ArrayList<String>();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (str.charAt(str.length() - 1) != '(') {
					str.append(',');
				}

				String columnName = prop.getColumnName();

				str.append(columnName);
				str.append(' ');

				DataType dataType = prop.getDataType();

				str.append(toCQL(dataType));

				if (prop.isIdProperty()) {
					ids.add(prop.getColumnName());
				}

				if (prop.isColumnId()) {
					idColumns.add(prop.getColumnName());
				}

			}

		});

		if (ids.isEmpty()) {
			throw new InvalidDataAccessApiUsageException("not found primary ID in the entity " + entity.getType());
		}

		str.append(",PRIMARY KEY(");

		// if (ids.size() > 1) {
		// str.append('(');
		// }

		for (String id : ids) {
			if (str.charAt(str.length() - 1) != '(') {
				str.append(',');
			}
			str.append(id);
		}

		// if (ids.size() > 1) {
		// str.append(')');
		// }

		for (String id : idColumns) {
			str.append(',');
			str.append(id);
		}

		str.append("));");

		return str.toString();
	}

	/**
	 * Create the List of CQL for the indexes required for Cassandra mapped Table.
	 * 
	 * @param tableName
	 * @param entity
	 * @return The list of CQL statements to run with session.execute()
	 */
	public static List<String> createIndexes(final String tableName, final CassandraPersistentEntity<?> entity) {
		final List<String> result = new ArrayList<String>();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isIndexed()) {

					final StringBuilder str = new StringBuilder();
					str.append("CREATE INDEX ON ");
					str.append(tableName);
					str.append(" (");
					str.append(prop.getColumnName());
					str.append(");");

					result.add(str.toString());
				}

			}
		});

		return result;
	}

	/**
	 * Alter the table to refelct the entity annotations
	 * 
	 * @param tableName
	 * @param entity
	 * @param table
	 * @return
	 */
	public static List<String> alterTable(final String tableName, final CassandraPersistentEntity<?> entity,
			final TableMetadata table) {
		final List<String> result = new ArrayList<String>();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				String columnName = prop.getColumnName();
				DataType columnDataType = prop.getDataType();
				ColumnMetadata columnMetadata = table.getColumn(columnName.toLowerCase());

				if (columnMetadata != null && columnDataType.equals(columnMetadata.getType())) {
					return;
				}

				final StringBuilder str = new StringBuilder();
				str.append("ALTER TABLE ");
				str.append(tableName);
				if (columnMetadata == null) {
					str.append(" ADD ");
				} else {
					str.append(" ALTER ");
				}

				str.append(columnName);
				str.append(' ');

				if (columnMetadata != null) {
					str.append("TYPE ");
				}

				str.append(toCQL(columnDataType));

				str.append(';');
				result.add(str.toString());

			}
		});

		return result;
	}

	/**
	 * Generates a Query Object for an insert
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param entity
	 * @param objectToSave
	 * @param optionsByName
	 * @param mappingContext
	 * @param beanClassLoader
	 * 
	 * @return The Query object to run with session.execute();
	 * @throws EntityWriterException
	 */
	public static Query toInsertQuery(String keyspaceName, String tableName, final Object objectToSave,
			CassandraPersistentEntity<?> entity, Map<String, Object> optionsByName) throws EntityWriterException {

		final Insert q = QueryBuilder.insertInto(keyspaceName, tableName);
		final Exception innerException = new Exception();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				/*
				 * See if the object has a value for that column, and if so, add it to the Query
				 */
				try {

					Object o = prop.getGetter().invoke(objectToSave, new Object[0]);

					log.info("Getter Invoke [" + prop.getColumnName() + " => " + o);

					if (o != null) {
						q.value(prop.getColumnName(), o);
					}

				} catch (IllegalAccessException e) {
					innerException.initCause(e);
				} catch (IllegalArgumentException e) {
					innerException.initCause(e);
				} catch (InvocationTargetException e) {
					innerException.initCause(e);
				}
			}
		});

		if (innerException.getCause() != null) {
			throw new EntityWriterException("Failed to convert Persistent Entity to CQL/Query", innerException.getCause());
		}

		/*
		 * Add Query Options
		 */
		addQueryOptions(q, optionsByName);

		/*
		 * Add TTL to Insert object
		 */
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.TTL) != null) {
			q.using(QueryBuilder.ttl((Integer) optionsByName.get(QueryOptions.QueryOptionMapKeys.TTL)));
		}

		return q;

	}

	/**
	 * Generates a Query Object for an Update
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param entity
	 * @param objectToSave
	 * @param optionsByName
	 * @param mappingContext
	 * @param beanClassLoader
	 * 
	 * @return The Query object to run with session.execute();
	 * @throws EntityWriterException
	 */
	public static Query toUpdateQuery(String keyspaceName, String tableName, final Object objectToSave,
			CassandraPersistentEntity<?> entity, Map<String, Object> optionsByName) throws EntityWriterException {

		final Update q = QueryBuilder.update(keyspaceName, tableName);
		final Exception innerException = new Exception();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				/*
				 * See if the object has a value for that column, and if so, add it to the Query
				 */
				try {

					Object o = prop.getGetter().invoke(objectToSave, new Object[0]);

					log.info("Getter Invoke [" + prop.getColumnName() + " => " + o);

					if (o != null) {
						if (prop.isIdProperty()) {
							q.where(QueryBuilder.eq(prop.getColumnName(), o));
						} else {
							q.with(QueryBuilder.set(prop.getColumnName(), o));
						}
					}

				} catch (IllegalAccessException e) {
					innerException.initCause(e);
				} catch (IllegalArgumentException e) {
					innerException.initCause(e);
				} catch (InvocationTargetException e) {
					innerException.initCause(e);
				}
			}
		});

		if (innerException.getCause() != null) {
			throw new EntityWriterException("Failed to convert Persistent Entity to CQL/Query", innerException.getCause());
		}

		/*
		 * Add Query Options
		 */
		addQueryOptions(q, optionsByName);

		/*
		 * Add TTL to Insert object
		 */
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.TTL) != null) {
			q.using(QueryBuilder.ttl((Integer) optionsByName.get(QueryOptions.QueryOptionMapKeys.TTL)));
		}

		return q;

	}

	/**
	 * Generates a Batch Object for multiple Updates
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param entity
	 * @param objectsToSave
	 * @param mappingContext
	 * @param beanClassLoader
	 * 
	 * @return The Query object to run with session.execute();
	 * @throws EntityWriterException
	 */
	public static <T> Batch toUpdateBatchQuery(final String keyspaceName, final String tableName,
			final List<T> objectsToSave, CassandraPersistentEntity<?> entity, Map<String, Object> optionsByName)
			throws EntityWriterException {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch b = QueryBuilder.batch();

		List<Query> queries = new ArrayList<Query>();

		for (final T objectToSave : objectsToSave) {

			queries.add(toUpdateQuery(keyspaceName, tableName, objectToSave, entity, optionsByName));

		}

		addQueryOptions(b, optionsByName);

		return b;

	}

	/**
	 * Generates a Batch Object for multiple inserts
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param entity
	 * @param objectsToSave
	 * @param mappingContext
	 * @param beanClassLoader
	 * 
	 * @return The Query object to run with session.execute();
	 * @throws EntityWriterException
	 */
	public static <T> Batch toInsertBatchQuery(final String keyspaceName, final String tableName,
			final List<T> objectsToSave, CassandraPersistentEntity<?> entity, Map<String, Object> optionsByName)
			throws EntityWriterException {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch b = QueryBuilder.batch();

		List<Query> queries = new ArrayList<Query>();

		for (final T objectToSave : objectsToSave) {

			queries.add(toInsertQuery(keyspaceName, tableName, objectToSave, entity, optionsByName));

		}

		/*
		 * Add all the Queries to the batch
		 */
		for (Query query : queries) {
			b.add((Statement) query);
		}

		addQueryOptions(b, optionsByName);

		return b;

	}

	/**
	 * @param keyspace
	 * @param tableName
	 * @param objectToRemove
	 * @param entity
	 * @return
	 * @throws EntityWriterException
	 */
	public static Query toDeleteQuery(String keyspace, String tableName, final Object objectToRemove,
			CassandraPersistentEntity<?> entity, Map<String, Object> optionsByName) throws EntityWriterException {

		final Delete.Selection ds = QueryBuilder.delete();
		final Delete q = ds.from(keyspace, tableName);
		final Where w = q.where();

		final Exception innerException = new Exception();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				/*
				 * See if the object has a value for that column, and if so, add it to the Query
				 */
				try {

					if (prop.isIdProperty()) {
						Object o = (String) prop.getGetter().invoke(objectToRemove, new Object[0]);

						log.info("Getter Invoke [" + prop.getColumnName() + " => " + o);

						if (o != null) {
							w.and(QueryBuilder.eq(prop.getColumnName(), o));
						}
					}

				} catch (IllegalAccessException e) {
					innerException.initCause(e);
				} catch (IllegalArgumentException e) {
					innerException.initCause(e);
				} catch (InvocationTargetException e) {
					innerException.initCause(e);
				}
			}
		});

		if (innerException.getCause() != null) {
			throw new EntityWriterException("Failed to convert Persistent Entity to CQL/Query", innerException.getCause());
		}

		addQueryOptions(q, optionsByName);

		return q;

	}

	/**
	 * @param dataType
	 * @return
	 */
	public static String toCQL(DataType dataType) {
		if (dataType.getTypeArguments().isEmpty()) {
			return dataType.getName().name();
		} else {
			StringBuilder str = new StringBuilder();
			str.append(dataType.getName().name());
			str.append('<');
			for (DataType argDataType : dataType.getTypeArguments()) {
				if (str.charAt(str.length() - 1) != '<') {
					str.append(',');
				}
				str.append(argDataType.getName().name());
			}
			str.append('>');
			return str.toString();
		}
	}

	/**
	 * @param tableName
	 * @return
	 */
	public static String dropTable(String tableName) {

		if (tableName == null) {
			return null;
		}

		StringBuilder str = new StringBuilder();
		str.append("DROP TABLE " + tableName + ";");
		return str.toString();
	}

	/**
	 * @param keyspace
	 * @param tableName
	 * @param entities
	 * @param cPEntity
	 * @return
	 * @throws EntityWriterException
	 */
	public static <T> Batch toDeleteBatchQuery(String keyspaceName, String tableName, List<T> entities,
			CassandraPersistentEntity<?> entity, Map<String, Object> optionsByName) throws EntityWriterException {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch b = QueryBuilder.batch();

		List<Query> queries = new ArrayList<Query>();

		for (final T objectToSave : entities) {

			queries.add(toDeleteQuery(keyspaceName, tableName, objectToSave, entity, optionsByName));

		}

		addQueryOptions(b, optionsByName);

		return b;

	}

	/**
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */
	private static void addQueryOptions(Query q, Map<String, Object> optionsByName) {

		if (optionsByName == null) {
			return;
		}

		/*
		 * Add Query Options
		 */
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL) != null) {
			q.setConsistencyLevel(ConsistencyLevelResolver.resolve((ConsistencyLevel) optionsByName
					.get(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL)));
		}
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.RETRY_POLICY) != null) {
			q.setRetryPolicy(RetryPolicyResolver.resolve((RetryPolicy) optionsByName
					.get(QueryOptions.QueryOptionMapKeys.RETRY_POLICY)));
		}

	}

}
