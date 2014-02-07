package org.springframework.data.cassandra.util;

import java.util.ArrayList;
import java.util.List;

import org.springframework.cassandra.core.cql.CqlStringUtils;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.PropertyHandler;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

/**
 * Utilities to convert Cassandra Annotated objects to Queries and CQL.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew T. Adams
 */
public abstract class CqlUtils {

	/**
	 * Alter the table to reflect the entity annotations
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
			@Override
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

				str.append(CqlStringUtils.toCql(columnDataType));

				str.append(';');
				result.add(str.toString());

			}
		});

		return result;
	}
}
