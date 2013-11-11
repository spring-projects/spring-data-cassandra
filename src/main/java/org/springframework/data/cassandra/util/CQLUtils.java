package org.springframework.data.cassandra.util;

import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.PropertyHandler;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;


public abstract class CQLUtils {

	public static String createTable(String tableName, final CassandraPersistentEntity<?> entity) {

		final StringBuilder str = new StringBuilder();
		str.append("CREATE TABLE ");
		str.append(tableName);
		str.append('(');

		final List<String> ids = new ArrayList<String>();
		final List<String> idColumns = new ArrayList<String>();
		
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {
				
				if (str.charAt(str.length()-1) != '(') {
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
		
		if (ids.size() > 1) {
			str.append('(');
		}
		
		for (String id: ids) {
			if (str.charAt(str.length()-1) != '(') {
				str.append(',');
			}
			str.append(id);
		}
		
		if (ids.size() > 1) {
			str.append(')');
		}

		for (String id: idColumns) {
			str.append(',');
			str.append(id);
		}

		str.append("));");
		
		
		return str.toString();
	}
	
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

	public static List<String> alterTable(final String tableName, final CassandraPersistentEntity<?> entity, final TableMetadata table) {
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
				}
				else {
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
		
		
		//System.out.println("CQL=" + table.asCQLQuery());
		
		return result;
	}
	
	public static String toCQL(DataType dataType) {
		if (dataType.getTypeArguments().isEmpty()) {
			return dataType.getName().name();
		}
		else {
			StringBuilder str = new StringBuilder();
			str.append(dataType.getName().name());
			str.append('<');
			for (DataType argDataType : dataType.getTypeArguments()) {
				if (str.charAt(str.length()-1) != '<') {
					str.append(',');
				}
				str.append(argDataType.getName().name());
			}
			str.append('>');
			return str.toString();
		}
	}

	
}
