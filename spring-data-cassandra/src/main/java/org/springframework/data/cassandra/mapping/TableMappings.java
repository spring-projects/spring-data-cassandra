package org.springframework.data.cassandra.mapping;

public interface TableMappings {

	TableMapping getTableMappingByClass(Class<?> entityClass);

	TableMapping getTableMappingByTableName(String tableName);
}
