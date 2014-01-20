package org.springframework.data.cassandra.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TableMappingsImpl implements TableMappings {

	private Map<Class<?>, TableMapping> mappingsByClass = new HashMap<Class<?>, TableMapping>();
	private Map<String, TableMapping> mappingsByTable = new HashMap<String, TableMapping>();

	public TableMappingsImpl(Set<TableMapping> mappings) {
		setMappings(mappings);
	}

	public void setMappings(Set<TableMapping> mappings) {

		mappingsByClass.clear();
		mappingsByTable.clear();

		if (mappings == null || mappings.size() == 0) {
			return;
		}

		for (TableMapping mapping : mappings) {

			if (mapping == null) {
				continue;
			}

			mappingsByTable.put(mapping.getTableName(), mapping);

			for (Class<?> entityClass : mapping.getEntityClasses()) {
				mappingsByClass.put(entityClass, mapping);
			}
		}
	}

	@Override
	public TableMapping getTableMappingByClass(Class<?> entityClass) {
		return mappingsByClass.get(entityClass);
	}

	@Override
	public TableMapping getTableMappingByTableName(String tableName) {
		return mappingsByTable.get(tableName);
	}
}
