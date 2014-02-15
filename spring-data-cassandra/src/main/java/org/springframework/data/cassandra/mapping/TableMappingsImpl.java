/*******************************************************************************
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
