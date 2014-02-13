package org.springframework.data.cassandra.mapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.util.TypeInformation;

public class CachingCassandraPersistentEntity<T> extends BasicCassandraPersistentEntity<T> {

	protected String tableName;
	protected String name;
	protected Boolean isCompositePrimaryKey;
	protected List<CassandraPersistentProperty> compositePrimaryKeyProperties;
	protected Map<String, CassandraPersistentProperty> properties = new HashMap<String, CassandraPersistentProperty>();

	public CachingCassandraPersistentEntity(TypeInformation<T> typeInformation) {
		super(typeInformation);
	}

	public CachingCassandraPersistentEntity(TypeInformation<T> typeInformation, CassandraMappingContext mappingContext) {
		super(typeInformation, mappingContext);
	}

	public CachingCassandraPersistentEntity(TypeInformation<T> typeInformation, CassandraMappingContext mappingContext,
			CassandraPersistentEntityMetadataVerifier verifier) {
		super(typeInformation, mappingContext, verifier);
	}

	@Override
	public String getTableName() {
		if (tableName == null) {
			tableName = super.getTableName();
		}
		return tableName;
	}

	@Override
	public String getName() {
		if (name == null) {
			name = super.getName();
		}
		return name;
	}

	@Override
	public boolean isCompositePrimaryKey() {
		if (isCompositePrimaryKey == null) {
			isCompositePrimaryKey = super.isCompositePrimaryKey();
		}
		return isCompositePrimaryKey;
	}

	@Override
	public List<CassandraPersistentProperty> getCompositePrimaryKeyProperties() {
		if (compositePrimaryKeyProperties == null) {
			compositePrimaryKeyProperties = super.getCompositePrimaryKeyProperties();
		}
		return compositePrimaryKeyProperties;
	}

	@Override
	public CassandraPersistentProperty getPersistentProperty(String name) {
		if (properties.get(name) == null) {
			properties.put(name, super.getPersistentProperty(name));
		}
		return properties.get(name);
	}
}
