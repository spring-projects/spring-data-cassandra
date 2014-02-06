package org.springframework.data.cassandra.convert;

import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;

public class CassandraPersistentEntityParameterValueProvider extends
		PersistentEntityParameterValueProvider<CassandraPersistentProperty> {

	public CassandraPersistentEntityParameterValueProvider(PersistentEntity<?, CassandraPersistentProperty> entity,
			PropertyValueProvider<CassandraPersistentProperty> provider, Object parent) {
		super(entity, provider, parent);
	}
}
