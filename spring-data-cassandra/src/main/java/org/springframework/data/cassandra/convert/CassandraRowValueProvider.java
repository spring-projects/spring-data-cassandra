package org.springframework.data.cassandra.convert;

import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.model.PropertyValueProvider;

import com.datastax.driver.core.Row;

public interface CassandraRowValueProvider extends PropertyValueProvider<CassandraPersistentProperty> {

	Row getRow();
}
