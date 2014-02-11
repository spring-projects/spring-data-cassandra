package org.springframework.data.cassandra.config;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.util.Assert;

public class CassandraDataMappingConverterFactoryBean implements FactoryBean<CassandraConverter>, InitializingBean {

	protected CassandraMappingContext mappingContext;
	protected Set<String> basePackages = new HashSet<String>();

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mappingContext);
		Assert.notNull(basePackages);
	}

	@Override
	public CassandraConverter getObject() throws Exception {
		return new MappingCassandraConverter(mappingContext);
	}

	@Override
	public Class<?> getObjectType() {
		return CassandraConverter.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	public CassandraMappingContext getMappingContext() {
		return mappingContext;
	}

	public void setMappingContext(CassandraMappingContext mappingContext) {

		Assert.notNull(mappingContext);

		this.mappingContext = mappingContext;
	}
}
