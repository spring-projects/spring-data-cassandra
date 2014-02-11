package org.springframework.data.cassandra.config;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;

public class CassandraDataCassandraMappingContextFactoryBean implements FactoryBean<CassandraMappingContext>,
		InitializingBean {

	protected Set<String> basePackages = new HashSet<String>();

	@Override
	public void afterPropertiesSet() throws Exception {
	}

	@Override
	public CassandraMappingContext getObject() throws Exception {

		DefaultCassandraMappingContext cmc = new DefaultCassandraMappingContext();
		cmc.setInitialEntitySet(new CassandraEntityClassScanner(basePackages).scanForEntityClasses());

		return cmc;
	}

	@Override
	public Class<?> getObjectType() {
		return CassandraMappingContext.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	public Set<String> getBasePackages() {
		return basePackages;
	}

	public void setBasePackages(Set<String> basePackages) {
		this.basePackages = basePackages;
	}
}
