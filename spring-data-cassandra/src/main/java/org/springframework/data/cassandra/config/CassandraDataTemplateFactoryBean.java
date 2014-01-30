package org.springframework.data.cassandra.config;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

public class CassandraDataTemplateFactoryBean implements FactoryBean<CassandraOperations>, InitializingBean {

	protected Session session;
	protected CassandraConverter converter;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(session);
		Assert.notNull(converter);
	}

	@Override
	public CassandraOperations getObject() throws Exception {
		return new CassandraTemplate(session, converter);
	}

	@Override
	public Class<?> getObjectType() {
		return CassandraOperations.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	public void setSession(Session session) {
		Assert.notNull(session);
		this.session = session;
	}

	public void setConverter(CassandraConverter converter) {
		Assert.notNull(converter);
		this.converter = converter;
	}
}
