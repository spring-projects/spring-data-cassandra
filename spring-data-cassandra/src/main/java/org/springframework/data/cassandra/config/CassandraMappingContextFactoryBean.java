package org.springframework.data.cassandra.config;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.mapping.Mapping;
import org.springframework.util.Assert;

public class CassandraMappingContextFactoryBean implements FactoryBean<CassandraMappingContext>, InitializingBean {

	protected Set<String> basePackages = new HashSet<String>();
	protected Mapping mapping;
	protected ClassLoader entityClassLoader;

	@Override
	public void afterPropertiesSet() throws Exception {
		mapping = mapping == null ? new Mapping() : mapping;
	}

	@Override
	public CassandraMappingContext getObject() throws Exception {

		DefaultCassandraMappingContext mappingContext = new DefaultCassandraMappingContext();
		mappingContext.setInitialEntitySet(new CassandraEntityClassScanner(basePackages).scanForEntityClasses());
		mappingContext.setMapping(mapping);
		mappingContext.setBeanClassLoader(entityClassLoader);

		mappingContext.initialize(); // this is necessary here

		return mappingContext;
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
		this.basePackages = basePackages == null ? new HashSet<String>() : new HashSet<String>(basePackages);
	}

	public Mapping getMapping() {
		return mapping;
	}

	public void setMapping(Mapping mapping) {

		Assert.notNull(mapping);

		this.mapping = mapping;
	}

	public ClassLoader getEntityClassLoader() {
		return entityClassLoader;
	}

	public void setEntityClassLoader(ClassLoader entityClassLoader) {
		this.entityClassLoader = entityClassLoader;
	}
}
