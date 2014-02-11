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

	protected Set<String> entityBasePackages = new HashSet<String>();
	protected Mapping mapping;
	protected ClassLoader beanClassLoader;

	@Override
	public void afterPropertiesSet() throws Exception {
		mapping = mapping == null ? new Mapping() : mapping;
	}

	@Override
	public CassandraMappingContext getObject() throws Exception {

		DefaultCassandraMappingContext mappingContext = new DefaultCassandraMappingContext();
		mappingContext.setInitialEntitySet(new CassandraEntityClassScanner(entityBasePackages).scanForEntityClasses());
		mappingContext.setMapping(mapping);
		mappingContext.setBeanClassLoader(beanClassLoader);

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

	public Set<String> getEntityBasePackages() {
		return entityBasePackages;
	}

	public void setEntityBasePackages(Set<String> basePackages) {
		this.entityBasePackages = basePackages == null ? new HashSet<String>() : new HashSet<String>(basePackages);
	}

	public Mapping getMapping() {
		return mapping;
	}

	public void setMapping(Mapping mapping) {

		Assert.notNull(mapping);

		this.mapping = mapping;
	}

	public ClassLoader getBeanClassLoader() {
		return beanClassLoader;
	}

	public void setBeanClassLoader(ClassLoader entityClassLoader) {
		this.beanClassLoader = entityClassLoader;
	}
}
