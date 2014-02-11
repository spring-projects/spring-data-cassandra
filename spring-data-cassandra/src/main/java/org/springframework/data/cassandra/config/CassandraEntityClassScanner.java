package org.springframework.data.cassandra.config;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Scans packages for Cassandra entities.
 * 
 * @author Matthew T. Adams
 */
public class CassandraEntityClassScanner {

	protected Set<String> entityBasePackages = new HashSet<String>();
	protected Set<Class<?>> entityBasePackageClasses = new HashSet<Class<?>>();
	protected ClassLoader beanClassLoader;

	public CassandraEntityClassScanner() {
	}

	public CassandraEntityClassScanner(Collection<String> entityBasePackages) {
		this(entityBasePackages, null);
	}

	public CassandraEntityClassScanner(Collection<String> entityBasePackages,
			Collection<Class<?>> entityBasePackageClasses) {

		setEntityBasePackages(entityBasePackages);
		setEntityBasePackageClasses(entityBasePackageClasses);
	}

	public Set<String> getEntityBasePackages() {
		return Collections.unmodifiableSet(entityBasePackages);
	}

	public void setEntityBasePackages(Collection<String> entityBasePackages) {
		this.entityBasePackages = entityBasePackages == null ? new HashSet<String>() : new HashSet<String>(
				entityBasePackages);
	}

	public Set<Class<?>> getEntityBasePackageClasses() {
		return Collections.unmodifiableSet(entityBasePackageClasses);
	}

	public void setEntityBasePackageClasses(Collection<Class<?>> entityBasePackageClasses) {
		this.entityBasePackageClasses = entityBasePackageClasses == null ? new HashSet<Class<?>>() : new HashSet<Class<?>>(
				entityBasePackageClasses);
	}

	public void setBeanClassLoader(ClassLoader beanClassLoader) {
		this.beanClassLoader = beanClassLoader;
	}

	/**
	 * Scans the mapping base package for entity classes annotated with {@link Table} or {@link Persistent}.
	 * 
	 * @see #getEntityBasePackages()
	 * @return <code>Set&lt;Class&lt;?&gt;&gt;</code> representing the annotated entity classes found.
	 * @throws ClassNotFoundException
	 */
	public Set<Class<?>> scanForEntityClasses() throws ClassNotFoundException {

		Set<Class<?>> classes = new HashSet<Class<?>>();

		for (String basePackage : getEntityBasePackages()) {
			classes.addAll(scanBasePackageForEntities(basePackage));
		}

		for (Class<?> basePackageClass : getEntityBasePackageClasses()) {
			classes.addAll(scanBasePackageForEntities(basePackageClass.getPackage().getName()));
		}

		return classes;
	}

	protected Set<Class<?>> scanBasePackageForEntities(String basePackage) throws ClassNotFoundException {

		HashSet<Class<?>> classes = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
					false);
			for (Class<? extends Annotation> annoClass : getEntityAnnotations()) {
				componentProvider.addIncludeFilter(new AnnotationTypeFilter(annoClass));
			}

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				classes.add(ClassUtils.forName(candidate.getBeanClassName(), beanClassLoader));
			}
		}

		return classes;
	}

	@SuppressWarnings("unchecked")
	public Class<? extends Annotation>[] getEntityAnnotations() {
		return new Class[] { Table.class, Persistent.class, PrimaryKeyClass.class };
	}
}
