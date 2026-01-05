/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.util.TypeScanner;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * Scans packages for Cassandra entities. The entity scanner scans for entity classes annotated with
 * {@link #getEntityAnnotations() entity annotations} on the class path using either base package names, base package
 * classes or both.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see ClassUtils#forName(String, ClassLoader)
 */
public class CassandraEntityClassScanner {

	private Set<String> entityBasePackages = new HashSet<>();

	private Set<Class<?>> entityBasePackageClasses = new HashSet<>();

	private @Nullable ClassLoader beanClassLoader;

	/**
	 * Scan one or more base packages for entity classes. Classes are loaded using the current class loader.
	 *
	 * @param entityBasePackages must not be {@literal null}.
	 * @return {@link Set} containing all discovered entity classes.
	 * @throws ClassNotFoundException
	 */
	public static Set<Class<?>> scan(String... entityBasePackages) throws ClassNotFoundException {
		return new CassandraEntityClassScanner(entityBasePackages).scanForEntityClasses();
	}

	/**
	 * Scan one or more base packages for entity classes. Classes are loaded using the current class loader.
	 *
	 * @param entityBasePackageClasses must not be {@literal null}.
	 * @return {@link Set} containing all discovered entity classes.
	 * @throws ClassNotFoundException if a discovered class could not be loaded via.
	 */
	public static Set<Class<?>> scan(Class<?>... entityBasePackageClasses) throws ClassNotFoundException {
		return new CassandraEntityClassScanner(entityBasePackageClasses).scanForEntityClasses();
	}

	/**
	 * Scan one or more base packages for entity classes. Classes are loaded using the current class loader.
	 *
	 * @param entityBasePackages must not be {@literal null}.
	 * @return {@link Set} containing all discovered entity classes.
	 * @throws ClassNotFoundException if a discovered class could not be loaded via.
	 */
	public static Set<Class<?>> scan(Collection<String> entityBasePackages) throws ClassNotFoundException {
		return new CassandraEntityClassScanner(entityBasePackages).scanForEntityClasses();
	}

	/**
	 * Scan one or more base packages for entity classes. Classes are loaded using the current class loader.
	 *
	 * @param entityBasePackages must not be {@literal null}.
	 * @param entityBasePackageClasses must not be {@literal null}.
	 * @return {@link Set} containing all discovered entity classes.
	 * @throws ClassNotFoundException if a discovered class could not be loaded via.
	 */
	public static Set<Class<?>> scan(Collection<String> entityBasePackages, Collection<Class<?>> entityBasePackageClasses)
			throws ClassNotFoundException {
		return new CassandraEntityClassScanner(entityBasePackages, entityBasePackageClasses).scanForEntityClasses();
	}

	/**
	 * Creates a new {@link CassandraEntityClassScanner}.
	 */
	public CassandraEntityClassScanner() {}

	/**
	 * Creates a new {@link CassandraEntityClassScanner} given {@code entityBasePackageClasses}.
	 *
	 * @param entityBasePackageClasses must not be {@literal null}.
	 */
	public CassandraEntityClassScanner(Class<?>... entityBasePackageClasses) {
		setEntityBasePackageClasses(Arrays.asList(entityBasePackageClasses));
	}

	/**
	 * Creates a new {@link CassandraEntityClassScanner} given {@code entityBasePackages}.
	 *
	 * @param entityBasePackages must not be {@literal null}.
	 */
	public CassandraEntityClassScanner(String... entityBasePackages) {
		this(Arrays.asList(entityBasePackages));
	}

	/**
	 * Creates a new {@link CassandraEntityClassScanner} given {@code entityBasePackages}.
	 *
	 * @param entityBasePackages must not be {@literal null}.
	 */
	public CassandraEntityClassScanner(Collection<String> entityBasePackages) {
		setEntityBasePackages(entityBasePackages);
	}

	/**
	 * Creates a new {@link CassandraEntityClassScanner} given {@code entityBasePackages} and
	 * {@code entityBasePackageClasses}.
	 *
	 * @param entityBasePackages must not be {@literal null}.
	 * @param entityBasePackageClasses must not be {@literal null}.
	 */
	public CassandraEntityClassScanner(Collection<String> entityBasePackages,
			Collection<Class<?>> entityBasePackageClasses) {

		setEntityBasePackages(entityBasePackages);
		setEntityBasePackageClasses(entityBasePackageClasses);
	}

	/**
	 * @return base package names used for the entity scan.
	 */
	public Set<String> getEntityBasePackages() {

		if (ObjectUtils.isEmpty(entityBasePackageClasses)) {
			return Collections.unmodifiableSet(entityBasePackages);
		}

		return entityBasePackageClasses.stream().map(ClassUtils::getPackageName).collect(Collectors.toSet());
	}

	/**
	 * Set the base package names to be used for the entity scan.
	 *
	 * @param entityBasePackages must not be {@literal null}.
	 */
	public void setEntityBasePackages(Collection<String> entityBasePackages) {
		this.entityBasePackages = new HashSet<>(entityBasePackages);
	}

	/**
	 * @return base package classes used for the entity scan.
	 */
	public Set<Class<?>> getEntityBasePackageClasses() {
		return Collections.unmodifiableSet(entityBasePackageClasses);
	}

	/**
	 * Set the base package classes to be used for the entity scan.
	 *
	 * @param entityBasePackageClasses must not be {@literal null}.
	 */
	public void setEntityBasePackageClasses(Collection<Class<?>> entityBasePackageClasses) {
		this.entityBasePackageClasses = new HashSet<>(entityBasePackageClasses);
	}

	/**
	 * Set the bean {@link ClassLoader} to load class candidates discovered by the class path scan.
	 *
	 * @param beanClassLoader
	 */
	public void setBeanClassLoader(@Nullable ClassLoader beanClassLoader) {
		this.beanClassLoader = beanClassLoader;
	}

	/**
	 * Scans the mapping base package for entity classes annotated with {@link Table} or {@link PrimaryKeyClass}.
	 *
	 * @see #getEntityBasePackages()
	 * @see #getEntityAnnotations()
	 * @return {@code Set} representing the annotated entity classes found.
	 */
	public Set<Class<?>> scanForEntityClasses() {

		TypeScanner scanner;

		if (this.beanClassLoader != null) {
			scanner = TypeScanner.typeScanner(this.beanClassLoader);
		} else {
			ClassLoader cl = ClassUtils.getDefaultClassLoader();
			scanner = TypeScanner.typeScanner(cl != null ? cl : getClass().getClassLoader());
		}

		return scanner.forTypesAnnotatedWith(getEntityAnnotations()).scanPackages(getEntityBasePackages()).collectAsSet();
	}

	/**
	 * @return entity annotations.
	 * @see Table
	 * @see PrimaryKeyClass
	 */
	@SuppressWarnings("unchecked")
	protected Class<? extends Annotation>[] getEntityAnnotations() {
		return new Class[] { Table.class, PrimaryKeyClass.class };
	}

}
