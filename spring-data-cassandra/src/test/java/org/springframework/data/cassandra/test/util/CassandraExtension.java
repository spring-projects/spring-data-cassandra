/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.util;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.commons.util.StringUtils;

import org.springframework.data.cassandra.support.RandomKeyspaceName;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * JUnit {@link org.junit.jupiter.api.extension.Extension} providing embedded Cassandra connectivity. Supports keyspace
 * generation through {@link TestKeyspaceName} and instance field injection of CQL Session and String (keyspace name)
 * for fields annotated with {@link TestKeyspace}.
 * <p>
 * Typical use:
 *
 * <pre class="code">
 * &#64;ExtendWith(CassandraExtension.class)
 * &#64;TestKeyspaceName
 * class MyIntegrationTests {
 *
 * 	&#64;TestKeyspace CqlSession session;
 *
 * 	&#64;TestKeyspace String keyspace;
 *
 * }
 * </pre>
 *
 * @author Mark Paluch
 */
public class CassandraExtension implements BeforeAllCallback, AfterAllCallback, TestInstancePostProcessor {

	private static final ExtensionContext.Namespace CASSANDRA = ExtensionContext.Namespace.create("cassandra");

	private static final ThreadLocal<Resources> TEST_RESOURCES = new ThreadLocal<>();

	private final CassandraDelegate delegate = new CassandraDelegate("embedded-cassandra.yaml");

	/**
	 * Retrieve the system {@link CqlSession} that is associated with the current {@link Thread}.
	 *
	 * @return the system {@link CqlSession}.
	 * @throws IllegalStateException if no system session was associated with the current {@link Thread}.
	 */
	public static CqlSession currentSystemSession() {
		return getResources().systemSession;
	}

	/**
	 * Retrieve the keyspace {@link CqlSession} that is associated with the current {@link Thread}.
	 *
	 * @return the keyspace {@link CqlSession}.
	 * @throws IllegalStateException if no keyspace session was associated with the current {@link Thread}.
	 */
	public static CqlSession currentCqlSession() {

		CqlSession cqlSession = getResources().cqlSession;

		if (cqlSession == null) {
			throw new IllegalStateException(
					"No keyspace-bound session. Make sure to annotate your test class with @TestKeyspaceName");
		}

		return cqlSession;
	}

	public static Resources getResources() {
		Resources resources = TEST_RESOURCES.get();

		if (resources == null) {
			throw new IllegalStateException(
					"No test in progress. Did you annotate your test class with @ExtendWith(CassandraExtension.class)?");
		}
		return resources;
	}

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {

		Optional<String> keyspaceName = getKeyspaceName(context.getTestClass());
		ExtensionContext.Store store = context.getStore(CASSANDRA);

		delegate.before();

		CqlSession session = delegate.createSession();

		TEST_RESOURCES.set(new Resources(delegate.getSystemSession(), session, delegate.getPort()));

		if (keyspaceName.isPresent()) {

			keyspaceName.ifPresent(name -> {

				store.put("KEYSPACE", name);
				TestKeyspaceDelegate.before(session, name);
				store.put(CqlSession.class, session);
			});
		}
	}

	@Override
	public void afterAll(ExtensionContext context) {

		TEST_RESOURCES.remove();

		ExtensionContext.Store store = context.getStore(CASSANDRA);
		String keyspaceName = store.getOrDefault("KEYSPACE", String.class, null);

		if (keyspaceName != null) {

			CqlSession cqlSession = store.get(CqlSession.class, CqlSession.class);

			TestKeyspaceDelegate.after(cqlSession, keyspaceName);
			store.remove(CqlSession.class);
			store.remove("KEYSPACE");

			cqlSession.closeAsync();
		}

		delegate.after();
	}

	@Override
	public void postProcessTestInstance(Object testInstance, ExtensionContext context) throws Exception {

		ExtensionContext.Store store = context.getStore(CASSANDRA);

		List<Field> fields = ReflectionUtils.findFields(testInstance.getClass(),
				field -> AnnotationUtils.isAnnotated(field, TestKeyspace.class),
				ReflectionUtils.HierarchyTraversalMode.TOP_DOWN);

		if (!fields.isEmpty()) {
			String keyspaceName = store.get("KEYSPACE", String.class);
			CqlSession cqlSession = store.get(CqlSession.class, CqlSession.class);

			Assert.state(!cqlSession.isClosed(), "CQL Session closed");

			for (Field field : fields) {
				if (ReflectionUtils.isAssignableTo(keyspaceName, field.getType())) {
					FieldUtils.writeField(field, testInstance, keyspaceName, true);
				}

				if (ReflectionUtils.isAssignableTo(cqlSession, field.getType())) {
					FieldUtils.writeField(field, testInstance, cqlSession, true);
				}
			}
		}
	}

	private Optional<String> getKeyspaceName(Optional<Class<?>> instance) {

		return instance.map(it -> {
			Optional<TestKeyspaceName> annotation = AnnotationUtils.findAnnotation(it, TestKeyspaceName.class);

			if (annotation.isPresent()) {
				return annotation.map(TestKeyspaceName::value).filter(StringUtils::isNotBlank)
						.orElseGet(RandomKeyspaceName::create);
			}

			return null;
		});
	}

	public static class Resources {

		private final CqlSession systemSession;
		private final CqlSession cqlSession;
		private final int port;

		private Resources(CqlSession systemSession, CqlSession cqlSession, int port) {
			this.systemSession = systemSession;
			this.cqlSession = cqlSession;
			this.port = port;
		}

		public int getPort() {
			return port;
		}
	}
}
