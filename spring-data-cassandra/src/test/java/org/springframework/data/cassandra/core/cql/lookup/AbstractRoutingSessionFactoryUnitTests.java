/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.lookup;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.data.cassandra.core.cql.session.lookup.AbstractRoutingSessionFactory;
import org.springframework.data.cassandra.core.cql.session.lookup.MapSessionFactoryLookup;
import org.springframework.data.cassandra.core.cql.session.lookup.SessionFactoryLookupFailureException;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Unit tests for {@link AbstractRoutingSessionFactory}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class AbstractRoutingSessionFactoryUnitTests {

	@Mock CqlSession defaultSession;
	@Mock CqlSession routedSession;

	private StubbedRoutingSessionFactory sut;

	@BeforeEach
	void before() throws Exception {

		sut = new StubbedRoutingSessionFactory();
		sut.setDefaultTargetSessionFactory(new DefaultSessionFactory(defaultSession));
	}

	@Test // DATACASS-330
	void shouldDetermineRoutedRepository() {

		sut.setTargetSessionFactories(Collections.singletonMap("key", new DefaultSessionFactory(routedSession)));
		sut.afterPropertiesSet();
		sut.setLookupKey("key");

		assertThat(sut.getSession()).isSameAs(routedSession);
	}

	@Test // DATACASS-330
	void shouldFallbackToDefaultSession() {

		sut.setTargetSessionFactories(Collections.singletonMap("key", new DefaultSessionFactory(routedSession)));
		sut.afterPropertiesSet();
		sut.setLookupKey("unknown");

		assertThat(sut.getSession()).isSameAs(defaultSession);
	}

	@Test // DATACASS-330
	void initializationShouldFailUnsupportedLookupKey() {

		sut.setTargetSessionFactories(Collections.singletonMap("key", new Object()));

		try {
			sut.afterPropertiesSet();
			fail("Missing IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertThat(e).hasMessageContaining("Illegal session factory value.");
		}
	}

	@Test // DATACASS-330
	void initializationShouldFailUnresolvableKey() {

		sut.setTargetSessionFactories(Collections.singletonMap("key", "value"));
		sut.setSessionFactoryLookup(new MapSessionFactoryLookup());

		try {
			sut.afterPropertiesSet();
			fail("Missing SessionFactoryLookupFailureException");
		} catch (SessionFactoryLookupFailureException e) {
			assertThat(e).hasMessageContaining("No SessionFactory with name [value] registered");
		}
	}

	@Test // DATACASS-330
	void unresolvableSessionRetrievalShouldFail() {

		sut.setLenientFallback(false);
		sut.setTargetSessionFactories(Collections.singletonMap("key", new DefaultSessionFactory(routedSession)));
		sut.afterPropertiesSet();
		sut.setLookupKey("unknown");

		assertThatIllegalStateException().isThrownBy(() -> sut.getSession());
	}

	@Test // DATACASS-330
	void sessionRetrievalWithoutLookupKeyShouldReturnDefaultSession() {

		sut.setTargetSessionFactories(Collections.singletonMap("key", new DefaultSessionFactory(routedSession)));
		sut.afterPropertiesSet();
		sut.setLookupKey(null);

		assertThat(sut.getSession()).isSameAs(defaultSession);
	}

	@Test // DATACASS-330
	void shouldLookupFromMap() {

		MapSessionFactoryLookup lookup = new MapSessionFactoryLookup("lookup-key",
				new DefaultSessionFactory(routedSession));

		sut.setSessionFactoryLookup(lookup);
		sut.setTargetSessionFactories(Collections.singletonMap("my-key", "lookup-key"));
		sut.afterPropertiesSet();

		sut.setLookupKey("my-key");

		assertThat(sut.getSession()).isSameAs(routedSession);
	}

	@Test // DATACASS-330
	void shouldAllowModificationsAfterInitialization() {

		MapSessionFactoryLookup lookup = new MapSessionFactoryLookup();

		sut.setSessionFactoryLookup(lookup);
		sut.setTargetSessionFactories((Map) lookup.getSessionFactories());
		sut.afterPropertiesSet();
		sut.setLookupKey("lookup-key");

		assertThat(sut.getSession()).isSameAs(defaultSession);

		lookup.addSessionFactory("lookup-key", new DefaultSessionFactory(routedSession));
		sut.afterPropertiesSet();

		assertThat(sut.getSession()).isSameAs(routedSession);
	}

	static class StubbedRoutingSessionFactory extends AbstractRoutingSessionFactory {

		private String lookupKey;

		private void setLookupKey(String lookupKey) {
			this.lookupKey = lookupKey;
		}

		@Override
		protected Object determineCurrentLookupKey() {
			return lookupKey;
		}
	}
}
