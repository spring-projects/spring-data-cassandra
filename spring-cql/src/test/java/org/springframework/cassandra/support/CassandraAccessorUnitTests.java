/*
 *  Copyright 2016 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.springframework.cassandra.support;

import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Session;

/**
 * The CassandraAccessorUnitTests class is a test suite of test cases testing the contract and functionality of the
 * {@link CassandraAccessor} class.
 *
 * @author John Blum
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraAccessorUnitTests {

	private CassandraAccessor cassandraAccessor;

	@Mock private CassandraExceptionTranslator mockExceptionTranslator;

	@Rule public ExpectedException exception = ExpectedException.none();

	@Mock private Session mockSession;

	@Before
	public void setup() {
		cassandraAccessor = new CassandraAccessor();
	}

	/**
	 * @see DATACASS-286
	 */
	@Test
	public void afterPropertiesSetWithUnitializedSessionThrowsIllegalStateException() {

		try {
			cassandraAccessor.afterPropertiesSet();
			fail("Missing IllegalStateException");
		} catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("Session must not be null");
		}
	}

	/**
	 * @see DATACASS-286
	 */
	@Test
	public void setAndGetExceptionTranslator() {

		cassandraAccessor.setExceptionTranslator(mockExceptionTranslator);
		assertThat(cassandraAccessor.getExceptionTranslator()).isSameAs(mockExceptionTranslator);
	}

	/**
	 * @see DATACASS-286
	 */
	@Test
	public void setExceptionTranslatorToNullThrowsIllegalArgumentException() {

		try {
			cassandraAccessor.setExceptionTranslator(null);
			fail("Missing IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertThat(e).hasMessageContaining("CassandraExceptionTranslator must not be null");
		}
	}

	/**
	 * @see DATACASS-286
	 */
	@Test
	public void getUninitializedExceptionTranslatorReturnsDefault() {
		assertThat(cassandraAccessor.getExceptionTranslator()).isEqualTo(cassandraAccessor.exceptionTranslator);
	}

	/**
	 * @see DATACASS-286
	 */
	@Test
	public void setAndGetSession() {

		cassandraAccessor.setSession(mockSession);
		assertThat(cassandraAccessor.getSession()).isSameAs(mockSession);
	}

	/**
	 * @see DATACASS-286
	 */
	@Test
	public void setSessionToNullThrowsIllegalArgumentException() {

		try {
			cassandraAccessor.setSession(null);
			fail("Missing IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertThat(e).hasMessageContaining("Session must not be null");
		}
	}

	/**
	 * @see DATACASS-286
	 */
	@Test
	public void getUninitializedSessionThrowsIllegalStateException() {

		try {
			cassandraAccessor.getSession();
			fail("Missing IllegalStateException");
		} catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("Session was not properly initialized");
		}
	}
}
