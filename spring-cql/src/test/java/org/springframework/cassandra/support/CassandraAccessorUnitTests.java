/*
 *  Copyright 2013-2016 the original author or authors
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

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
 * @see org.springframework.cassandra.support.CassandraAccessor
 * @since 1.5.0
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

	@Test
	public void afterPropertiesSetWithUnitializedSessionThrowsIllegalStateException() {
		exception.expect(IllegalStateException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("Session must not be null");

		cassandraAccessor.afterPropertiesSet();
	}

	@Test
	public void setAndGetExceptionTranslator() {
		cassandraAccessor.setExceptionTranslator(mockExceptionTranslator);
		assertThat(cassandraAccessor.getExceptionTranslator(), is(sameInstance(mockExceptionTranslator)));
	}

	@Test
	public void setExceptionTranslatorToNullThrowsIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage(is(equalTo("CassandraExceptionTranslator must not be null")));

		cassandraAccessor.setExceptionTranslator(null);
	}

	@Test
	public void getUninitializedExceptionTranslatorReturnsDefault() {
		assertThat(cassandraAccessor.getExceptionTranslator(), is(equalTo(cassandraAccessor.exceptionTranslator)));
	}

	@Test
	public void setAndGetSession() {
		cassandraAccessor.setSession(mockSession);
		assertThat(cassandraAccessor.getSession(), is(sameInstance(mockSession)));
	}

	@Test
	public void setSessionToNullThrowsIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage(is(equalTo("Session must not be null")));

		cassandraAccessor.setSession(null);
	}

	@Test
	public void getUninitializedSessionThrowsIllegalStateException() {
		exception.expect(IllegalStateException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage(is(equalTo("Session was not properly initialized")));

		cassandraAccessor.getSession();
	}
}
