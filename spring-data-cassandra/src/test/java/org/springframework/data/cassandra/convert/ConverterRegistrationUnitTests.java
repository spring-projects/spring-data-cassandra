/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.cassandra.convert;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.cassandra.domain.Person;

/**
 * Unit tests for {@link ConverterRegistration}.
 *
 * @author Mark Paluch
 */
public class ConverterRegistrationUnitTests {

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void considersNotExplicitlyReadingDependingOnTypes() {

		ConverterRegistration context = new ConverterRegistration(Person.class, String.class, false, false);
		assertThat(context.isWriting()).isTrue();
		assertThat(context.isReading()).isFalse();

		context = new ConverterRegistration(String.class, Person.class, false, false);
		assertThat(context.isWriting()).isFalse();
		assertThat(context.isReading()).isTrue();

		context = new ConverterRegistration(String.class, Class.class, false, false);
		assertThat(context.isWriting()).isTrue();
		assertThat(context.isReading()).isTrue();
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void forcesReadWriteOnlyIfAnnotated() {

		ConverterRegistration context = new ConverterRegistration(String.class, Class.class, false, true);
		assertThat(context.isWriting()).isTrue();
		assertThat(context.isReading()).isFalse();

		context = new ConverterRegistration(String.class, Class.class, true, false);
		assertThat(context.isWriting()).isFalse();
		assertThat(context.isReading()).isTrue();
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void considersConverterForReadAndWriteIfBothAnnotated() {

		ConverterRegistration context = new ConverterRegistration(String.class, Class.class, true, true);
		assertThat(context.isWriting()).isTrue();
		assertThat(context.isReading()).isTrue();
	}
}
