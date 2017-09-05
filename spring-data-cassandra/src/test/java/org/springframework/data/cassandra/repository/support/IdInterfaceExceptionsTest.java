/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

/**
 * Unit tests for class {@link IdInterfaceExceptions org.springframework.data.cassandra.repository.support.IdInterfaceExceptions}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class IdInterfaceExceptionsTest {

	@Test  //DATACASS-405
	public void testGetIdInterfaceName() throws Exception {

		IdInterfaceExceptions idInterfaceExceptions = new IdInterfaceExceptions(List.class);

		assertThat(idInterfaceExceptions.getIdInterfaceName()).isEqualTo("java.lang.Class");
	}
}