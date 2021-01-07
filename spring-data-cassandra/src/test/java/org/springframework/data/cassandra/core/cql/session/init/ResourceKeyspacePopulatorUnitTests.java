/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session.init;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * Unit tests for {@link ResourceKeyspacePopulator}.
 *
 * @author Mark Paluch
 */
class ResourceKeyspacePopulatorUnitTests {

	private static final Resource script1 = mock(Resource.class);
	private static final Resource script2 = mock(Resource.class);
	private static final Resource script3 = mock(Resource.class);

	@Test // DATACASS-704
	void constructWithNullResource() {

		assertThatIllegalArgumentException().isThrownBy(() -> new ResourceKeyspacePopulator((Resource) null));
	}

	@Test // DATACASS-704
	void constructWithNullResourceArray() {

		assertThatIllegalArgumentException().isThrownBy(() -> new ResourceKeyspacePopulator((Resource[]) null));
	}

	@Test // DATACASS-704
	void constructWithResource() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator(script1);

		assertThat(keyspacePopulator.scripts).hasSize(1);
	}

	@Test // DATACASS-704
	void constructWithMultipleResources() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator(script1, script2);

		assertThat(keyspacePopulator.scripts).hasSize(2);
	}

	@Test // DATACASS-704
	void constructWithMultipleResourcesAndThenAddScript() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator(script1, script2);

		assertThat(keyspacePopulator.scripts).hasSize(2);

		keyspacePopulator.addScript(script3);

		assertThat(keyspacePopulator.scripts).hasSize(3);
	}

	@Test // DATACASS-704
	void addScriptsWithNullResource() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator();

		assertThatIllegalArgumentException().isThrownBy(() -> keyspacePopulator.addScripts((Resource) null));
	}

	@Test // DATACASS-704
	void addScriptsWithNullResourceArray() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator();

		assertThatIllegalArgumentException().isThrownBy(() -> keyspacePopulator.addScripts((Resource[]) null));
	}

	@Test // DATACASS-704
	void setScriptsWithNullResource() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator();

		assertThatIllegalArgumentException().isThrownBy(() -> keyspacePopulator.setScripts((Resource) null));
	}

	@Test // DATACASS-704
	void setScriptsWithNullResourceArray() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator();

		assertThatIllegalArgumentException().isThrownBy(() -> keyspacePopulator.setScripts((Resource[]) null));
	}

	@Test // DATACASS-704
	void shouldFailOnError() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator();
		keyspacePopulator.setScripts(new ByteArrayResource("drop table;create table;".getBytes()));

		CqlSession sessionMock = mock(CqlSession.class);
		when(sessionMock.execute("drop table")).thenThrow(new IllegalStateException("Boom!"));

		assertThatExceptionOfType(ScriptStatementFailedException.class)
				.isThrownBy(() -> keyspacePopulator.populate(sessionMock));

		verify(sessionMock).execute("drop table");
		verifyNoMoreInteractions(sessionMock);
	}

	@Test // DATACASS-704
	void shouldContinueOnError() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator();
		keyspacePopulator.setIgnoreFailedDrops(true);
		keyspacePopulator.setScripts(new ByteArrayResource("drop table;create table;".getBytes()));

		CqlSession sessionMock = mock(CqlSession.class);
		when(sessionMock.execute("drop table")).thenThrow(new IllegalStateException("Boom!"));

		when(sessionMock.execute("create table")).thenReturn(mock(ResultSet.class));

		keyspacePopulator.populate(sessionMock);

		verify(sessionMock).execute("drop table");
		verify(sessionMock).execute("create table");
	}

	@Test
	void setScriptsAndThenAddScript() {

		ResourceKeyspacePopulator keyspacePopulator = new ResourceKeyspacePopulator();
		assertThat(keyspacePopulator.scripts).isEmpty();

		keyspacePopulator.setScripts(script1, script2);
		assertThat(keyspacePopulator.scripts).hasSize(2);

		keyspacePopulator.addScript(script3);
		assertThat(keyspacePopulator.scripts).hasSize(3);
	}
}
