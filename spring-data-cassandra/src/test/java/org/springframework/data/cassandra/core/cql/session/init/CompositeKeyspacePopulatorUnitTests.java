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

import static org.mockito.Mockito.*;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Unit tests for {@link CompositeKeyspacePopulator}.
 *
 * @author Mark Paluch
 */
class CompositeKeyspacePopulatorUnitTests {

	private final CqlSession mockedConnection = mock(CqlSession.class);

	private final KeyspacePopulator mockedKeyspacePopulator1 = mock(KeyspacePopulator.class);

	private final KeyspacePopulator mockedKeyspacePopulator2 = mock(KeyspacePopulator.class);

	@Test // DATACASS-704
	void addPopulators() {

		CompositeKeyspacePopulator populator = new CompositeKeyspacePopulator();
		populator.addPopulators(mockedKeyspacePopulator1, mockedKeyspacePopulator2);

		populator.populate(mockedConnection);

		verify(mockedKeyspacePopulator1, times(1)).populate(mockedConnection);
		verify(mockedKeyspacePopulator2, times(1)).populate(mockedConnection);
	}

	@Test // DATACASS-704
	void setPopulatorsWithMultiple() {

		CompositeKeyspacePopulator populator = new CompositeKeyspacePopulator();
		populator.setPopulators(mockedKeyspacePopulator1, mockedKeyspacePopulator2); // multiple

		populator.populate(mockedConnection);

		verify(mockedKeyspacePopulator1, times(1)).populate(mockedConnection);
		verify(mockedKeyspacePopulator2, times(1)).populate(mockedConnection);
	}

	@Test // DATACASS-704
	void setPopulatorsForOverride() {

		CompositeKeyspacePopulator populator = new CompositeKeyspacePopulator();
		populator.setPopulators(mockedKeyspacePopulator1);
		populator.setPopulators(mockedKeyspacePopulator2); // override

		populator.populate(mockedConnection);

		verify(mockedKeyspacePopulator1, times(0)).populate(mockedConnection);
		verify(mockedKeyspacePopulator2, times(1)).populate(mockedConnection);
	}

	@Test // DATACASS-704
	void constructWithVarargs() {

		CompositeKeyspacePopulator populator = new CompositeKeyspacePopulator(mockedKeyspacePopulator1,
				mockedKeyspacePopulator2);

		populator.populate(mockedConnection);

		verify(mockedKeyspacePopulator1, times(1)).populate(mockedConnection);
		verify(mockedKeyspacePopulator2, times(1)).populate(mockedConnection);
	}

	@Test // DATACASS-704
	void constructWithCollection() {

		Set<KeyspacePopulator> populators = new LinkedHashSet<>();
		populators.add(mockedKeyspacePopulator1);
		populators.add(mockedKeyspacePopulator2);
		CompositeKeyspacePopulator populator = new CompositeKeyspacePopulator(populators);

		populator.populate(mockedConnection);

		verify(mockedKeyspacePopulator1, times(1)).populate(mockedConnection);
		verify(mockedKeyspacePopulator2, times(1)).populate(mockedConnection);
	}
}
