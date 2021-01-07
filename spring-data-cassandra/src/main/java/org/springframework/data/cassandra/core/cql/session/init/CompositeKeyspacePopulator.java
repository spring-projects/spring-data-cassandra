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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Composite {@link KeyspacePopulator} that delegates to a list of given {@link KeyspacePopulator} implementations,
 * executing all scripts.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public class CompositeKeyspacePopulator implements KeyspacePopulator {

	private final List<KeyspacePopulator> populators = new ArrayList<>(4);

	/**
	 * Create an empty {@link CompositeKeyspacePopulator}.
	 *
	 * @see #setPopulators
	 * @see #addPopulators
	 */
	public CompositeKeyspacePopulator() {}

	/**
	 * Create a {@link CompositeKeyspacePopulator} with the given populators.
	 *
	 * @param populators one or more populators to delegate to.
	 */
	public CompositeKeyspacePopulator(Collection<KeyspacePopulator> populators) {
		this.populators.addAll(populators);
	}

	/**
	 * Create a {@link CompositeKeyspacePopulator} with the given populators.
	 *
	 * @param populators one or more populators to delegate to.
	 */
	public CompositeKeyspacePopulator(KeyspacePopulator... populators) {
		this.populators.addAll(Arrays.asList(populators));
	}

	/**
	 * Specify one or more populators to delegate to.
	 */
	public void setPopulators(KeyspacePopulator... populators) {
		this.populators.clear();
		this.populators.addAll(Arrays.asList(populators));
	}

	/**
	 * Add one or more populators to the list of delegates.
	 */
	public void addPopulators(KeyspacePopulator... populators) {
		this.populators.addAll(Arrays.asList(populators));
	}

	@Override
	public void populate(CqlSession session) throws ScriptException {

		for (KeyspacePopulator populator : this.populators) {
			populator.populate(session);
		}
	}
}
