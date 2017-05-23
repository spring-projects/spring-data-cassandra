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

package org.springframework.data.cassandra.mapping;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.junit.Test;
import org.springframework.cassandra.core.cql.CqlIdentifier;

/**
 * Unit tests for class {@link SimpleUserTypeResolver org.springframework.data.cassandra.mapping.SimpleUserTypeResolver}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class SimpleUserTypeResolverTest {

	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testObjectCreationClusterMustNotBeNull() throws Exception {

		new SimpleUserTypeResolver(null, "a");
	}

	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testObjectCreationKeySpaceNameMustNotBeNull() throws Exception {

		Cluster cluster = Cluster.builder().addContactPoint("192.168.0.1").build();

		new SimpleUserTypeResolver(cluster, null);
	}

	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testObjectCreationKeySpaceNameMustContainText() throws Exception {

		Cluster cluster = Cluster.builder().addContactPoint("192.168.0.1").build();

		new SimpleUserTypeResolver(cluster, "");
	}
	
	@Test(expected = NoHostAvailableException.class)  //DATACASS-405
	public void testResolveType() throws Exception {

		Cluster cluster = Cluster.builder().addContactPoint("192.168.0.1").build();

		CqlIdentifier cqlIdentifier = new CqlIdentifier("asdf");

		SimpleUserTypeResolver simpleUserTypeResolver = new SimpleUserTypeResolver(cluster, "a");

		simpleUserTypeResolver.resolveType(cqlIdentifier);
	}
}