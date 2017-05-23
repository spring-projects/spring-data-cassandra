/*
 * Copyright 2013-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.integration.config.java;

import java.util.ArrayList;
import java.util.List;

import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.test.integration.support.AbstractTestJavaConfig;
import org.springframework.context.annotation.Configuration;

/**
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@Configuration
public class KeyspaceCreatingJavaConfig extends AbstractTestJavaConfig {

	public static final String KEYSPACE_NAME = "foo";

	@Override
	protected String getKeyspaceName() {
		return KEYSPACE_NAME;
	}

	@Override
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		ArrayList<CreateKeyspaceSpecification> list = new ArrayList<>();

		CreateKeyspaceSpecification specification = CreateKeyspaceSpecification.createKeyspace().name(getKeyspaceName());
		specification.with(KeyspaceOption.REPLICATION, KeyspaceAttributes.newSimpleReplication(1L));

		list.add(specification);
		return list;
	}
}
