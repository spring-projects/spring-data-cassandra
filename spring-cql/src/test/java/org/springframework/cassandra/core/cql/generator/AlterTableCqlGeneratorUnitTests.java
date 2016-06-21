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
package org.springframework.cassandra.core.cql.generator;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;

import com.datastax.driver.core.DataType;

/**
 * Unit tests for {@link AlterTableCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public class AlterTableCqlGeneratorUnitTests {

	/**
	 * @see DATACASS-192
	 */
	@Test
	public void alterTableAlterColumnType() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").alter("lastKnownLocation",
				DataType.uuid());

		assertThat(toCql(spec), is(equalTo("ALTER TABLE addamsfamily ALTER lastknownlocation TYPE uuid;")));
	}

	/**
	 * @see DATACASS-192
	 */
	@Test
	public void alterTableAlterListColumnType() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").alter("lastKnownLocation",
				DataType.list(DataType.ascii()));

		assertThat(toCql(spec), is(equalTo("ALTER TABLE addamsfamily ALTER lastknownlocation TYPE list<ascii>;")));
	}

	/**
	 * @see DATACASS-192
	 */
	@Test
	public void alterTableAddColumn() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").add("gravesite",
				DataType.varchar());

		assertThat(toCql(spec), is(equalTo("ALTER TABLE addamsfamily ADD gravesite varchar;")));
	}

	/**
	 * @see DATACASS-192
	 */
	@Test
	public void alterTableAddListColumn() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("users").add("top_places",
				DataType.list(DataType.ascii()));

		assertThat(toCql(spec), is(equalTo("ALTER TABLE users ADD top_places list<ascii>;")));
	}

	private String toCql(AlterTableSpecification spec) {
		return new AlterTableCqlGenerator(spec).toCql();
	}
}
