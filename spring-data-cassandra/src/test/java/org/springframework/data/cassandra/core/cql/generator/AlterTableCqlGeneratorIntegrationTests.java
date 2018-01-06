/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.cql.keyspace.AlterTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CachingOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.KeyCachingOption;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.util.Version;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

/**
 * Integration tests tests for {@link AlterTableCqlGenerator}.
 *
 * @author Mark Paluch
 */
public class AlterTableCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	static final Version CASSANDRA_3_10 = Version.parse("3.10");

	Version cassandraVersion;

	@Before
	public void setUp() throws Exception {

		cassandraVersion = CassandraVersion.get(session);

		session.execute("DROP TABLE IF EXISTS addamsFamily;");
		session.execute("DROP TABLE IF EXISTS users;");
	}

	@Test // DATACASS-192, DATACASS-429
	public void alterTableAlterColumnType() {

		assumeTrue(cassandraVersion.isLessThan(CASSANDRA_3_10));

		session.execute(
				"CREATE TABLE addamsFamily (name varchar PRIMARY KEY, gender varchar,\n" + "  lastknownlocation bigint);");

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").alter("lastKnownLocation",
				DataType.varint());

		execute(spec);

		ColumnMetadata column = getTableMetadata("addamsFamily").getColumn("lastKnownLocation");

		assertThat(column.getType()).isEqualTo(DataType.varint());
	}

	@Test // DATACASS-192, DATACASS-429
	public void alterTableAlterListColumnType() {

		assumeTrue(cassandraVersion.isLessThan(CASSANDRA_3_10));

		session.execute(
				"CREATE TABLE addamsFamily (name varchar PRIMARY KEY, gender varchar,\n" + "  lastknownlocation list<ascii>);");

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").alter("lastKnownLocation",
				DataType.list(DataType.varchar()));

		execute(spec);

		ColumnMetadata column = getTableMetadata("addamsFamily").getColumn("lastKnownLocation");

		assertThat(column.getType()).isEqualTo((DataType) DataType.list(DataType.varchar()));
	}

	@Test // DATACASS-192
	public void alterTableAddColumn() {

		session.execute(
				"CREATE TABLE addamsFamily (name varchar PRIMARY KEY, gender varchar,\n" + "  lastknownlocation varchar);");

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").add("gravesite",
				DataType.varchar());

		execute(spec);

		ColumnMetadata column = getTableMetadata("addamsFamily").getColumn("gravesite");

		assertThat(column.getType()).isEqualTo(DataType.varchar());
	}

	@Test // DATACASS-192
	public void alterTableAddListColumn() {

		session.execute("CREATE TABLE users (user_name varchar PRIMARY KEY);");

		AlterTableSpecification spec = AlterTableSpecification.alterTable("users").add("top_places",
				DataType.list(DataType.ascii()));

		execute(spec);

		ColumnMetadata column = getTableMetadata("users").getColumn("top_places");

		assertThat(column.getType()).isEqualTo((DataType) DataType.list(DataType.ascii()));
	}

	@Test // DATACASS-192
	public void alterTableDropColumn() {

		session.execute("CREATE TABLE addamsFamily (name varchar PRIMARY KEY, gender varchar);");

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").drop("gender");

		execute(spec);

		assertThat(getTableMetadata("addamsfamily").getColumn("gender")).isNull();
	}

	@Test // DATACASS-192
	public void alterTableRenameColumn() {

		session.execute("CREATE TABLE addamsFamily (name varchar PRIMARY KEY, firstname varchar);");

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").rename("name", "newname");

		execute(spec);

		assertThat(getTableMetadata("addamsfamily").getColumn("name")).isNull();
		assertThat(getTableMetadata("addamsfamily").getColumn("newname")).isNotNull();
	}

	@Test // DATACASS-192
	public void alterTableAddCaching() {

		session.execute("CREATE TABLE users (user_name varchar PRIMARY KEY);");

		Map<Object, Object> cachingMap = new LinkedHashMap<>();
		cachingMap.put(CachingOption.KEYS, KeyCachingOption.NONE);
		cachingMap.put(CachingOption.ROWS_PER_PARTITION, "15");

		AlterTableSpecification spec = AlterTableSpecification.alterTable("users").with(TableOption.CACHING, cachingMap);

		execute(spec);

		assertThat(getTableMetadata("users").getOptions().getCaching().get("keys")).isEqualTo("NONE");
		assertThat(getTableMetadata("users").getOptions().getCaching().get("rows_per_partition")).isEqualTo("15");

	}

	private void execute(AlterTableSpecification spec) {
		session.execute(new AlterTableCqlGenerator(spec).toCql());
	}

	private TableMetadata getTableMetadata(String table) {

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		return keyspace.getTable(table);
	}
}
