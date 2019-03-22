/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.benchmarks;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import io.netty.channel.EventLoopGroup;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.mapping.UserTypeResolver;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Benchmark for {@link MappingCassandraConverter} requiring a running Apache Cassandra server on {@code localhost:9042}
 * providing a keyspace named {@code example}.
 *
 * @author Mark Paluch
 */
@State(Scope.Benchmark)
public class MappingCassandraConverterOnlineBenchmark {

	private final BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();
	private final MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

	private Cluster cluster;
	private Session session;
	private UserTypeResolver userTypeResolver;

	private Customer customerWithMappedUdt;

	@Setup
	public void beforeBenchmark() throws Exception {

		cluster = Cluster.builder().addContactPoint("localhost").withNettyOptions(new NettyOptions() {

			public void onClusterClose(EventLoopGroup eventLoopGroup) {
				eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
			}

		}).build();

		CassandraSessionFactoryBean sessionFactoryBean = new CassandraSessionFactoryBean();

		sessionFactoryBean.setKeyspaceName("example");
		sessionFactoryBean.setSchemaAction(SchemaAction.RECREATE);
		sessionFactoryBean.setCluster(cluster);
		sessionFactoryBean.setConverter(converter);

		this.userTypeResolver = new SimpleUserTypeResolver(cluster, "example");
		this.mappingContext.setUserTypeResolver(userTypeResolver);

		this.mappingContext.getPersistentEntity(Customer.class);
		this.mappingContext.getPersistentEntity(Address.class);

		sessionFactoryBean.afterPropertiesSet();

		this.session = sessionFactoryBean.getObject();

		UDTValue udtValue = this.userTypeResolver.resolveType(CqlIdentifier.cqlId("address")).newValue();
		udtValue.setString("zip", "12345");
		udtValue.setString("city", "Albuquerque");

		this.session.execute(QueryBuilder.truncate("customer"));
		this.session.execute(QueryBuilder.insertInto("customer").value("id", "my-id").value("firstname", "Walter")
				.value("lastname", "White").value("address", udtValue));

		Address address = new Address("12345", "Albuquerque");

		Customer customerWithMappedUdt = createCustomer();
		customerWithMappedUdt.setAddress(address);

		this.customerWithMappedUdt = customerWithMappedUdt;
	}

	private Customer createCustomer() {

		Customer customer = new Customer();

		customer.setId("my-id");
		customer.setFirstname("Walter");
		customer.setLastname("White");

		return customer;
	}

	@TearDown
	public void afterBenchmark() {

		this.session.close();
		this.cluster.close();
	}

	@Benchmark
	public void measureReadRowPlain(Blackhole blackhole) {

		ResultSet rows = this.session.execute(QueryBuilder.select().from("customer").where(eq("id", "my-id")));

		Row row = rows.one();
		blackhole.consume(row.getString("id"));
		blackhole.consume(row.getString("firstname"));
		blackhole.consume(row.getString("lastname"));

		UDTValue address = row.getUDTValue("address");
		blackhole.consume(address.getString("zip"));
		blackhole.consume(address.getString("city"));
	}

	@Benchmark
	public void measureReadRowMapped(Blackhole blackhole) {

		ResultSet rows = this.session.execute(QueryBuilder.select().from("customer").where(eq("id", "my-id")));

		Row row = rows.one();
		blackhole.consume(this.converter.read(Customer.class, row));
	}

	@Benchmark
	public void measureWriteRowPlain() {

		UDTValue udtValue = this.userTypeResolver.resolveType(CqlIdentifier.cqlId("address")).newValue();
		udtValue.setString("zip", "12345");
		udtValue.setString("city", "Albuquerque");

		Statement statement = QueryBuilder.update("customer") //
				.where(eq("id", "my-id")) //
				.with(QueryBuilder.set("firstname", "Walter")) //
				.and(QueryBuilder.set("lastname", "White")) //
				.and(QueryBuilder.set("address", udtValue));

		this.session.execute(statement);
	}

	@Benchmark
	public void measureWriteRowMapped() {

		Update update = QueryBuilder.update("customer");

		this.converter.write(customerWithMappedUdt, update);
		this.session.execute(update);
	}

	public static void main(String[] args) throws Exception {

		Options opt = new OptionsBuilder() //
				.include(MappingCassandraConverterOnlineBenchmark.class.getSimpleName()) //
				.forks(1) //
				.warmupIterations(5) //
				.measurementIterations(10) //
				.mode(Mode.AverageTime) //
				.timeUnit(TimeUnit.NANOSECONDS) //
				.build();

		new Runner(opt).run();

	}
}
