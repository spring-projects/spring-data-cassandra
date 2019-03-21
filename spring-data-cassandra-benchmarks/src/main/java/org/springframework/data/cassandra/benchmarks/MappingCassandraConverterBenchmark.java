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

import static java.util.Arrays.*;
import static org.springframework.data.cassandra.benchmarks.MappingCassandraConverterBenchmark.BenchmarkDependencyFactory.*;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.UserTypeResolver;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.UserType.Field;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Benchmark for {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 */
@State(Scope.Benchmark)
public class MappingCassandraConverterBenchmark {

	private final BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();
	private final MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

	private ColumnDefinitions customerColumns;
	private UserType addressType;

	private ByteBuffer id;
	private ByteBuffer firstname;
	private ByteBuffer lastname;
	private ByteBuffer address;

	private Customer customer;
	private Customer customerWithMappedUdt;

	@Setup
	public void beforeBenchmark() throws ReflectiveOperationException {

		this.mappingContext.setUserTypeResolver(new BenchmarkUserTypeResolver());

		this.mappingContext.getPersistentEntity(Customer.class);
		this.mappingContext.getPersistentEntity(Address.class);

		this.addressType = createUserType("address",
				asList(field("zip", DataType.varchar()), field("city", DataType.varchar())));

		this.customerColumns = columnDefinitions(asList(definition("id", DataType.varchar()), //
				definition("firstname", DataType.varchar()), //
				definition("lastname", DataType.varchar()), //
				definition("address", this.addressType) //
		));

		this.id = encode("my-id", DataType.varchar());
		this.firstname = encode("Walter", DataType.varchar());
		this.lastname = encode("White", DataType.varchar());

		UDTValue udtValue = this.addressType.newValue();
		udtValue.setString("zip", "12345");
		udtValue.setString("city", "Albuquerque");

		this.address = encode(udtValue, this.addressType);

		Address address = new Address("12345", "Albuquerque");

		this.customer = createCustomer();

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

	// Benchmark
	public void measureReadRow() throws ReflectiveOperationException {

		Row row = createRow(this.customerColumns,
				asList(id.duplicate(), this.firstname.duplicate(), this.lastname.duplicate(), null));

		this.converter.read(Customer.class, row);
	}

	// @Benchmark
	public void measureReadRowWithUdt() throws ReflectiveOperationException {

		Row row = createRow(this.customerColumns,
				asList(this.id.duplicate(), this.firstname.duplicate(), this.lastname.duplicate(), this.address.duplicate()));

		converter.read(Customer.class, row);
	}

	@Benchmark
	public void measureWriteQuery() throws ReflectiveOperationException {
		converter.write(this.customer, QueryBuilder.insertInto("table"));
	}

	@Benchmark
	public void measureWriteRowWithUdt() throws ReflectiveOperationException {
		converter.write(this.customerWithMappedUdt, QueryBuilder.insertInto("table"));
	}

	public static void main(String[] args) throws Exception {

		Options opt = new OptionsBuilder() //
				.include(MappingCassandraConverterBenchmark.class.getSimpleName()) //
				.forks(1) //
				.warmupIterations(5) //
				.measurementIterations(10) //
				.mode(Mode.AverageTime) //
				.timeUnit(TimeUnit.NANOSECONDS) //
				.build();

		new Runner(opt).run();
	}

	class BenchmarkUserTypeResolver implements UserTypeResolver {

		@Override
		public UserType resolveType(CqlIdentifier typeName) {
			return addressType;
		}
	}

	/**
	 * Factory to create dependencies required for the benchmark.
	 */
	static class BenchmarkDependencyFactory {

		static Row createRow(ColumnDefinitions definitions, List<ByteBuffer> data) throws ReflectiveOperationException {

			Class<Row> rowClass = (Class) Class.forName("com.datastax.driver.core.ArrayBackedRow");
			Class<?> tokenFactoryClass = Class.forName("com.datastax.driver.core.Token$Factory");

			Constructor<Row> constructor = ReflectionUtils.accessibleConstructor(rowClass, ColumnDefinitions.class,
					tokenFactoryClass, ProtocolVersion.class, List.class);

			return constructor.newInstance(definitions, null, ProtocolVersion.NEWEST_SUPPORTED, data);
		}

		static Definition definition(String name, DataType type) throws ReflectiveOperationException {

			Constructor<Definition> constructor = ReflectionUtils.accessibleConstructor(Definition.class, String.class,
					String.class, String.class, DataType.class);

			return constructor.newInstance("keyspace", "table", name, type);
		}

		static ColumnDefinitions columnDefinitions(Collection<Definition> definitions) throws ReflectiveOperationException {

			Constructor<ColumnDefinitions> constructor = ReflectionUtils.accessibleConstructor(ColumnDefinitions.class,
					Definition[].class, CodecRegistry.class);

			return constructor.newInstance(definitions.toArray(new ColumnDefinitions.Definition[0]),
					CodecRegistry.DEFAULT_INSTANCE);
		}

		static Field field(String name, DataType type) throws ReflectiveOperationException {

			Class<Field> fieldClass = (Class) Class.forName("com.datastax.driver.core.UserType$Field");

			Constructor<Field> constructor = ReflectionUtils.accessibleConstructor(fieldClass, String.class, DataType.class);

			return constructor.newInstance(name, type);
		}

		static UserType createUserType(String name, Collection<Field> fields) throws ReflectiveOperationException {

			Constructor<UserType> constructor = ReflectionUtils.accessibleConstructor(UserType.class, String.class,
					String.class, Collection.class, ProtocolVersion.class, CodecRegistry.class);

			return constructor.newInstance("keyspace", name, fields, ProtocolVersion.NEWEST_SUPPORTED,
					CodecRegistry.DEFAULT_INSTANCE);
		}

		static ByteBuffer encode(Object data, DataType type) throws ReflectiveOperationException {

			TypeCodec<Object> objectTypeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(type);

			return objectTypeCodec.serialize(data, ProtocolVersion.NEWEST_SUPPORTED);
		}
	}
}
