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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;

/**
 * Benchmark for {@link CassandraMappingContext}.
 *
 * @author Mark Paluch
 */
@State(Scope.Benchmark)
public class CassandraMappingContextBenchmark {

	@Benchmark
	public void measureGetPersistentEntity(Blackhole blackhole) {

		BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();
		mappingContext.setUserTypeResolver(typeName -> null);
		blackhole.consume(mappingContext.getPersistentEntity(Address.class));
	}

	@Benchmark
	public void measureGetPersistentEntityWithUdtReference(Blackhole blackhole) {

		BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();
		mappingContext.setUserTypeResolver(typeName -> null);
		blackhole.consume(mappingContext.getRequiredPersistentEntity(Customer.class));
	}

	public static void main(String[] args) throws RunnerException {

		Options opt = new OptionsBuilder() //
				.include(CassandraMappingContextBenchmark.class.getSimpleName()) //
				.forks(1) //
				.warmupIterations(5) //
				.measurementIterations(10) //
				.mode(Mode.AverageTime) //
				.timeUnit(TimeUnit.NANOSECONDS) //
				.build();

		new Runner(opt).run();
	}
}
