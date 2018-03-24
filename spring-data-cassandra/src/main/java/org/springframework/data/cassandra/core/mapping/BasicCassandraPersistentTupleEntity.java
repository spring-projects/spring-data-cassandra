/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;

/**
 * Cassandra Tuple-specific {@link org.springframework.data.mapping.PersistentEntity} for a mapped tuples. Mapped tuples
 * are nested level entities that can be referred from a {@link CassandraPersistentEntity}.
 *
 * @author Mark Paluch
 * @since 2.1
 * @see Tuple
 * @see Element
 */
public class BasicCassandraPersistentTupleEntity<T> extends BasicCassandraPersistentEntity<T> {

	private final Lazy<TupleType> tupleType;

	/**
	 * Creates a new {@link BasicCassandraPersistentTupleEntity} given {@link TypeInformation} and
	 * {@link TupleTypeFactory}.
	 *
	 * @param information must not be {@literal null}.
	 * @param tupleTypeFactory must not be {@literal null}.
	 */
	public BasicCassandraPersistentTupleEntity(TypeInformation<T> information, TupleTypeFactory tupleTypeFactory) {

		super(information, CassandraPersistentTupleMetadataVerifier.INSTANCE, TuplePropertyComparator.INSTANCE);

		Assert.notNull(tupleTypeFactory, "TupleTypeFactory must not be null");

		this.tupleType = Lazy.of(() -> tupleTypeFactory.create(getTupleFieldDataTypes()));
	}

	private List<DataType> getTupleFieldDataTypes() {

		return StreamSupport.stream(spliterator(), false)
				.sorted(TuplePropertyComparator.INSTANCE)
				.map(CassandraPersistentProperty::getDataType)
				.collect(Collectors.toList());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.BasicPersistentEntity#verify()
	 */
	@Override
	public void verify() throws MappingException {

		super.verify();

		CassandraPersistentTupleMetadataVerifier.INSTANCE.verify(this);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity#isTupleType()
	 */
	@Override
	public boolean isTupleType() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity#getTupleType()
	 */
	@Override
	public TupleType getTupleType() {
		return this.tupleType.get();
	}

	/**
	 * {@link CassandraPersistentProperty} comparator using to sort properties by their
	 * {@link CassandraPersistentProperty#getRequiredOrdinal()}.
	 *
	 * @see Element
	 */
	enum TuplePropertyComparator implements Comparator<CassandraPersistentProperty> {

		INSTANCE;

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(CassandraPersistentProperty propertyOne, CassandraPersistentProperty propertyTwo) {
			return Integer.compare(propertyOne.getRequiredOrdinal(), propertyTwo.getRequiredOrdinal());
		}
	}
}
