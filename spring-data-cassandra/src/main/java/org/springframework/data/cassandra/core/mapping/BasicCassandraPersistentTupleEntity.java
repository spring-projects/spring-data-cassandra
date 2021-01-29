/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.util.Comparator;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.TypeInformation;

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

	/**
	 * Creates a new {@link BasicCassandraPersistentTupleEntity} given {@link TypeInformation}.
	 *
	 * @param information must not be {@literal null}.
	 */
	public BasicCassandraPersistentTupleEntity(TypeInformation<T> information) {

		super(information, CassandraPersistentTupleMetadataVerifier.INSTANCE, TuplePropertyComparator.INSTANCE);
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
