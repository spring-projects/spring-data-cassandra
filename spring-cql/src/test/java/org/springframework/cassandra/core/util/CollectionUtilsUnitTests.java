/*
 *  Copyright 2013-2016 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cassandra.core.util;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

/**
 * The CollectionUtilsUnitTests class is a test suite of test cases testing the contract and functionality
 * of the {@link CollectionUtils} class.
 *
 * @author John Blum
 * @see org.springframework.cassandra.core.util.CollectionUtils
 * @since 1.5.0
 */
public class CollectionUtilsUnitTests {

	<T> List<T> asList(T... array) {
		return Arrays.asList(array);
	}

	void assertNonNullEmptyArray(Object[] array) {
		assertThat(array, is(notNullValue()));
		assertThat(array.length, is(equalTo(0)));
	}

	void assertNonNullEmptyCollection(Collection<?> collection) {
		assertThat(collection, is(notNullValue()));
		assertThat(collection.isEmpty(), is(true));
	}

	<T> Iterable<T> newIterable(final T... elements) {
		return new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return new Iterator<T>() {
					int index = 0;

					@Override
					public boolean hasNext() {
						return (index < elements.length);
					}

					@Override
					public T next() {
						return elements[index++];
					}
				};
			}
		};
	}

	@Test
	public void toArrayWithIterable() {
		Object[] array = CollectionUtils.toArray(newIterable(1, 2, 3));

		assertThat(array, is(notNullValue()));
		assertThat(array.length, is(equalTo(3)));

		for (int index = 0; index < array.length; index++) {
			Object valueAtIndex = (index + 1);
			assertThat(array[index], is(equalTo(valueAtIndex)));
		}
	}

	@Test
	public void toArrayWithEmptyIterable() {
		assertNonNullEmptyArray(CollectionUtils.toArray(newIterable()));
	}

	@Test
	public void toArrayWithNull() {
		assertNonNullEmptyArray(CollectionUtils.toArray(null));
	}

	@Test
	public void toListWithArray() {
		List<String> list = CollectionUtils.toList("test", "testing", "tested");

		assertThat(list, is(notNullValue()));
		assertThat(list.size(), is(equalTo(3)));
		assertThat(list.containsAll(asList("test", "testing", "tested")), is(true));
	}

	@Test
	public void toListWithEmptyArray() {
		assertNonNullEmptyCollection(CollectionUtils.toList());
	}

	@Test
	public void toListWithNullArray() {
		assertNonNullEmptyCollection(CollectionUtils.toList((Object[]) null));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void toListWithIterable() {
		List<Integer> list = CollectionUtils.toList(newIterable(1, 2, 3));

		assertThat(list, is(notNullValue()));
		assertThat(list.size(), is(equalTo(3)));
		assertThat(list.containsAll(asList(1, 2, 3)), is(true));
	}

	@Test
	public void toListWithList() {
		List<Integer> expected = asList(1, 2, 3);
		List<Integer> actual = CollectionUtils.toList(expected);

		assertThat(actual, is(sameInstance(expected)));
	}

	@Test
	public void toListWithNullIterable() {
		assertNonNullEmptyCollection(CollectionUtils.toList((Iterable<?>) null));
	}

}
