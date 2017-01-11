/*
 *  Copyright 2013-2017 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cassandra.core.util;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

/**
 * The CollectionUtilsUnitTests class is a test suite of test cases testing the contract and functionality of the
 * {@link CollectionUtils} class.
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
		assertThat(array).isNotNull();
		assertThat(array.length).isEqualTo(0);
	}

	void assertNonNullEmptyCollection(Collection<?> collection) {
		assertThat(collection).isNotNull();
		assertThat(collection.isEmpty()).isTrue();
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

		assertThat(array).isNotNull();
		assertThat(array.length).isEqualTo(3);

		for (int index = 0; index < array.length; index++) {
			Object valueAtIndex = (index + 1);
			assertThat(array[index]).isEqualTo(valueAtIndex);
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

		assertThat(list).isNotNull();
		assertThat(list).hasSize(3);
		assertThat(list.containsAll(asList("test", "testing", "tested"))).isTrue();
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

		assertThat(list).isNotNull();
		assertThat(list).hasSize(3);
		assertThat(list.containsAll(asList(1, 2, 3))).isTrue();
	}

	@Test
	public void toListWithList() {
		List<Integer> expected = asList(1, 2, 3);
		List<Integer> actual = CollectionUtils.toList(expected);

		assertThat(actual).isSameAs(expected);
	}

	@Test
	public void toListWithNullIterable() {
		assertNonNullEmptyCollection(CollectionUtils.toList((Iterable<?>) null));
	}

}
