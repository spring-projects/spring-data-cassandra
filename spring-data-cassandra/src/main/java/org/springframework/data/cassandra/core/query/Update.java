/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.core.query;

import static org.springframework.data.cassandra.core.query.SerializationUtils.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.core.query.Update.AddToOp.Mode;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Update object representing representing a set of update operations.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class Update {

	private Map<ColumnName, UpdateOp> updateOperations = new LinkedHashMap<>();

	/**
	 * Create an empty {@link Update} object.
	 */
	public Update() {}

	/**
	 * Create a {@link Update} object given a list of {@link UpdateOp}s.
	 *
	 * @param updateOps must not be {@literal null}.
	 */
	public Update(List<UpdateOp> updateOps) {

		Assert.notNull(updateOps, "Update operations must not be null");

		updateOps.forEach(this::add);
	}

	/**
	 * Set the {@code columnName} to {@code value}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @param value
	 * @return {@code this} {@link Update} object.
	 */
	public Update set(String columnName, Object value) {
		return add(new SetOp(ColumnName.from(columnName), value));
	}

	/**
	 * Create a new {@link SetBuilder} to set a collection item for {@code columnName} in a fluent style.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return {@code this} {@link Update} object.
	 */
	public SetBuilder set(String columnName) {
		return new DefaultSetBuilder(ColumnName.from(columnName));
	}

	/**
	 * Create a new {@link AddToBuilder} to add items to a collection for {@code columnName} in a fluent style.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return {@code this} {@link Update} object.
	 */
	public AddToBuilder addTo(String columnName) {
		return new DefaultAddToBuilder(ColumnName.from(columnName));
	}

	/**
	 * Remove {@code value} from the collection at {@code columnName}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@code this} {@link Update} object.
	 */
	public Update remove(String columnName, Object value) {
		return add(new RemoveOp(ColumnName.from(columnName), Collections.singletonList(value)));
	}

	/**
	 * Cleat the collection at {@code columnName}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return {@code this} {@link Update} object.
	 */
	public Update clear(String columnName) {
		return add(new SetOp(ColumnName.from(columnName), Collections.emptyList()));
	}

	/**
	 * Increment the value at {@code columnName} by {@literal 1}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return {@code this} {@link Update} object.
	 */
	public Update increment(String columnName) {
		return increment(columnName, 1);
	}

	/**
	 * Increment the value at {@code columnName} by {@code delta}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @param delta increment value.
	 * @return {@code this} {@link Update} object.
	 */
	public Update increment(String columnName, int delta) {
		return add(new IncrOp(ColumnName.from(columnName), delta));
	}

	/**
	 * Decrement the value at {@code columnName} by {@literal 1}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return {@code this} {@link Update} object.
	 */
	public Update decrement(String columnName) {
		return decrement(columnName, 1);
	}

	/**
	 * Decrement the value at {@code columnName} by {@code delta}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @param delta decrement value.
	 * @return {@code this} {@link Update} object.
	 */
	public Update decrement(String columnName, int delta) {
		return add(new IncrOp(ColumnName.from(columnName), -Math.abs(delta)));
	}

	/**
	 * @return {@link Collection} of update operations.
	 */
	public Collection<UpdateOp> getUpdateOperations() {
		return Collections.unmodifiableCollection(updateOperations.values());
	}

	private Update add(UpdateOp updateOp) {

		this.updateOperations.put(updateOp.getColumnName(), updateOp);
		return this;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return StringUtils.collectionToDelimitedString(updateOperations.values(), ", ");
	}

	/**
	 * Builder to add a single element/multiple elements to a collection associated with a {@link ColumnName}.
	 *
	 * @author Mark Paluch
	 */
	public interface AddToBuilder {

		/**
		 * Prepend the {@code value} to the collection.
		 *
		 * @param object must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update prepend(Object value);

		/**
		 * Prepend all {@code values} to the collection.
		 *
		 * @param values must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update prependAll(Object... values);

		/**
		 * Prepend all {@code values} to the collection.
		 *
		 * @param values must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update prependAll(Iterable<? extends Object> values);

		/**
		 * Append the {@code value} to the collection.
		 *
		 * @param object must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update append(Object value);

		/**
		 * Append all {@code values} to the collection.
		 *
		 * @param values must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update appendAll(Object... values);

		/**
		 * Append all {@code values} to the collection.
		 *
		 * @param values must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update appendAll(Iterable<? extends Object> values);

		/**
		 * Associate the specified {@code value} with the specified {@code key} in the map.
		 *
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update entry(Object key, Object value);

		/**
		 * Associate all entries of the specified {@code map} with the map at {@link ColumnName}.
		 *
		 * @param map must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update addAll(Map<? extends Object, ? extends Object> map);
	}

	/**
	 * Default {@link AddToBuilder} implementation.
	 */
	private class DefaultAddToBuilder implements AddToBuilder {

		private final ColumnName columnName;

		DefaultAddToBuilder(ColumnName columnName) {
			this.columnName = columnName;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#prepend(java.lang.Object)
		 */
		@Override
		public Update prepend(Object value) {
			return prependAll(Collections.singleton(value));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#append(java.lang.Object)
		 */
		@Override
		public Update append(Object value) {
			return prependAll(Collections.singleton(value));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#entry(java.lang.Object, java.lang.Object)
		 */
		@Override
		public Update entry(Object key, Object value) {

			Assert.notNull(key, "Key must not be null");
			Assert.notNull(value, "Value must not be null");

			return addAll(Collections.singletonMap(key, value));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#appendAll(java.lang.Object[])
		 */
		@Override
		public Update appendAll(Object... values) {

			Assert.notNull(values, "Values must not be null");

			return appendAll(Arrays.asList(values));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#appendAll(java.lang.Iterable)
		 */
		@Override
		public Update appendAll(Iterable<? extends Object> values) {

			Assert.notNull(values, "Values must not be null");

			return add(new AddToOp(columnName, values, Mode.APPEND));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#prependAll(java.lang.Object[])
		 */
		@Override
		public Update prependAll(Object... values) {

			Assert.notNull(values, "Values must not be null");

			return prependAll(Arrays.asList(values));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#prependAll(java.lang.Iterable)
		 */
		@Override
		public Update prependAll(Iterable<? extends Object> values) {

			Assert.notNull(values, "Values must not be null");

			return add(new AddToOp(columnName, values, Mode.PREPEND));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#addAll(java.util.Map)
		 */
		@Override
		public Update addAll(Map<? extends Object, ? extends Object> map) {

			Assert.notNull(map, "Map must not be null");

			return add(new AddToMapOp(columnName, map));
		}
	}

	/**
	 * Builder to associate a single value with a collection at a given index at {@link ColumnName}.
	 *
	 * @author Mark Paluch
	 */
	public interface SetBuilder {

		/**
		 * Create a {@link SetValueBuilder} to set a value at a numeric {@code index}. Used for
		 * {@link com.datastax.driver.core.DataType.Name#LIST} type columns.
		 *
		 * @param index positional index.
		 * @return a {@link SetValueBuilder} to set a value at {@code index}
		 */
		SetValueBuilder atIndex(int index);

		/**
		 * Create a {@link SetValueBuilder} to set a value at {@code index}. Used for
		 * {@link com.datastax.driver.core.DataType.Name#MAP} type columns.
		 *
		 * @param key must not be {@literal null}.
		 * @return a {@link SetValueBuilder} to set a value at {@code index}
		 */
		SetValueBuilder atKey(Object key);
	}

	/**
	 * Builder to associate a single value with a collection at a given index at {@link ColumnName}.
	 *
	 * @author Mark Paluch
	 */
	public interface SetValueBuilder {

		/**
		 * Associate the {@code value} with the collection at {@link ColumnName} with a previously specified index.
		 *
		 * @param value must not be {@literal null}.
		 * @return the {@link Update} object.
		 */
		Update to(Object value);
	}

	/**
	 * Default {@link SetBuilder} implementation.
	 */
	private class DefaultSetBuilder implements SetBuilder {

		private final ColumnName columnName;

		DefaultSetBuilder(ColumnName columnName) {
			this.columnName = columnName;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.SetBuilder#atIndex(int)
		 */
		@Override
		public SetValueBuilder atIndex(int index) {
			return value -> add(new SetAtIndexOp(columnName, index, value));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.SetBuilder#atKey(java.lang.Object)
		 */
		@Override
		public SetValueBuilder atKey(Object key) {

			Assert.notNull(key, "Key must not be null");

			return value -> add(new SetAtKeyOp(columnName, key, value));
		}
	}

	public abstract static class UpdateOp {

		private final ColumnName columnName;

		protected UpdateOp(ColumnName columnName) {
			this.columnName = columnName;
		}

		public ColumnName getColumnName() {
			return columnName;
		}
	}

	/**
	 * Add element(s) to collection operation.
	 */
	public static class AddToOp extends UpdateOp {

		private final Iterable<Object> value;
		private final Mode mode;

		@SuppressWarnings("unchecked")
		public AddToOp(ColumnName columnName, Iterable<? extends Object> value, Mode mode) {

			super(columnName);

			this.value = (Iterable<Object>) value;
			this.mode = mode;
		}

		public Iterable<Object> getValue() {
			return value;
		}

		public Mode getMode() {
			return mode;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {

			if (mode == Mode.PREPEND) {

				return String.format("%s = %s + %s", getColumnName(), serializeToCqlSafely(value), getColumnName());
			}

			return String.format("%s = %s + %s", getColumnName(), getColumnName(), serializeToCqlSafely(value));
		}

		public enum Mode {
			PREPEND, APPEND,
		}
	}

	/**
	 * Add element(s) to Map operation.
	 */
	public static class AddToMapOp extends UpdateOp {

		private final Map<Object, Object> value;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public AddToMapOp(ColumnName columnName, Map<? extends Object, ? extends Object> value) {

			super(columnName);
			this.value = (Map) value;
		}

		public Map<Object, Object> getValue() {
			return value;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s = %s + %s", getColumnName(), getColumnName(), serializeToCqlSafely(value));
		}
	}

	/**
	 * Set operation.
	 */
	public static class SetOp extends UpdateOp {

		private final Object value;

		public SetOp(ColumnName columnName, Object value) {

			super(columnName);
			this.value = value;
		}

		public Object getValue() {
			return value;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s = %s", getColumnName(), serializeToCqlSafely(value));
		}
	}

	/**
	 * Set at index operation.
	 */
	public static class SetAtIndexOp extends SetOp {

		private final int index;

		public SetAtIndexOp(ColumnName columnName, int index, Object value) {

			super(columnName, value);

			Assert.notNull(value, "Value must not be null");

			this.index = index;
		}

		public int getIndex() {
			return index;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.SetOp#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s[%d] = %s", getColumnName(), index, serializeToCqlSafely(getValue()));
		}
	}

	/**
	 * Set at map key operation.
	 */
	public static class SetAtKeyOp extends SetOp {

		private final Object key;
		private final Object value;

		public SetAtKeyOp(ColumnName columnName, Object key, Object value) {

			super(columnName, value);

			Assert.notNull(key, "Key must not be null");

			this.key = key;
			this.value = value;
		}

		public Object getKey() {
			return key;
		}

		@Override
		public Object getValue() {
			return value;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.SetOp#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s[%s] = %s", getColumnName(), serializeToCqlSafely(key), serializeToCqlSafely(getValue()));
		}
	}

	/**
	 * Increment operation.
	 */
	public static class IncrOp extends UpdateOp {

		private final long value;

		public IncrOp(ColumnName columnName, long value) {

			super(columnName);
			this.value = value;
		}

		public long getValue() {
			return value;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s = %s %s %d", getColumnName(), getColumnName(), value > 0 ? "+" : "-", Math.abs(value));
		}
	}

	/**
	 * Remove operation.
	 */
	public static class RemoveOp extends UpdateOp {

		private final Object value;

		public RemoveOp(ColumnName columnName, Object value) {

			super(columnName);

			Assert.notNull(value, "Value must not be null");

			this.value = value;
		}

		public Object getValue() {
			return value;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s = %s - %s", getColumnName(), getColumnName(), serializeToCqlSafely(getValue()));
		}
	}
}
