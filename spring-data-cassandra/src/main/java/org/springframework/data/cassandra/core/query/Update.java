/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.cassandra.core.query;

import static org.springframework.data.cassandra.core.query.SerializationUtils.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.cassandra.core.query.Update.AddToOp.Mode;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Update object representing representing a set of update operations. {@link Update} objects can be created in a fluent
 * style. Each construction operation creates a new immutable {@link Update} object.
 *
 * <pre class="code">
 * Update update = Update.empty().set("foo", "bar").addTo("baz").prependAll(listOfValues);
 * </pre>
 *
 * @author Mark Paluch
 * @author Chema Vinacua
 * @since 2.0
 */
public class Update {

	private static final Update EMPTY = new Update(Collections.emptyMap());

	private final Map<ColumnName, AssignmentOp> updateOperations;

	/**
	 * Create an empty {@link Update} object.
	 *
	 * @return a new, empty {@link Update}.
	 */
	public static Update empty() {
		return EMPTY;
	}

	/**
	 * Create a {@link Update} object given a list of {@link AssignmentOp}s.
	 *
	 * @param assignmentOps must not be {@literal null}.
	 */
	public static Update of(Iterable<AssignmentOp> assignmentOps) {

		Assert.notNull(assignmentOps, "Update operations must not be null");

		Map<ColumnName, AssignmentOp> updateOperations = assignmentOps instanceof Collection
				? new LinkedHashMap<>(((Collection<?>) assignmentOps).size())
				: new LinkedHashMap<>();

		assignmentOps.forEach(assignmentOp -> updateOperations.put(assignmentOp.getColumnName(), assignmentOp));

		return new Update(updateOperations);
	}

	/**
	 * Set the {@code columnName} to {@code value}.
	 *
	 * @return a new {@link Update}.
	 */
	public static Update update(String columnName, @Nullable Object value) {
		return empty().set(columnName, value);
	}

	private Update(Map<ColumnName, AssignmentOp> updateOperations) {
		this.updateOperations = updateOperations;
	}

	/**
	 * Set the {@code columnName} to {@code value}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @param value value to set on column with name, may be {@literal null}.
	 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
	 *         assignment.
	 */
	public Update set(String columnName, @Nullable Object value) {
		return add(new SetOp(ColumnName.from(columnName), value));
	}

	/**
	 * Create a new {@link SetBuilder} to set a collection item for {@code columnName} in a fluent style.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link AddToBuilder} to build an set assignment.
	 */
	public SetBuilder set(String columnName) {
		return new DefaultSetBuilder(ColumnName.from(columnName));
	}

	/**
	 * Create a new {@link AddToBuilder} to add items to a collection for {@code columnName} in a fluent style.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link AddToBuilder} to build an add-to assignment.
	 */
	public AddToBuilder addTo(String columnName) {
		return new DefaultAddToBuilder(ColumnName.from(columnName));
	}

	/**
	 * Remove {@code value} from the collection at {@code columnName}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
	 *         assignment.
	 */
	public Update remove(String columnName, Object value) {
		return add(new RemoveOp(ColumnName.from(columnName), Collections.singletonList(value)));
	}

	/**
	 * Cleat the collection at {@code columnName}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
	 *         assignment.
	 */
	public Update clear(String columnName) {
		return add(new SetOp(ColumnName.from(columnName), Collections.emptyList()));
	}

	/**
	 * Increment the value at {@code columnName} by {@literal 1}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
	 *         assignment.
	 */
	public Update increment(String columnName) {
		return increment(columnName, 1);
	}

	/**
	 * Increment the value at {@code columnName} by {@code delta}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @param delta increment value.
	 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
	 *         assignment.
	 */
	public Update increment(String columnName, Number delta) {
		return add(new IncrOp(ColumnName.from(columnName), delta));
	}

	/**
	 * Decrement the value at {@code columnName} by {@literal 1}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
	 *         assignment.
	 */
	public Update decrement(String columnName) {
		return decrement(columnName, 1);
	}

	/**
	 * Decrement the value at {@code columnName} by {@code delta}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @param delta decrement value.
	 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
	 *         assignment.
	 */
	public Update decrement(String columnName, Number delta) {

		if (delta instanceof Integer || delta instanceof Long) {

			long deltaValue = delta.longValue() > 0 ? -Math.abs(delta.longValue()) : delta.longValue();
			return add(new IncrOp(ColumnName.from(columnName), deltaValue));
		}

		double deltaValue = delta.doubleValue();

		deltaValue = deltaValue > 0 ? -Math.abs(deltaValue) : deltaValue;

		return add(new IncrOp(ColumnName.from(columnName), deltaValue));
	}

	/**
	 * @return {@link Collection} of update operations.
	 */
	public Collection<AssignmentOp> getUpdateOperations() {
		return Collections.unmodifiableCollection(updateOperations.values());
	}

	private Update add(AssignmentOp assignmentOp) {

		Map<ColumnName, AssignmentOp> map = new LinkedHashMap<>(this.updateOperations.size() + 1);

		map.putAll(this.updateOperations);
		map.put(assignmentOp.getColumnName(), assignmentOp);

		return new Update(map);
	}

	/*
	 * (non-Javadoc)
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
	@SuppressWarnings("unused")
	public interface AddToBuilder {

		/**
		 * Prepend the {@code value} to the collection.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
		 *         assignment.
		 */
		Update prepend(Object value);

		/**
		 * Prepend all {@code values} to the collection.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
		 *         assignment.
		 */
		Update prependAll(Object... values);

		/**
		 * Prepend all {@code values} to the collection.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
		 *         assignment.
		 */
		Update prependAll(Iterable<? extends Object> values);

		/**
		 * Append the {@code value} to the collection.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
		 *         assignment.
		 */
		Update append(Object value);

		/**
		 * Append all {@code values} to the collection.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
		 *         assignment.
		 */
		Update appendAll(Object... values);

		/**
		 * Append all {@code values} to the collection.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
		 *         assignment.
		 */
		Update appendAll(Iterable<? extends Object> values);

		/**
		 * Associate the specified {@code value} with the specified {@code key} in the map.
		 *
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
		 *         assignment.
		 */
		Update entry(Object key, Object value);

		/**
		 * Associate all entries of the specified {@code map} with the map at {@link ColumnName}.
		 *
		 * @param map must not be {@literal null}.
		 * @return a new {@link Update} object containing the merge result of the existing assignments and the current
		 *         assignment.
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
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#append(java.lang.Object)
		 */
		@Override
		public Update append(Object value) {
			return appendAll(Collections.singleton(value));
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
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#entry(java.lang.Object, java.lang.Object)
		 */
		@Override
		public Update entry(Object key, Object value) {

			Assert.notNull(key, "Key must not be null");
			Assert.notNull(value, "Value must not be null");

			return addAll(Collections.singletonMap(key, value));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.AddToBuilder#addAll(java.util.Map)
		 */
		@Override
		public Update addAll(Map<?, ?> map) {

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
		 * {@link com.datastax.oss.driver.api.core.type.ListType} type columns.
		 *
		 * @param index positional index.
		 * @return a {@link SetValueBuilder} to set a value at {@code index}
		 */
		SetValueBuilder atIndex(int index);

		/**
		 * Create a {@link SetValueBuilder} to set a value at {@code index}. Used for
		 * {@link com.datastax.oss.driver.api.core.type.MapType} type columns.
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
			return value -> add(new SetAtIndexOp(this.columnName, index, value));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Update.SetBuilder#atKey(java.lang.Object)
		 */
		@Override
		public SetValueBuilder atKey(Object key) {
			return value -> add(new SetAtKeyOp(this.columnName, key, value));
		}
	}

	/**
	 * Abstract class for an update assignment related to a specific {@link ColumnName}.
	 */
	public abstract static class AssignmentOp {

		private final ColumnName columnName;

		protected AssignmentOp(ColumnName columnName) {
			this.columnName = columnName;
		}

		/**
		 * @return the {@link ColumnName}.
		 */
		public ColumnName getColumnName() {
			return this.columnName;
		}

		/**
		 * @return the {@link ColumnName}.
		 */
		public CqlIdentifier toCqlIdentifier() {
			return this.columnName.getCqlIdentifier().orElseGet(() -> CqlIdentifier.fromCql(this.columnName.toCql()));
		}
	}

	/**
	 * Add element(s) to collection operation.
	 */
	public static class AddToOp extends AssignmentOp {

		private final Iterable<Object> value;
		private final Mode mode;

		@SuppressWarnings("unchecked")
		public AddToOp(ColumnName columnName, Iterable<? extends Object> value, Mode mode) {

			super(columnName);

			this.value = (Iterable<Object>) value;
			this.mode = mode;
		}

		public Iterable<Object> getValue() {
			return this.value;
		}

		public Mode getMode() {
			return this.mode;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {

			return Mode.PREPEND.equals(getMode())
					? String.format("%s = %s + %s", getColumnName(), serializeToCqlSafely(value), getColumnName())
					: String.format("%s = %s + %s", getColumnName(), getColumnName(), serializeToCqlSafely(value));
		}

		public enum Mode {
			PREPEND, APPEND,
		}
	}

	/**
	 * Add element(s) to Map operation.
	 */
	public static class AddToMapOp extends AssignmentOp {

		private final Map<Object, Object> value;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public AddToMapOp(ColumnName columnName, Map<? extends Object, ? extends Object> value) {
			super(columnName);
			this.value = (Map) value;
		}

		public Map<Object, Object> getValue() {
			return value;
		}

		/*
		 * (non-Javadoc)
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
	public static class SetOp extends AssignmentOp {

		private final @Nullable Object value;

		public SetOp(ColumnName columnName, @Nullable Object value) {
			super(columnName);
			this.value = value;
		}

		@Nullable
		public Object getValue() {
			return value;
		}

		/*
		 * (non-Javadoc)
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

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
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

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s[%s] = %s", getColumnName(), serializeToCqlSafely(key), serializeToCqlSafely(getValue()));
		}
	}

	/**
	 * Increment operation.
	 */
	public static class IncrOp extends AssignmentOp {

		private final Number value;

		public IncrOp(ColumnName columnName, Number value) {
			super(columnName);
			this.value = value;
		}

		public Number getValue() {
			return value;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s = %s %s %d", getColumnName(), getColumnName(), value.doubleValue() > 0 ? "+" : "-",
					Math.abs(value.longValue()));
		}
	}

	/**
	 * Remove operation.
	 */
	public static class RemoveOp extends AssignmentOp {

		private final Object value;

		public RemoveOp(ColumnName columnName, Object value) {

			super(columnName);

			Assert.notNull(value, "Value must not be null");

			this.value = value;
		}

		public Object getValue() {
			return value;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("%s = %s - %s", getColumnName(), getColumnName(), serializeToCqlSafely(getValue()));
		}
	}
}
