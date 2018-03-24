/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.core.query.Update.AddToMapOp;
import org.springframework.data.cassandra.core.query.Update.AddToOp;
import org.springframework.data.cassandra.core.query.Update.AssignmentOp;
import org.springframework.data.cassandra.core.query.Update.IncrOp;
import org.springframework.data.cassandra.core.query.Update.RemoveOp;
import org.springframework.data.cassandra.core.query.Update.SetAtIndexOp;
import org.springframework.data.cassandra.core.query.Update.SetAtKeyOp;
import org.springframework.data.cassandra.core.query.Update.SetOp;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;

/**
 * Map {@link org.springframework.data.cassandra.core.query.Update} to CQL-specific data types.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.query.Filter
 * @see org.springframework.data.cassandra.core.query.Update
 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity
 * @see org.springframework.data.mapping.PersistentProperty
 * @see org.springframework.data.util.TypeInformation
 * @since 2.0
 */
public class UpdateMapper extends QueryMapper {

	/**
	 * Creates a new {@link UpdateMapper} with the given {@link CassandraConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public UpdateMapper(CassandraConverter converter) {
		super(converter);
	}

	/**
	 * Map a {@link Update} with a {@link CassandraPersistentEntity type hint}. Update mapping translates property names
	 * to column names and maps {@link AssignmentOp update operation} values to simple Cassandra values.
	 *
	 * @param update must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the mapped {@link Filter}.
	 */
	public Update getMappedObject(Update update, CassandraPersistentEntity<?> entity) {

		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		Collection<AssignmentOp> assignmentOperations = update.getUpdateOperations();
		List<AssignmentOp> mapped = new ArrayList<>(assignmentOperations.size());

		for (AssignmentOp assignmentOp : assignmentOperations) {

			Field field = createPropertyField(entity, assignmentOp.getColumnName());

			field.getProperty().filter(it -> it.getOrdinal() != null).ifPresent(it -> {
				throw new IllegalArgumentException(
						String.format("Cannot reference tuple value elements, property [%s]", field.getMappedKey()));
			});

			mapped.add(getMappedUpdateOperation(assignmentOp, field));
		}

		return Update.of(mapped);
	}

	private AssignmentOp getMappedUpdateOperation(AssignmentOp assignmentOp, Field field) {

		if (assignmentOp instanceof SetOp) {
			return getMappedUpdateOperation(field, (SetOp) assignmentOp);
		}

		if (assignmentOp instanceof RemoveOp) {
			return getMappedUpdateOperation(field, (RemoveOp) assignmentOp);
		}

		if (assignmentOp instanceof IncrOp) {
			return new IncrOp(field.getMappedKey(), ((IncrOp) assignmentOp).getValue());
		}

		if (assignmentOp instanceof AddToOp) {
			return getMappedUpdateOperation(field, (AddToOp) assignmentOp);
		}

		if (assignmentOp instanceof AddToMapOp) {
			return getMappedUpdateOperation(field, (AddToMapOp) assignmentOp);
		}

		throw new IllegalArgumentException(String.format("UpdateOp [%s] not supported", assignmentOp));
	}

	private AssignmentOp getMappedUpdateOperation(Field field, SetOp updateOp) {

		Object rawValue = updateOp.getValue();

		if (updateOp instanceof SetAtKeyOp) {

			SetAtKeyOp op = (SetAtKeyOp) updateOp;

			Assert.state(op.getValue() != null,
					() -> String.format("SetAtKeyOp for %s attempts to set null", field.getProperty()));

			Optional<? extends TypeInformation<?>> typeInformation =
					field.getProperty().map(PersistentProperty::getTypeInformation);

			Optional<TypeInformation<?>> keyType = typeInformation.map(TypeInformation::getComponentType);
			Optional<TypeInformation<?>> valueType = typeInformation.map(TypeInformation::getMapValueType);

			Object mappedKey = keyType.map(typeInfo -> getConverter().convertToColumnType(op.getKey(), typeInfo))
					.orElseGet(() -> getConverter().convertToColumnType(op.getKey()));

			Object mappedValue = valueType.map(typeInfo -> getConverter().convertToColumnType(op.getValue(), typeInfo))
					.orElseGet(() -> getConverter().convertToColumnType(op.getValue()));

			return new SetAtKeyOp(field.getMappedKey(), mappedKey, mappedValue);
		}

		TypeInformation<?> typeInformation = getTypeInformation(field, rawValue);

		if (updateOp instanceof SetAtIndexOp) {

			SetAtIndexOp op = (SetAtIndexOp) updateOp;

			Assert.state(op.getValue() != null,
					() -> String.format("SetAtIndexOp for %s attempts to set null", field.getProperty()));

			Object mappedValue = getConverter().convertToColumnType(op.getValue(), typeInformation);

			return new SetAtIndexOp(field.getMappedKey(), op.getIndex(), mappedValue);
		}

		if (rawValue instanceof Collection && typeInformation.isCollectionLike()) {

			Collection<?> collection = (Collection) rawValue;

			if (collection.isEmpty()) {

				DataType.Name dataType = field.getProperty().map(property ->
						getMappingContext().getDataType(property)).map(DataType::getName).orElse(Name.LIST);

				if (dataType == Name.SET) {
					return new SetOp(field.getMappedKey(), Collections.emptySet());
				}

				return new SetOp(field.getMappedKey(), Collections.emptyList());
			}
		}

		Object mappedValue = rawValue == null ? null : getConverter().convertToColumnType(rawValue, typeInformation);

		return new SetOp(field.getMappedKey(), mappedValue);
	}

	private AssignmentOp getMappedUpdateOperation(Field field, RemoveOp updateOp) {

		Object value = updateOp.getValue();
		TypeInformation<?> typeInformation = getTypeInformation(field, value);
		Object mappedValue = getConverter().convertToColumnType(value, typeInformation);

		return new RemoveOp(field.getMappedKey(), mappedValue);
	}

	@SuppressWarnings("unchecked")
	private AssignmentOp getMappedUpdateOperation(Field field, AddToOp updateOp) {

		Iterable<Object> value = updateOp.getValue();
		TypeInformation<?> typeInformation = getTypeInformation(field, value);
		Collection<Object> mappedValue = (Collection) getConverter().convertToColumnType(value, typeInformation);

		if (field.getProperty().isPresent()) {

			DataType dataType = getMappingContext().getDataType(field.getProperty().get());

			if (dataType.getName() == Name.SET && !(mappedValue instanceof Set)) {
				Collection<Object> collection = new HashSet<>();
				collection.addAll(mappedValue);
				mappedValue = collection;
			}

			if (dataType.getName() == Name.LIST && !(mappedValue instanceof List)) {
				Collection<Object> collection = new ArrayList<>();
				collection.addAll(mappedValue);
				mappedValue = collection;
			}
		}

		return new AddToOp(field.getMappedKey(), mappedValue, updateOp.getMode());
	}

	private AssignmentOp getMappedUpdateOperation(Field field, AddToMapOp updateOp) {

		Optional<? extends TypeInformation<?>> typeInformation = field.getProperty()
				.map(PersistentProperty::getTypeInformation);

		Optional<TypeInformation<?>> keyType = typeInformation.map(TypeInformation::getComponentType);
		Optional<TypeInformation<?>> valueType = typeInformation.map(TypeInformation::getMapValueType);

		Map<Object, Object> result = new LinkedHashMap<>(updateOp.getValue().size(), 1);

		updateOp.getValue().forEach((key, value) -> {

			Object mappedKey = keyType.map(typeInfo -> getConverter().convertToColumnType(key, typeInfo))
					.orElseGet(() -> getConverter().convertToColumnType(key));

			Object mappedValue = valueType.map(typeInfo -> getConverter().convertToColumnType(value, typeInfo))
					.orElseGet(() -> getConverter().convertToColumnType(value));

			result.put(mappedKey, mappedValue);
		});

		return new AddToMapOp(field.getMappedKey(), result);
	}
}
