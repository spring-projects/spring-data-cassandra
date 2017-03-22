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
package org.springframework.data.cassandra.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.core.query.Update.AddToMapOp;
import org.springframework.data.cassandra.core.query.Update.AddToOp;
import org.springframework.data.cassandra.core.query.Update.IncrOp;
import org.springframework.data.cassandra.core.query.Update.RemoveOp;
import org.springframework.data.cassandra.core.query.Update.SetAtIndexOp;
import org.springframework.data.cassandra.core.query.Update.SetAtKeyOp;
import org.springframework.data.cassandra.core.query.Update.SetOp;
import org.springframework.data.cassandra.core.query.Update.UpdateOp;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;

/**
 * Map {@link org.springframework.data.cassandra.core.query.Update} to CQL-specific data types.
 *
 * @author Mark Paluch
 */
public class UpdateMapper extends QueryMapper {

	private final CassandraConverter converter;

	private final CassandraMappingContext mappingContext;

	/**
	 * Creates a new {@link UpdateMapper} with the given {@link CassandraConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public UpdateMapper(CassandraConverter converter) {

		super(converter);

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
	}

	/**
	 * Map a {@link Update} with a {@link CassandraPersistentEntity type hint}. Update mapping translates property names
	 * to column names and maps {@link org.springframework.data.cassandra.core.query.Update.UpdateOp update operation}
	 * values to simple Cassandra values.
	 *
	 * @param update must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the mapped {@link Filter}.
	 */
	public Update getMappedObject(Update update, CassandraPersistentEntity<?> entity) {

		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		Collection<UpdateOp> updateOperations = update.getUpdateOperations();
		List<UpdateOp> mapped = new ArrayList<>(updateOperations.size());

		for (UpdateOp updateOp : updateOperations) {

			Field field = createPropertyField(entity, updateOp.getColumnName());

			mapped.add(getMappedUpdateOperation(updateOp, field));
		}

		return new Update(mapped);
	}

	protected UpdateOp getMappedUpdateOperation(UpdateOp updateOp, Field field) {

		if (updateOp instanceof SetOp) {
			return getMappedUpdateOperation(field, (SetOp) updateOp);
		}

		if (updateOp instanceof RemoveOp) {
			return getMappedUpdateOperation(field, (RemoveOp) updateOp);
		}

		if (updateOp instanceof IncrOp) {
			return new IncrOp(field.getMappedKey(), ((IncrOp) updateOp).getValue());
		}

		if (updateOp instanceof AddToOp) {
			return getMappedUpdateOperation(field, (AddToOp) updateOp);
		}

		if (updateOp instanceof AddToMapOp) {
			return getMappedUpdateOperation(field, (AddToMapOp) updateOp);
		}

		throw new IllegalArgumentException(String.format("UpdateOp %s not supported", updateOp));
	}

	private UpdateOp getMappedUpdateOperation(Field field, SetOp updateOp) {

		TypeInformation<?> typeInformation = field.getProperty() == null ? null : field.getProperty().getTypeInformation();

		if (updateOp instanceof SetAtIndexOp) {

			SetAtIndexOp op = (SetAtIndexOp) updateOp;

			Object mappedValue = converter.convertToCassandraColumn(Optional.ofNullable(op.getValue()), typeInformation);
			return new SetAtIndexOp(field.getMappedKey(), op.getIndex(), mappedValue);
		}

		if (updateOp instanceof SetAtKeyOp) {

			SetAtKeyOp op = (SetAtKeyOp) updateOp;

			TypeInformation<?> keyType = typeInformation == null ? null : typeInformation.getActualType();
			TypeInformation<?> valueType = typeInformation == null ? null : typeInformation.getMapValueType().orElse(null);

			Object mappedKey = converter.convertToCassandraColumn(Optional.ofNullable(op.getKey()), keyType);
			Object mappedValue = converter.convertToCassandraColumn(Optional.ofNullable(op.getValue()), valueType);

			return new SetAtKeyOp(field.getMappedKey(), mappedKey, mappedValue);
		}

		if (updateOp.getValue() instanceof Collection && typeInformation != null && typeInformation.isCollectionLike()) {

			Collection<?> collection = (Collection) updateOp.getValue();

			if (collection.isEmpty()) {

				DataType dataType = mappingContext.getDataType(field.getProperty());
				if (dataType.getName() == Name.SET) {
					return new SetOp(field.getMappedKey(), Collections.emptySet());
				}

				return new SetOp(field.getMappedKey(), Collections.emptyList());
			}
		}

		Object mappedValue = converter.convertToCassandraColumn(Optional.ofNullable(updateOp.getValue()), typeInformation);
		return new SetOp(field.getMappedKey(), mappedValue);
	}

	private UpdateOp getMappedUpdateOperation(Field field, RemoveOp updateOp) {

		TypeInformation<?> typeInformation = field.getProperty() == null ? null : field.getProperty().getTypeInformation();

		Object mappedValue = converter.convertToCassandraColumn(Optional.ofNullable(updateOp.getValue()), typeInformation);
		return new RemoveOp(field.getMappedKey(), mappedValue);
	}

	@SuppressWarnings("unchecked")
	private UpdateOp getMappedUpdateOperation(Field field, AddToOp updateOp) {

		CassandraPersistentProperty property = field.getProperty();
		TypeInformation<?> typeInformation = property == null ? null : property.getTypeInformation();

		Collection<Object> mappedValue = (Collection) converter
				.convertToCassandraColumn(Optional.ofNullable(updateOp.getValue()), typeInformation).orElse(null);

		if (property != null) {

			DataType dataType = mappingContext.getDataType(property);
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

	private UpdateOp getMappedUpdateOperation(Field field, AddToMapOp updateOp) {

		TypeInformation<?> typeInformation = field.getProperty() == null ? null : field.getProperty().getTypeInformation();
		TypeInformation<?> keyType = typeInformation == null ? null : typeInformation.getActualType();
		TypeInformation<?> valueType = typeInformation == null ? null : typeInformation.getMapValueType().orElse(null);

		Map<Object, Object> result = new LinkedHashMap<>(updateOp.getValue().size(), 1);

		updateOp.getValue().forEach((k, v) -> {

			Object mappedKey = converter.convertToCassandraColumn(Optional.ofNullable(k), keyType);
			Object mappedValue = converter.convertToCassandraColumn(Optional.ofNullable(v), valueType);
			result.put(mappedKey, mappedValue);

		});

		return new AddToMapOp(field.getMappedKey(), result);
	}
}
