/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.support;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;

/**
 * Builder for {@link UserDefinedType}. Intended for internal usage during testing.
 *
 * @author Mark Paluch
 */
public class UserDefinedTypeBuilder {

	private final CqlIdentifier typeName;
	private Map<CqlIdentifier, DataType> fields = new LinkedHashMap<>();

	private UserDefinedTypeBuilder(CqlIdentifier typeName) {
		this.typeName = typeName;
	}

	public static UserDefinedTypeBuilder forName(String typeName) {
		return forName(CqlIdentifier.fromCql(typeName));
	}

	private static UserDefinedTypeBuilder forName(CqlIdentifier typeName) {
		return new UserDefinedTypeBuilder(typeName);
	}

	public UserDefinedTypeBuilder withField(String fieldName, DataType dataType) {
		this.fields.put(CqlIdentifier.fromCql(fieldName), dataType);
		return this;
	}

	public UserDefinedType build() {

		DefaultUserDefinedType type = new DefaultUserDefinedType(CqlIdentifier.fromCql("system"), this.typeName, false,
				new ArrayList<>(fields.keySet()), new ArrayList<>(fields.values()));
		return type;
	}

	private static class UserDefinedTypeWrapper implements UserDefinedType {
		private final UserDefinedType delegate;

		private UserDefinedTypeWrapper(UserDefinedType delegate) {
			this.delegate = delegate;
		}

		@Override
		public void attach(@NonNull AttachmentPoint attachmentPoint) {
			throw new UnsupportedOperationException();
		}

		@Override
		@Nullable
		public CqlIdentifier getKeyspace() {
			return null;
		}

		@Override
		@NonNull
		public CqlIdentifier getName() {
			return delegate.getName();
		}

		@Override
		public boolean isFrozen() {
			return delegate.isFrozen();
		}

		@Override
		public boolean isDetached() {
			return delegate.isDetached();
		}

		@Override
		@NonNull
		public List<CqlIdentifier> getFieldNames() {
			return delegate.getFieldNames();
		}

		@Override
		public int firstIndexOf(CqlIdentifier id) {
			return delegate.firstIndexOf(id);
		}

		@Override
		public int firstIndexOf(String name) {
			return delegate.firstIndexOf(name);
		}

		@Override
		@NonNull
		public List<DataType> getFieldTypes() {
			return delegate.getFieldTypes();
		}

		@Override
		@NonNull
		public UserDefinedType copy(boolean newFrozen) {
			return new UserDefinedTypeWrapper(delegate.copy(newFrozen));
		}

		@Override
		@NonNull
		public UdtValue newValue() {
			return delegate.newValue();
		}

		@Override
		@NonNull
		public UdtValue newValue(@NonNull Object... fields) {
			return delegate.newValue(fields);
		}

		@Override
		public int hashCode() {
			return delegate.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj) || delegate.equals(obj);
		}

		@Override
		@NonNull
		public AttachmentPoint getAttachmentPoint() {
			return delegate.getAttachmentPoint();
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [delegate=").append(delegate);
			sb.append(']');
			return sb.toString();
		}
	}
}
