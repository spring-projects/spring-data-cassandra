/*
 * Copyright 2016-2019 the original author or authors.
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
package org.springframework.data.cassandra.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

/**
 * @author Mark Paluch
 */
@Table
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person {

	@Id String id;
	String firstname;
	String lastname;

	private Kindness kindness;

	public enum Kindness {

		Nice("+"), Rude("-");

		private final String identifier;

		Kindness(String identifier) {
			this.identifier = identifier;
		}

		public String getIdentifier() {
			return identifier;
		}
	}

	@WritingConverter
	public enum KindnessToStringConverter implements Converter<Kindness, String> {

		INSTANCE;

		@Override
		public String convert(Kindness source) {
			return source.getIdentifier();
		}
	}

	@ReadingConverter
	public enum StringToKindnessConverter implements Converter<String, Kindness> {

		INSTANCE;

		@Override
		public Kindness convert(String source) {
			return "+".equals(source) ? Kindness.Nice : Kindness.Rude;
		}
	}
}
