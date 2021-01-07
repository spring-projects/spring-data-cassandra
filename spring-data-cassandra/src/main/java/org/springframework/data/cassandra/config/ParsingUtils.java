/*
 * Copyright 2013-2021 the original author or authors.
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
package org.springframework.data.cassandra.config;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;

/**
 * Utility class for parsing Cassandra XML namespace configuration meta-data.
 *
 * @author John Blum
 * @author Mark Paluch
 */
abstract class ParsingUtils {

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyReference(BeanDefinitionBuilder builder, String propertyName, Attr attribute,
			String defaultValue) {

		addProperty(builder, propertyName, attribute.getValue(), defaultValue, false, true);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyReference(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attributeName) {

		addProperty(builder, propertyName, element.getAttribute(attributeName), null, false, true);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyReference(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attributeName, String defaultValue) {

		addProperty(builder, propertyName, element.getAttribute(attributeName), defaultValue, false, true);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyValue(BeanDefinitionBuilder builder, String propertyName, Attr attribute,
			String defaultValue) {

		addProperty(builder, propertyName, attribute.getValue(), defaultValue, false, false);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyValue(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attributeName) {

		addProperty(builder, propertyName, element.getAttribute(attributeName), null, false, false);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyValue(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attributeName, @Nullable String defaultValue) {

		addProperty(builder, propertyName, element.getAttribute(attributeName), defaultValue, false, false);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyReference(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attributeName) {

		addProperty(builder, propertyName, element.getAttribute(attributeName), null, true, true);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyReference(BeanDefinitionBuilder builder, String propertyName, String value) {

		addProperty(builder, propertyName, value, null, true, true);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyValue(BeanDefinitionBuilder builder, String propertyName, Attr attribute) {

		addProperty(builder, propertyName, attribute.getValue(), null, true, false);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyValue(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attributeName) {

		addProperty(builder, propertyName, element.getAttribute(attributeName), null, true, false);
	}

	/**
	 * Convenience method delegating to
	 * {@link #addProperty(BeanDefinitionBuilder, String, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyValue(BeanDefinitionBuilder builder, String propertyName, String value) {

		addProperty(builder, propertyName, value, null, true, false);
	}

	/**
	 * Adds the named property and value, or reference to the given {@link BeanDefinitionBuilder} with an optional default
	 * value if the value has not been specified.
	 * <p/>
	 * If {@code required} is <code>false</code>, <code>value</code> is null or empty, and {@code defaultValue} is null or
	 * empty, then no property is added to the bean definition and this method silently returns.
	 *
	 * @param builder {@link BeanDefinitionBuilder} used to build the {@link BeanDefinition}.
	 * @param propertyName name of the property to add.
	 * @param value value for the property being added.
	 * @param defaultValue default value for the property if value is null or empty.
	 * @param required If {@code true}, then <code>value</code> must not be null or empty. If <code>false</code>, then
	 *          {@code value} may be null and the <code>defaultValue</code> will be used. If <code>required</code> is
	 *          {@code false}, <code>value</code> is null or empty, and <code>defaultValue</code> is null or empty, then
	 *          no property is added to the bean definition and this method silently returns.
	 * @param reference If {@code true}, then the <code>value</code>value for the named property is considered a reference
	 *          to another bean in the Spring context.
	 * @return the given {@link BeanDefinitionBuilder}.
	 * @throws IllegalArgumentException if either the {@link BeanDefinitionBuilder} is null or the {@code propertyName}
	 *           has not been specified.
	 * @see BeanDefinitionBuilder#addPropertyReference(String, String)
	 * @see BeanDefinitionBuilder#addPropertyValue(String, Object)
	 */
	public static BeanDefinitionBuilder addProperty(BeanDefinitionBuilder builder, String propertyName, String value,
			@Nullable String defaultValue, boolean required, boolean reference) {

		Assert.notNull(builder, "BeanDefinitionBuilder must not be null");
		Assert.hasText(propertyName, "Property name must not be null");

		if (!StringUtils.hasText(value)) {
			if (required) {
				throw new IllegalArgumentException(String.format("value required for property %1$s[%2$s] on class [%3$s]",
						reference ? "reference " : "", propertyName, builder.getRawBeanDefinition().getBeanClassName()));
			} else {
				value = defaultValue;
			}
		}

		if (StringUtils.hasText(value)) {
			if (reference) {
				builder.addPropertyReference(propertyName, value);
			} else {
				builder.addPropertyValue(propertyName, value);
			}
		}

		return builder;
	}

	/**
	 * Returns a {@link BeanDefinition} built from the given {@link BeanDefinitionBuilder} enriched with source meta-data
	 * derived from the given {@link Element}.
	 *
	 * @param builder {@link BeanDefinitionBuilder} used to build the {@link BeanDefinition}.
	 * @param parserContext {@link ParserContext} used to track state during the parsing operation.
	 * @param element DOM {@link Element} defining the meta-data that is the source of the {@link BeanDefinition}s
	 *          configuration.
	 * @return the {@link BeanDefinition} built by the given {@link BeanDefinitionBuilder}.
	 * @throws IllegalArgumentException if the {@link BeanDefinitionBuilder} or {@link ParserContext} are null.
	 */
	public static AbstractBeanDefinition getSourceBeanDefinition(BeanDefinitionBuilder builder,
			ParserContext parserContext, Element element) {

		Assert.notNull(parserContext, "ParserContext must not be null");

		return getSourceBeanDefinition(builder, parserContext.extractSource(element));
	}

	/**
	 * Returns a {@link AbstractBeanDefinition} built from the given {@link BeanDefinitionBuilder} with the given
	 * extracted source applied.
	 *
	 * @param builder {@link BeanDefinitionBuilder} used to build the {@link BeanDefinition}.
	 * @param source source meta-data used by the builder to construct the {@link BeanDefinition}.
	 * @return a raw {@link BeanDefinition} built by the given {@link BeanDefinitionBuilder}.
	 * @throws IllegalArgumentException if {@link BeanDefinitionBuilder} is null.
	 */
	public static AbstractBeanDefinition getSourceBeanDefinition(BeanDefinitionBuilder builder, Object source) {

		Assert.notNull(builder, "BeanDefinitionBuilder must not be null");

		AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();

		beanDefinition.setSource(source);

		return beanDefinition;
	}
}
