/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.config.xml;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;

public class ParsingUtils {

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyValue(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attrName, String defaultValue) {

		addProperty(builder, propertyName, element.getAttribute(attrName), defaultValue, false, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyReference(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attrName, String defaultValue) {

		addProperty(builder, propertyName, element.getAttribute(attrName), defaultValue, false, true);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyValue(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attrName) {

		addProperty(builder, propertyName, element.getAttribute(attrName), null, true, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyReference(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attrName) {

		addProperty(builder, propertyName, element.getAttribute(attrName), null, true, true);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addPropertyValue(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attrName, String defaultValue, boolean required) {

		addProperty(builder, propertyName, element.getAttribute(attrName), defaultValue, required, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addPropertyReference(BeanDefinitionBuilder builder, String propertyName, Element element,
			String attrName, String defaultValue, boolean required) {

		addProperty(builder, propertyName, element.getAttribute(attrName), defaultValue, required, true);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addProperty(BeanDefinitionBuilder builder, String propertyName, Element element, String attrName,
			String defaultValue, boolean required, boolean reference) {

		Assert.notNull(element, "Element must not be null!");
		Assert.hasText(attrName, "Attribute name must not be null!");

		addProperty(builder, propertyName, element.getAttribute(attrName), defaultValue, required, reference);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyValue(BeanDefinitionBuilder builder, String propertyName, Attr attr,
			String defaultValue) {

		addProperty(builder, propertyName, attr, defaultValue, false, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyReference(BeanDefinitionBuilder builder, String propertyName, Attr attr,
			String defaultValue) {

		addProperty(builder, propertyName, attr, defaultValue, false, true);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyValue(BeanDefinitionBuilder builder, String propertyName, Attr attr) {

		addProperty(builder, propertyName, attr, null, true, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyReference(BeanDefinitionBuilder builder, String propertyName, Attr attr) {

		addProperty(builder, propertyName, attr, null, true, true);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addPropertyValue(BeanDefinitionBuilder builder, String propertyName, Attr attr,
			String defaultValue, boolean required) {

		addProperty(builder, propertyName, attr, defaultValue, required, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addPropertyReference(BeanDefinitionBuilder builder, String propertyName, Attr attr,
			String defaultValue, boolean required) {

		addProperty(builder, propertyName, attr, defaultValue, required, true);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addProperty(BeanDefinitionBuilder builder, String propertyName, Attr attr, String defaultValue,
			boolean required, boolean reference) {

		Assert.notNull(attr, "Attr must not be null!");

		addProperty(builder, propertyName, attr.getValue(), defaultValue, required, reference);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyValue(BeanDefinitionBuilder builder, String propertyName, String value) {

		addProperty(builder, propertyName, value, null, true, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addRequiredPropertyReference(BeanDefinitionBuilder builder, String propertyName, String value) {

		addProperty(builder, propertyName, value, null, true, true);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyValue(BeanDefinitionBuilder builder, String propertyName, String value,
			String defaultValue) {

		addProperty(builder, propertyName, value, defaultValue, false, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addOptionalPropertyReference(BeanDefinitionBuilder builder, String propertyName, String value,
			String defaultValue) {

		addProperty(builder, propertyName, value, defaultValue, false, true);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addPropertyValue(BeanDefinitionBuilder builder, String propertyName, String value,
			String defaultValue, boolean required) {

		addProperty(builder, propertyName, value, defaultValue, required, false);
	}

	/**
	 * Convenience method that ultimately delegates to
	 * {@link #addProperty(BeanDefinitionBuilder, String, Element, String, String, boolean, boolean)}.
	 */
	public static void addPropertyReference(BeanDefinitionBuilder builder, String propertyName, String value,
			String defaultValue, boolean required) {

		addProperty(builder, propertyName, value, defaultValue, required, true);
	}

	/**
	 * Adds the named property as a value or reference to the given {@link BeanDefinitionBuilder}, with an optional
	 * default value.
	 * <p/>
	 * Note: If <code>required</code> is <code>false</code>, <code>value</code> is null or empty, and
	 * <code>defaultValue</code> is null or empty, then no property is added and this method silently returns.
	 * 
	 * @param builder The {@link BeanDefinitionBuilder}; must not be null.
	 * @param propertyName The name of the property being added; must not be null or empty.
	 * @param value The value of the property being added; may be null.
	 * @param defaultValue The default value of the property being set.
	 * @param required If <code>true</code>, then the <code>value</code> parameter must not be null or empty. If
	 *          <code>false</code>, the <code>value</code> parameter may be null, in which case the
	 *          <code>defaultValue</code> is used. If <code>required</code> is <code>false</code>, <code>value</code> is
	 *          null or empty, and <code>defaultValue</code> is null or empty, then no property is added and this method
	 *          silently returns.
	 * @param reference If <code>true</code>, this method will add the property as a reference, else as a value.
	 * @see BeanDefinitionBuilder#addPropertyReference(String, String)
	 * @see BeanDefinitionBuilder#addPropertyValue(String, Object)
	 */
	public static void addProperty(BeanDefinitionBuilder builder, String propertyName, String value, String defaultValue,
			boolean required, boolean reference) {

		Assert.notNull(builder, "BeanDefinitionBuilder must not be null!");
		Assert.hasText(propertyName, "Property name must not be null!");

		if (!StringUtils.hasText(value)) {
			if (required) {
				throw new IllegalStateException(String.format("value required for property %s [%s] on class [%s]",
						reference ? "reference" : "", propertyName, builder.getBeanDefinition().getClass().getName()));
			}
			// else optional; use default
			if (defaultValue != null) {
				value = defaultValue;
			} else { // no default value given; quietly ignore & return
				return;
			}
		}

		if (reference) {
			builder.addPropertyReference(propertyName, value);
		} else {
			builder.addPropertyValue(propertyName, value);
		}
	}

	/**
	 * Returns the {@link BeanDefinition} built by the given {@link BeanDefinitionBuilder} enriched with source
	 * information derived from the given {@link Element}.
	 * 
	 * @param builder must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param element must not be {@literal null}.
	 * @return
	 */
	public static AbstractBeanDefinition getSourceBeanDefinition(BeanDefinitionBuilder builder, ParserContext context,
			Element element) {

		Assert.notNull(element, "Element must not be null!");
		Assert.notNull(context, "ParserContext must not be null!");

		return getSourceBeanDefinition(builder, context.extractSource(element));
	}

	/**
	 * Returns the {@link AbstractBeanDefinition} built by the given builder with the given extracted source applied.
	 * 
	 * @param builder must not be {@literal null}.
	 * @param source
	 * @return
	 */
	public static AbstractBeanDefinition getSourceBeanDefinition(BeanDefinitionBuilder builder, Object source) {

		Assert.notNull(builder, "Builder must not be null!");

		AbstractBeanDefinition definition = builder.getRawBeanDefinition();
		definition.setSource(source);
		return definition;
	}
}
