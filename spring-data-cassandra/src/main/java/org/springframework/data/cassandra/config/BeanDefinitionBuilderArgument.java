package org.springframework.data.cassandra.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.util.StringUtils;

/**
 * Parameter used in conjunction with:
 * <p/>
 * {@link BeanDefinitionBuilder#addConstructorArgReference(String)},
 * {@link BeanDefinitionBuilder#addConstructorArgValue(Object)},
 * {@link BeanDefinitionBuilder#addPropertyReference(String, String)}, and
 * {@link BeanDefinitionBuilder#addPropertyValue(String, Object)}.
 * <p/>
 * Easy and succinct to create if methods {@link #ref(CharSequence)} or {@link #val(Object)} are used and imported
 * statically.
 * 
 * @see BeanDefinitionBuilderArgument#ref(CharSequence)
 * @see BeanDefinitionBuilderArgument#val(Object)
 */
public class BeanDefinitionBuilderArgument {

	/**
	 * Returns a {@link BeanDefinitionBuilderArgument} with {@link #reference} equal to <code>true</code>. Convenient if
	 * imported statically.
	 * 
	 * @param value The name of the bean reference.
	 */
	public static BeanDefinitionBuilderArgument ref(CharSequence value) {
		return new BeanDefinitionBuilderArgument(true, value);
	}

	/**
	 * Returns a {@link BeanDefinitionBuilderArgument} with {@link #reference} equal to <code>false</code>. Convenient if
	 * imported statically.
	 * 
	 * @param value The constructor argument's value.
	 */
	public static BeanDefinitionBuilderArgument val(Object value) {
		return new BeanDefinitionBuilderArgument(false, value);
	}

	protected boolean reference;
	protected Object value;

	protected BeanDefinitionBuilderArgument(boolean reference, Object value) {
		this.reference = reference;
		if (this.reference && (value == null || !(value instanceof CharSequence))) {
			throw new IllegalArgumentException(String.format(
					"reference argument must have value of type CharSequence, not [%s]", value == null ? "null" : value
							.getClass().getName()));
		}
		if (!StringUtils.hasText((CharSequence) value)) {
			throw new IllegalArgumentException("given CharSequence has no text");
		}
		this.value = value;
	}

	public boolean isReference() {
		return reference;
	}

	public Object getValue() {
		return value;
	}
}