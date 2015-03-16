package org.springframework.data.cassandra.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Fabio Mendes <fabiojmendes@gmail.com> [Mar 14, 2015]
 *
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface UserDefinedType {

	/**
	 * The name of the UDT; must be a valid CQL identifier or quoted identifier.
	 */
	String value() default "";

	/**
	 * Whether to cause the UDT name to be force-quoted.
	 */
	boolean forceQuote() default false;
}
