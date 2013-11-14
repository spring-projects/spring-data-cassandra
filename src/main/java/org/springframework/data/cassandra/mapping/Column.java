package org.springframework.data.cassandra.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to define custom metadata for document fields.
 * 
 * @author Alex Shvid
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

	/**
	 * The name of the column in the table.
	 * 
	 * @return
	 */
	String value() default "";

}
