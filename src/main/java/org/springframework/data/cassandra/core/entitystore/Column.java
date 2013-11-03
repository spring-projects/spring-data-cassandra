package org.springframework.data.cassandra.core.entitystore;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to define custom metadata for ColumnFamily Columns.
 * 
 * @author David Webb
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
	
    /**
     * (Optional) The name of the column. Defaults to 
     * the property or field name.
     */
    String name() default "";

    /**
     * (Optional) Whether the database column is nullable.
     */
    boolean nullable() default true;
    
	/**
	 * The key to be used to store the field inside the document.
	 * 
	 * @return
	 */
	String value() default "";

	/**
	 * The order in which various fields shall be stored. Has to be a positive integer.
	 * 
	 * @return the order the field shall have in the document or -1 if undefined.
	 */
	int order() default Integer.MAX_VALUE;
}
