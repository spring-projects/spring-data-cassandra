/**
 * All BrightMove Code is Copyright 2004-2013 BrightMove Inc.
 * Modification of code without the express written consent of
 * BrightMove, Inc. is strictly forbidden.
 *
 * Author: David Webb (dwebb@brightmove.com)
 * Created On: Nov 12, 2013 
 */
package org.springframework.data.cassandra.core;

import com.datastax.driver.core.Row;

/**
 * Simple internal callback to allow operations on a {@link Row}.
 * 
 * @author Alex Shvid
 */

public interface RowCallback<T> {

	T doWith(Row object);
}
