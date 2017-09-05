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

package org.springframework.data.cassandra.util;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Unit tests for class {@link SpelUtils org.springframework.data.cassandra.util.SpelUtils}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class SpelUtilsTest {


	@Test  //DATACASS-405
	public void testEvaluateProvidingNullContext() throws Exception {

		SpelExpressionParser spelExpressionParser = SpelUtils.DEFAULT_PARSER;
		String result = SpelUtils.evaluate("cellEditor", null, spelExpressionParser);

		assertThat(result).isEqualTo("cellEditor");
	}


	@Test  //DATACASS-405
	public void testEvaluateProvidingEmptyCharSequenceAndNullContext() throws Exception {

		SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
		String result = SpelUtils.evaluate("", null, spelExpressionParser);

		assertThat(result).isEmpty();
	}


	@Test  //DATACASS-405
	public void testEvaluateProvidingAnOwnExpressionParser() throws Exception {

		SpelParserConfiguration spelParserConfiguration = new SpelParserConfiguration(false, false);
		SpelExpressionParser spelExpressionParser = new SpelExpressionParser(spelParserConfiguration);
		String result = SpelUtils.evaluate("+18:00", null, String.class, spelExpressionParser);

		assertThat(spelParserConfiguration.isAutoGrowNullReferences()).isFalse();
		assertThat(spelParserConfiguration.isAutoGrowCollections()).isFalse();

		assertThat(spelParserConfiguration.getCompilerMode()).isEqualTo(SpelCompilerMode.OFF);
		assertThat(spelParserConfiguration.getMaximumAutoGrowSize()).isEqualTo(Integer.MAX_VALUE);

		assertThat(result).isEqualTo("+18:00");
	}


	@Test  //DATACASS-405
	public void testProvidingNullEvaluationContextOne() throws Exception {

		assertThat(SpelUtils.evaluate("caret-begin-word", null, Object.class)).isEqualTo("caret-begin-word");
	}


	@Test  //DATACASS-405
	public void testProvidingNullEvaluationContextTwo() throws Exception {

		assertThat(SpelUtils.evaluate("", null)).isEmpty();
	}


	@Test  //DATACASS-405
	public void testProvidingNullEvaluationContextThree() throws Exception {

		assertThat(SpelUtils.evaluate("caret-begin-word", null)).isEqualTo("caret-begin-word");
	}
}