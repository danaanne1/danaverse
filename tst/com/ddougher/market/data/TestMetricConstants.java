package com.ddougher.market.data;

import static org.junit.jupiter.api.Assertions.*;

import java.text.SimpleDateFormat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestMetricConstants {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void test() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Object [] asserts = { 
				"2023-01-09 04:00", 2023, 5,
				"2023-01-06 04:00", 2023, 4,
				"2023-01-02 04:00", 2023, 0,
				"2022-01-10 04:00", 2022, 5,
				"2022-01-07 04:00", 2022, 4,
				"2022-01-04 04:00", 2022, 1,
				"2022-01-03 04:00", 2022, 0,
				"2017-01-02 04:00", 2017, 0,
		};
		for (int i = asserts.length-1; i >= 0; i-=3) {
			assertEquals((String)asserts[i-2],sdf.format(MetricConstants.calendarDateFromTradingOffset((int)asserts[i-1], (int)asserts[i], 0)));
		}
	}

}
