package com.ddougher.market.data.extensions

import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.core.Equity.Day
import com.ddougher.market.data.core.Equity.Metric
import com.ddougher.market.data.core.Equity.Year
import com.theunknowablebits.proxamic.DocumentView
import java.util.Calendar

/**
 * Given a map record containing a minute level aggregate of the form:
 * <pre>
 *  {
 *       "c": 75.0875,
 *       "h": 75.15,
 *       "l": 73.7975,
 *       "n": 1,
 *       "o": 74.06,
 *       "t": 1577941200000,
 *       "v": 135647456,
 *       "vw": 74.6099
 *  }
 * </pre>
 * This will merge that minute aggregate into the existing equity data.
 */
fun Equity.mergeAggregateData(d: Map<String,Number>) {
    val data = object {
        val c:Double by d
        val h:Double by d
        val l:Double by d
        val n:Long by d
        val o:Double by d
        val t:Long by d
        val v:Long by d
        val vw:Double by d
        val year:Int get() = Calendar.getInstance().apply { timeInMillis = t }.get(Calendar.YEAR)
        val day:Int get() = Calendar.getInstance().apply { timeInMillis = t }.get(Calendar.DAY_OF_YEAR)
    }

    // transactional puts have deduped execution so inside a transaction this will be very efficient

    // metrics is indirect. Adding a metric to an equity requires an equity put.
    val metric = metrics.getOrPut("ohlc") { documentStore.newInstance(Metric::class.java).also { documentStore.put(this) }  }

    // year is indirect. Adding a year to a metric requires a metric put
    val year = metric.years.getOrPut( data.year.toString() ) { documentStore.newInstance(Year::class.java).also { documentStore.put(metric) } }

    // day is a member of year. So we never put the day record. When we are done, we put the year record that contains the new day.
    val day = year.days.getOrPut( data.day.toString() ) { documentStore.newInstance(Day::class.java) }

    day.values.binarySearch { numbers -> numbers[5].toLong().compareTo(data.t) }.let { insertPoint ->
        arrayOf<Number>(data.c, data.h, data.l, data.n, data.o, data.t, data.v, data.vw).let {
            if (insertPoint < 0)
                day.values.add((-insertPoint - 1), it)
            else
                day.values[insertPoint] = it
        }
    }

    documentStore.put(year)
}
