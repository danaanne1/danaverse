package com.ddougher.market.data.extensions

import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.core.Equity.Day
import com.ddougher.market.data.core.Equity.Metric
import com.ddougher.market.data.core.Equity.Year
import java.util.Calendar

/**
 * Given a map record containing a minute level aggregate of the form:
 * <pre>
    {
        "ev": "AM",  // Event Type
        "sym": "GTE", // ticker symbol
        "v": 4110,  // volume
        "vw": 0.4488, // volume weighted avg price
        "o": 0.4488,
        "c": 0.4486,
        "h": 0.4489,
        "l": 0.4486,
        "z": 685,   // avg trade size
        "s": 1610144640000, // start timestamp
        "e": 1610144700000  // end timestamp
    }
 * </pre>
 * This will merge that minute aggregate into the existing equity data.
 */
fun Equity.mergeAggregateData(d: Map<String,Number>) {
    val data = object {
        val o:Double by d
        val h:Double by d
        val l:Double by d
        val c:Double by d
        val v:Long by d
        val vw:Double by d
        val z:Long by d // avg trade size
        val s:Long by d
        val year:Int get() = Calendar.getInstance().apply { timeInMillis = s }.get(Calendar.YEAR)
        val day:Int get() = Calendar.getInstance().apply { timeInMillis = s }.get(Calendar.DAY_OF_YEAR)
    }

    // transactional puts have deduped execution so inside a transaction this will be very efficient

    // metrics is indirect. Adding a metric to an equity requires an equity put.
    val metric = metrics.getOrPut("ohlc_min") { documentStore.newInstance(Metric::class.java).also { documentStore.put(this) }  }

    // year is indirect. Adding a year to a metric requires a metric put
    val year = metric.years.getOrPut( data.year.toString() ) { documentStore.newInstance(Year::class.java).also { documentStore.put(metric) } }

    // day is a member of year. So we never put the day record. When we are done, we put the year record that contains the new day.
    val day = year.days.getOrPut( data.day.toString() ) { documentStore.newInstance(Day::class.java) }

    day.values.binarySearch { numbers -> numbers[0].toLong().compareTo(data.s) }.let { insertPoint ->
        arrayOf<Number>(data.s, data.o, data.h, data.l, data.c, data.v, data.vw, data.z).let {
            if (insertPoint < 0)
                day.values.add((-insertPoint - 1), it)
            else
                day.values[insertPoint] = it
        }
    }
    documentStore.put(year)
}
