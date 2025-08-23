package com.ddougher.market.application

import com.ddougher.market.data.core.Stocks
import com.ddougher.documentstore.DocumentStore
import com.ddougher.documentstore.DocumentStoreAware
import com.ddougher.market.data.MetricConstants.Candle
import com.ddougher.market.data.core.Equity
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.Serializable
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.util.*
import kotlin.collections.listOf

data class Swing(val startTimeMs: Long, val endTimeMs: Long, val swingPercent: Double, val vol: Long, val ticker:String): Serializable {
    companion object {
        private const val serialVersionUID: Long = 0L
    }
    override fun toString(): String {
        val sdf = SimpleDateFormat("MM/dd HH:mm")
        val minutes = (endTimeMs-startTimeMs)/60000
        return ("""
            Swing($ticker, $swingPercent, $vol, ${sdf.format(Date(startTimeMs.toLong()))} over $minutes minutes)
        """).trimIndent()
    }
}

class StockCrawler(val documentStore: DocumentStore) {

    suspend fun calculateVWMA10(equity: Equity, dayData: Equity.Day,  metricData: List<Array<Number>>): List<Unit> {
        // ignoring equity and metricData, calculates the VWMA ( volume weighted moving average ) of the day data
        var printed = false
        // step 1: create a sliding window for 11 pairs of (price, volume)
        val slidingWindow = mutableListOf<Pair<Double, Long>>()

        // iterate and replace day data values with a new array containing the VWMA10
        val values = dayData.values
        for (i in 0 until values.size) {
            if (!printed) {
                // println("${equity.symbol} ${SimpleDateFormat("MM/dd/yyyy").format(Date(values[i][Candle.TIME.value].toLong()))}")
                printed = true
            }
            val avgPrice = (values[i][Candle.CLOSE.value].toDouble() + values[i][Candle.LOW.value].toDouble() + values[i][Candle.HIGH.value].toDouble())
            if (avgPrice != 0.0 && values[i][Candle.VOLUME.value].toLong() != 0L)
                slidingWindow.add(Pair(avgPrice/3 , values[i][Candle.VOLUME.value].toLong()))
            while (slidingWindow.size > 10) { slidingWindow.removeAt(0) }
            // copy values to a new longer array
            var newValues = arrayOf<Number>(*values[i])
            while (newValues.size <= Candle.VWMA.value) newValues = arrayOf<Number>(*newValues, 0.0)
            if (slidingWindow.isNotEmpty()) {
                newValues[Candle.VWMA.value]= slidingWindow.sumOf { it.first * it.second } / slidingWindow.sumOf { it.second }
                values[i] = newValues
            }
        }

        try {
            equity.documentStore.put(dayData)
        } catch (e: ConcurrentModificationException) {
            if (printed)
                println("Concurrent modification exception for ${equity.symbol} ${SimpleDateFormat("MM/dd/yyyy").format(Date(values[0][Candle.TIME.value].toLong()))}")
            else
                println("Concurrent modification exception for ${equity.symbol} ")
        }

        return listOf()
    }


    /** find all the stocks that went up by more than 4% in a day */
    suspend fun locateFourPercentSwings(equity: Equity, dayData: Equity.Day,  metricData: List<Array<Number>>): List<Swing> {
        val localLow = LocalTime.of(6,29)
        val localHi = LocalTime.of(13,1)

        val filtered = metricData.filter {
            Instant.ofEpochMilli(it[0] as Long).atZone(ZoneId.systemDefault()).toLocalTime().let {
                it.isAfter(localLow) && it.isBefore(localHi)
            }
        }
        if (filtered.isEmpty()) return listOf()

        val lowCandle = filtered.minBy { it[Candle.LOW.value].toDouble() }
        val highCandle = filtered.maxBy { it[Candle.HIGH.value].toDouble() }

        val lowTime = lowCandle[Candle.TIME.value].toLong()
        val highTime = highCandle[Candle.TIME.value].toLong()
        if (highTime <= lowTime) return listOf()

        val high = highCandle[Candle.HIGH.value].toDouble()
        val low = lowCandle[Candle.LOW.value].toDouble()
        if ((high-low)/low < .04) return listOf()

        val vol = filtered.filter { it[Candle.TIME.value].toLong() >= lowTime && it[Candle.TIME.value].toLong() <= highTime }.sumOf { it[Candle.VOLUME.value].toLong() }

        return listOf(
            Swing(
                lowTime,
                highTime,
                (high-low)/low,
                vol,
                equity.symbol
            )
        )
    }
    
    /** invokes handler for every equity in the list of tickers */
    @Suppress("ControlFlowWithEmptyBody")
    suspend fun <T> visitStocksByTicker(tickers: List<String>, handler: suspend (Equity) -> List<T>): List<T> {
        val stocks = documentStore.get(Stocks::class.java, "stocks")
        val tickersChannel = Channel<String>(100)
        val stockMutex = Mutex()

        return channelFlow {
            repeat(60) {
                launch {
                    while (null != tickersChannel.receiveCatching().getOrNull()?.also { ticker ->
                        val equity = stockMutex.withLock { stocks.tickers()[ticker] }
                        if (equity != null) {
                            send(handler.invoke(equity))
                        }
                    });
                }
            }
            tickers.distinct().forEach { tickersChannel.send(it)}
            tickersChannel.close()
        }.toList().flatten()
    }

    /** Invokes handler for every set of day aggregates for equitiy, beginning from startTime */
    @Suppress("UNCHECKED_CAST")
    suspend fun <T> visitEveryDaysMetricByTicker(tickers: List<String>, startDate: Date, metric:String = "ohlc_min", handler: suspend (Equity, Equity.Day, List<Array<Number>>) -> List<T>): List<T> {
        val calendar = Calendar.getInstance().apply { timeInMillis = startDate.time }
        val startYear = calendar[Calendar.YEAR]
        val startDay = calendar[Calendar.DAY_OF_YEAR]
        return visitStocksByTicker(tickers) { equity ->
            println("Visiting ${equity.symbol}")
            (equity.metrics[metric]?.let { metric ->
                metric.years
                    .filter { (year, _) -> 
                        year.toInt() >= startYear 
                    }.flatMap { (year, yearData) -> 
                        yearData.days.filter { (day, _) -> year.toInt() > startYear || day.toInt() >= startDay
                    }.flatMap { (_, dayData) ->
                        handler.invoke(equity, dayData, dayData.values)
                    }
                }
            } ?: listOf<T>()) as List<T>
        }
    }

    /** invokes handler for a single days metric */
    @Suppress("UNCHECKED_CAST")
    suspend fun <T> visitSingleDaysMetricByTicker(tickers: List<String>, date: Date, metric:String = "ohlc_min", handler: suspend (Equity, Equity.Day, List<Array<Number>>) -> List<T>): List<T> {
        val calendar = Calendar.getInstance().apply { timeInMillis = date.time }
        val startYear = calendar[Calendar.YEAR]
        val startDay = calendar[Calendar.DAY_OF_YEAR]
        return visitStocksByTicker(tickers) { equity ->
            (equity.metrics[metric]?.let { metric ->
                metric.years[startYear.toString()]?.let { year ->
                    year.days[startDay.toString()]?.let { day ->
                        handler.invoke(equity, day, day.values)
                    }
                }
            } ?: listOf<T>()) as List<T>
        }
    }

}