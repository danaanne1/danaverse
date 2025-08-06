package com.ddougher.market.application

import com.ddougher.market.data.core.Stocks
import com.ddougher.documentstore.DocumentStore
import com.ddougher.market.data.MetricConstants.Candle
import com.ddougher.market.data.core.Equity
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import java.io.Serializable
import java.util.*
import kotlin.collections.listOf

data class Swing(val startTimeMs: Long, val endTimeMs: Long, val swingPercent: Double, val ticker:String): Serializable {
    companion object {
        private const val serialVersionUID: Long = 0L
    }
}

class StockCrawler(val documentStore: DocumentStore) {

    /** find all the stocks that went up by more than 4% in a day */
    suspend fun locateFourPercentSwings(equity: Equity, metricData: List<Array<Number>>): List<Swing> {
        val lowCandle = metricData.minBy { it[Candle.LOW.value].toDouble() }
        val highCandle = metricData.minBy { it[Candle.HIGH.value].toDouble() }

        val lowTime = lowCandle[Candle.TIME.value].toLong()
        val highTime = highCandle[Candle.TIME.value].toLong()
        if (highTime < lowTime) return listOf()

        val high = highCandle[Candle.HIGH.value].toDouble()
        val low = lowCandle[Candle.LOW.value].toDouble()
        if ((high-low)/low < .04) return listOf()

        return listOf(
            Swing(
                lowTime,
                highTime,
                (high-low)/low,
                equity.symbol
            )
        )
    }
    
    /** invokes handler for every equity in the list of tickers */
    @Suppress("ControlFlowWithEmptyBody")
    suspend fun <T> visitStocksByTicker(tickers: List<String>, handler: suspend (Equity) -> List<T>): List<T> {
        val stocks = documentStore.get(Stocks::class.java, "stocks")
        val tickersChannel = Channel<String>(100)

        return channelFlow {
            repeat(60) {
                launch {
                    while (null != tickersChannel.receiveCatching().getOrNull()?.also { ticker ->
                            stocks.tickers()[ticker]?.also { equity ->
                                send(handler.invoke(equity))
                            }
                        }
                    );
                }
            }
            tickers.distinct().forEach { tickersChannel.send(it)}
            tickersChannel.close()
        }.toList().flatten()
    }

    /** Invokes handler for every set of day aggregates for equitiy, beginning from startTime */
    @Suppress("UNCHECKED_CAST")
    suspend fun <T> visitEveryDaysMetricByTicker(tickers: List<String>, startDate: Date, metric:String = "ohlc_min", handler: suspend (Equity, List<Array<Number>>) -> List<T>): List<T> {
        val calendar = Calendar.getInstance().apply { timeInMillis = startDate.time }
        val startYear = calendar[Calendar.YEAR]
        val startDay = calendar[Calendar.DAY_OF_YEAR]
        return visitStocksByTicker(tickers) { equity ->
            (equity.metrics[metric]?.let { metric ->
                metric.years
                    .filter { (year, _) -> 
                        year.toInt() >= startYear 
                    }.flatMap { (year, yearData) -> 
                        yearData.days.filter { (day, _) -> year.toInt() > startYear || day.toInt() >= startDay
                    }.flatMap { (_, dayData) ->
                        handler.invoke(equity, dayData.values)
                    }
                }
            } ?: listOf<T>()) as List<T>
        }
    }

    /** invokes handler for a single days metric */
    @Suppress("UNCHECKED_CAST")
    suspend fun <T> visitSingleDaysMetricByTicker(tickers: List<String>, date: Date, metric:String = "ohlc_min", handler: suspend (Equity, List<Array<Number>>) -> List<T>): List<T> {
        val calendar = Calendar.getInstance().apply { timeInMillis = date.time }
        val startYear = calendar[Calendar.YEAR]
        val startDay = calendar[Calendar.DAY_OF_YEAR]
        return visitStocksByTicker(tickers) { equity ->
            (equity.metrics[metric]?.let { metric ->
                metric.years[startYear.toString()]?.let { year ->
                    year.days[startDay.toString()]?.let { day ->
                        handler.invoke(equity, day.values)
                    }
                }
            } ?: listOf<T>()) as List<T>
        }
    }

}