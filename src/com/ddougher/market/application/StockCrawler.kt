package com.ddougher.market.application

import com.ddougher.market.data.core.Stocks
import com.ddougher.proxamic.DocumentStore
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.*



class StockCrawler(val documentStore: DocumentStore) {




    @Suppress("ControlFlowWithEmptyBody")
    fun <T> crawlStocksByTicker(tickers: List<String>, date: Date, handler: (List<Array<Number>>) -> List<T>): List<T> {
        return runBlocking {
            val stocks = documentStore.get(Stocks::class.java, "stocks")
            val tickersChannel = Channel<String>(100)
            val calendar = Calendar.getInstance().apply { timeInMillis = date.time }
            val startYear = calendar[Calendar.YEAR]
            val startDay = calendar[Calendar.DAY_OF_YEAR]

            channelFlow {
                repeat(60) {
                    launch {
                        while (null != tickersChannel.receiveCatching().getOrNull()?.also { ticker ->
                                stocks.tickers()[ticker]?.also { equity ->
                                    equity.metrics["ohlc_min"]?.also { metric ->
                                        metric.years[startYear.toString()]?.also { year ->
                                            year.days[startDay.toString()]?.also { day ->
                                                send(handler.invoke(day.values))
                                            }
                                        }
                                    }
                                }
                            }
                        );
                    }
                }
                tickers.distinct().forEach { tickersChannel.send(it)}
                tickersChannel.close()
            }.toList().flatten()
        }
    }

}