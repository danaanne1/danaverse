package com.ddougher.market.polygon

import com.ddougher.extensions.toObject
import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.core.Stocks
import com.ddougher.proxamic.ObservableDocumentStore
import com.fasterxml.jackson.databind.JsonNode
import com.theunknowablebits.proxamic.DocumentStore
import java.io.Closeable
import java.net.URL
import java.util.function.Consumer

class BackfilTickers(val docStore: ObservableDocumentStore, val apiKey: String ) : Runnable {

    override fun run() {
        var url: URL? = URL("https://api.polygon.io/v3/reference/tickers?market=stocks&type=CS&active=true&sort=ticker&order=asc&limit=1000&apiKey=" + apiKey)
        while (url != null) {
            val response = url.openStream().use { it.readBytes().toObject<Map<String,Any>>() }
            docStore.transact { ds: DocumentStore ->
                val stocks: Stocks = ds.get(Stocks::class.java, "stocks")
                for (result in response["results"] as List<Map<String,Any>>) {
                    val equity:Equity = stocks.tickers().getOrPut(result["ticker"] as String) { ds.newInstance(Equity::class.java, "Equity/${result["ticker"]}")}
                    equity.apply {
                        active = result["active"] as Boolean
                        exchange = result["exchange"] as String
                        locale = result["local"] as String
                        name = result["name"] as String
                        type = result["type"] as String
                        symbol = result["ticker"] as String
                    }
                    ds.put(equity)
                }
                ds.put(stocks)
            }
            url = null
            if ("next_url" in response) {
                url = URL(response["next_url"].toString() + "&apiKey=" + apiKey)
            }
            println("Backfill Looping next 1000")
        }
    }

}