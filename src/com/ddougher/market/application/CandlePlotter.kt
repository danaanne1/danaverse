package com.ddougher.market.application

import com.ddougher.market.data.core.Equity
import com.ddougher.market.data.MetricConstants
import java.awt.AlphaComposite
import java.awt.Color
import java.awt.Graphics2D
import java.awt.geom.Rectangle2D
import java.util.*

class CandlePlotter {

    /**
     * Given an equity and an end time, this method plots 15 candlesticks with the wicks half as wide as the body
     *
     * from left to right, the candles cover a continuous range of time prior to the end time as follows
     * - the first 5 candles represent 24 hour aggregates
     * - the next 5 candles represent 1 hour aggregates
     * - the next 5 candles represent 5 minute aggregates
     * - the last 5 candles represent 1 minute aggregates
     *
     * there is no lost time between candles and the sequence must end on the 1 minute aggregate prior to the end time
     *
     * the rendering of the candle plot should be zoomed to cover the entire graphics, ideally at floating point resolution
     *
     * the candles are drawn in filled black with a alpha value of 1/2^16
     *
     * the plot should render into the supplied graphics additively
     *
     * the graphics is in 16 bit greyscale
     */
    fun plotCandles(equity: Equity, endTime: Long, graphics2D: Graphics2D, color: Color = Color.BLACK) {
        val candleData = collectCandleData(equity, endTime)
        if (candleData.isEmpty()) return

        // Since graphics is pre-transformed to 100x100 coordinate system
        val canvasWidth = 100.0
        val canvasHeight = 100.0
        val candleWidth = canvasWidth / 20.0
        val wickWidth = candleWidth / 2

        // Find price range for scaling - filter out NaN values which represent placeholders
        val allPrices = candleData.flatMap { candle ->
            listOf(candle.open, candle.high, candle.low, candle.close)
        }.filter { !it.isNaN() && it > 0 }

        if (allPrices.isEmpty()) return

        val minPrice = allPrices.minOrNull() ?: return
        val maxPrice = allPrices.maxOrNull() ?: return
        val priceRange = maxPrice - minPrice

        if (priceRange <= 0) return

        candleData.forEachIndexed { index, candle ->
            // Skip candles with NaN values (placeholders)
            if (candle.open.isNaN() || candle.high.isNaN() || candle.low.isNaN() || candle.close.isNaN()) {
                // Skip this placeholder candle
                return@forEachIndexed
            }

            // Calculate x position in 100x100 space
            val x = index * candleWidth

            // Scale prices to fit the 100x100 coordinate space
            val openY = canvasHeight - ((candle.open - minPrice) / priceRange * canvasHeight)
            val highY = canvasHeight - ((candle.high - minPrice) / priceRange * canvasHeight)
            val lowY = canvasHeight - ((candle.low - minPrice) / priceRange * canvasHeight)
            val closeY = canvasHeight - ((candle.close - minPrice) / priceRange * canvasHeight)

            // Calculate body bounds
            val bodyTop = minOf(openY, closeY)
            val bodyBottom = maxOf(openY, closeY)
            val bodyHeight = bodyBottom - bodyTop

            // Calculate wick center position
            val wickX = x + candleWidth / 2 - wickWidth / 2

            // Draw upper wick (from high to top of body) - only if there's space above the body
            if (highY < bodyTop) {
                graphics2D.fill(Rectangle2D.Double(wickX, highY, wickWidth, bodyTop - highY))
            }

            // Draw lower wick (from bottom of body to low) - only if there's space below the body
            if (lowY > bodyBottom) {
                graphics2D.fill(Rectangle2D.Double(wickX, bodyBottom, wickWidth, lowY - bodyBottom))
            }

            // Draw body (open-close rectangle)
            if (bodyHeight > 0) {
                graphics2D.fill(Rectangle2D.Double(x, bodyTop, candleWidth, bodyHeight))
            }
        }
    }

    /**
     * Given an equity and an end time, this method plots 15 candlesticks with the wicks adjacent to the body
     *
     * from left to right, the candles cover a continuous range of time prior to the end time as follows
     * - the first 5 candles represent 24 hour aggregates
     * - the next 5 candles represent 1 hour aggregates
     * - the next 5 candles represent 5 minute aggregates
     * - the last 5 candles represent 1 minute aggregates
     *
     * there is no lost time between candles and the sequence must end on the 1 minute aggregate prior to the end time
     *
     * the rendering of the candle plot should be zoomed to cover the entire graphics, ideally at floating point resolution
     *
     * the candles are drawn in filled black with a alpha value of 1/2^16
     *
     * the plot should render into the supplied graphics additively
     *
     * the graphics is in 16 bit greyscale
     *
     * Unlike the original plotCandles method, the wicks are positioned adjacent to the body rather than centered
     */
    fun plotCandlesWithAdjacentWicks(equity: Equity, endTime: Long, graphics2D: Graphics2D) {
        val candleData = collectCandleData(equity, endTime)
        if (candleData.isEmpty()) return

        // Since graphics is pre-transformed to 100x100 coordinate system
        val canvasWidth = 100.0
        val canvasHeight = 100.0
        val candleWidth = canvasWidth / 20.0
        val wickWidth = candleWidth / 2

        // Find price range for scaling - filter out NaN values which represent placeholders
        val allPrices = candleData.flatMap { candle ->
            listOf(candle.open, candle.high, candle.low, candle.close)
        }.filter { !it.isNaN() && it > 0 }

        if (allPrices.isEmpty()) return

        val minPrice = allPrices.minOrNull() ?: return
        val maxPrice = allPrices.maxOrNull() ?: return
        val priceRange = maxPrice - minPrice

        if (priceRange <= 0) return

        // Set up graphics for additive rendering with specified alpha
        // val originalComposite = graphics2D.composite
        // graphics2D.composite = AlphaComposite.getInstance(AlphaComposite.SRC_OVER, 1.0f / (1 shl 16))
        graphics2D.color = Color.BLACK

        candleData.forEachIndexed { index, candle ->
            // Skip candles with NaN values (placeholders)
            if (candle.open.isNaN() || candle.high.isNaN() || candle.low.isNaN() || candle.close.isNaN()) {
                // Skip this placeholder candle
                return@forEachIndexed
            }

            // Calculate x position in 100x100 space
            val x = index * candleWidth

            // Scale prices to fit the 100x100 coordinate space
            val openY = canvasHeight - ((candle.open - minPrice) / priceRange * canvasHeight)
            val highY = canvasHeight - ((candle.high - minPrice) / priceRange * canvasHeight)
            val lowY = canvasHeight - ((candle.low - minPrice) / priceRange * canvasHeight)
            val closeY = canvasHeight - ((candle.close - minPrice) / priceRange * canvasHeight)

            // Calculate body bounds
            val bodyTop = minOf(openY, closeY)
            val bodyBottom = maxOf(openY, closeY)
            val bodyHeight = bodyBottom - bodyTop

            // Position wick adjacent to the body (at the right side of the candle space)
            val wickX = x + candleWidth - wickWidth

            // Draw upper wick (from high to low, full range)
            graphics2D.fill(Rectangle2D.Double(wickX, highY, wickWidth, lowY - highY))

            // Draw body (open-close rectangle) - positioned to leave room for adjacent wick
            if (bodyHeight > 0) {
                val bodyWidth = candleWidth - wickWidth
                graphics2D.fill(Rectangle2D.Double(x, bodyTop, bodyWidth, bodyHeight))
            }
        }
    }

    private data class CandleInfo(
        val time: Long,
        val open: Double,
        val high: Double,
        val low: Double,
        val close: Double,
        val volume: Long
    )

    private fun collectCandleData(equity: Equity, endTime: Long): List<CandleInfo> {
        val candles = mutableListOf<CandleInfo>()
        val ohlcMetric = equity.metrics["ohlc_min"] ?: return candles

        // Time intervals in milliseconds
        val dayMs = 24 * 60 * 60 * 1000L
        val hourMs = 60 * 60 * 1000L
        val minuteMs = 60 * 1000L
        val fiveMinuteMs = 5 * minuteMs

        // Calculate total time span for all candles
        val totalTimeSpan = (5 * dayMs) + (5 * hourMs) + (5 * fiveMinuteMs) + (5 * minuteMs)
        val startTime = endTime - totalTimeSpan

        // Create continuous, non-overlapping time ranges
        var currentTime = startTime
        val timeRanges = mutableListOf<Pair<Long, Long>>()

        // 5 day candles (oldest data) - each candle spans 24 hours
        repeat(5) {
            val rangeStart = currentTime
            val rangeEnd = currentTime + dayMs
            timeRanges.add(Pair(rangeStart, rangeEnd))
            currentTime = rangeEnd
        }

        // 5 hour candles - each candle spans 1 hour  
        repeat(5) {
            val rangeStart = currentTime
            val rangeEnd = currentTime + hourMs
            timeRanges.add(Pair(rangeStart, rangeEnd))
            currentTime = rangeEnd
        }

        // 5 five-minute candles - each candle spans 5 minutes
        repeat(5) {
            val rangeStart = currentTime
            val rangeEnd = currentTime + fiveMinuteMs
            timeRanges.add(Pair(rangeStart, rangeEnd))
            currentTime = rangeEnd
        }

        // 5 one-minute candles (newest data) - each candle spans 1 minute
        repeat(5) {
            val rangeStart = currentTime
            val rangeEnd = currentTime + minuteMs
            timeRanges.add(Pair(rangeStart, rangeEnd))
            currentTime = rangeEnd
        }

        // Verify continuity (currentTime should equal endTime)
        // assert(currentTime == endTime) { "Time ranges are not continuous: expected $endTime, got $currentTime" }

        for ((rangeStart, rangeEnd) in timeRanges) {
            val candleInfo = aggregateDataForTimeRange(ohlcMetric, rangeStart, rangeEnd)
            candleInfo?.let { candles.add(it) }
        }

        return candles
    }

    private fun aggregateDataForTimeRange(
        ohlcMetric: Equity.Metric,
        startTime: Long,
        endTime: Long
    ): CandleInfo? {
        val calendar = Calendar.getInstance()
        val relevantData = mutableListOf<Array<Number>>()

        // Calculate date range for efficient year/day lookup
        calendar.timeInMillis = startTime
        val startYear = calendar.get(Calendar.YEAR)
        val startDay = calendar.get(Calendar.DAY_OF_YEAR)

        calendar.timeInMillis = endTime - 1 // endTime is exclusive
        val endYear = calendar.get(Calendar.YEAR)
        val endDay = calendar.get(Calendar.DAY_OF_YEAR)

        // Iterate through relevant years and days only
        for (year in startYear..endYear) {
            val yearData = ohlcMetric.years[year.toString()] ?: continue

            val dayStart = if (year == startYear) startDay else 1
            val dayEnd = if (year == endYear) endDay else 365 // Will be filtered by actual days available

            for (day in dayStart..dayEnd) {
                val dayData = yearData.days[day.toString()] ?: continue

                // Use binary search to find start and end indices for the time range
                val values = dayData.values
                if (values.isEmpty()) continue

                val startIndex = findStartIndex(values, startTime)
                val endIndex = findEndIndex(values, endTime)

                if (startIndex < values.size && endIndex >= 0 && startIndex <= endIndex) {
                    for (i in startIndex..endIndex) {
                        if (i < values.size) {
                            relevantData.add(values[i])
                        }
                    }
                }
            }
        }

        if (relevantData.isEmpty()) {
            // Return a placeholder with null values that won't be plotted
            // but still represents the time range for continuity
            return CandleInfo(startTime, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0)
        }

        // Data should already be ordered, but ensure it for aggregation
        relevantData.sortBy { it[MetricConstants.Candle.TIME.value].toLong() }

        // Aggregate the data
        val open = relevantData.first()[MetricConstants.Candle.OPEN.value].toDouble()
        val close = relevantData.last()[MetricConstants.Candle.CLOSE.value].toDouble()
        val high = relevantData.maxOfOrNull { it[MetricConstants.Candle.HIGH.value].toDouble() } ?: 0.0
        val low = relevantData.minOfOrNull { it[MetricConstants.Candle.LOW.value].toDouble() } ?: 0.0
        val totalVolume = relevantData.sumOf { it[MetricConstants.Candle.VOLUME.value].toLong() }

        return CandleInfo(startTime, open, high, low, close, totalVolume)
    }

    /**
     * Find the first index where timestamp >= startTime using binarySearch similar to mergeAggregateData
     */
    private fun findStartIndex(values: List<Array<Number>>, startTime: Long): Int {
        val result = values.binarySearch { numbers -> numbers[MetricConstants.Candle.TIME.value].toLong().compareTo(startTime) }
        return if (result >= 0) {
            result // Found exact match
        } else {
            -result - 1 // Insert point gives us the first element >= startTime
        }
    }

    /**
     * Find the last index where timestamp < endTime using binarySearch similar to mergeAggregateData
     */
    private fun findEndIndex(values: List<Array<Number>>, endTime: Long): Int {
        val result = values.binarySearch { numbers -> numbers[MetricConstants.Candle.TIME.value].toLong().compareTo(endTime) }
        return if (result >= 0) {
            result - 1 // Found exact match, we want the element before it (since endTime is exclusive)
        } else {
            -result - 2 // Insert point - 1 gives us the last element < endTime
        }
    }

}