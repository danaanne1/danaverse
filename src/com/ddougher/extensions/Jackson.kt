package com.ddougher.extensions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.reflect.KClass

val jacksonMapper: ObjectMapper by lazy {
    jacksonObjectMapper().apply {
      disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
      dateFormat = StdDateFormat()
    }
  }

fun <T : Any> KClass<out T>.fromJson(data: String) = jacksonMapper.readValue(data, this.java)!!
fun <T : Any> KClass<out T>.fromJson(data: ByteArray) = jacksonMapper.readValue(data, this.java)!!

inline fun <reified T : Any> String.toObject() = T::class.fromJson(this)
inline fun <reified T : Any> ByteArray.toObject() = T::class.fromJson(this)

fun Any?.toJsonString(): String = jacksonMapper.writeValueAsString(this)
fun Any?.toJsonBytes(): ByteArray = jacksonMapper.writeValueAsBytes(this)

