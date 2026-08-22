package com.eventslooped.klokka

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

/**
 * Serializes job payloads to and from the string form stored by the
 * [com.eventslooped.klokka.spi.JobStore]. The default is lenient JSON; a custom codec
 * lets non-kotlinx users plug in Jackson, protobuf-as-base64, or anything else.
 *
 * Implementations must wrap decode failures in [PayloadDecodeException]. Decode failures
 * are [NonRetryable] by definition: a payload that cannot be parsed will not parse better
 * on the next attempt, so the runtime dead-letters it immediately.
 */
public interface PayloadCodec {
    public fun <T> encode(serializer: KSerializer<T>, value: T): String

    /** @throws PayloadDecodeException when [payload] cannot be decoded. */
    public fun <T> decode(serializer: KSerializer<T>, payload: String): T
}

/**
 * Raised when a stored payload cannot be decoded. Implements [NonRetryable], which is
 * what makes "decode failures skip retries and dead-letter immediately" fall out of the
 * ordinary failure path instead of needing a special case.
 */
public class PayloadDecodeException(
    public val kind: String,
    message: String,
    cause: Throwable? = null,
) : RuntimeException("Cannot decode payload for job kind '$kind': $message", cause), NonRetryable

/**
 * The default codec: JSON via kotlinx-serialization, configured leniently so a
 * new-version producer does not break an old-version consumer mid-rolling-deploy:
 * unknown keys are ignored, defaults are encoded.
 */
public class JsonPayloadCodec(
    private val json: Json = DEFAULT_JSON,
) : PayloadCodec {
    override fun <T> encode(serializer: KSerializer<T>, value: T): String = json.encodeToString(serializer, value)

    override fun <T> decode(serializer: KSerializer<T>, payload: String): T =
        try {
            json.decodeFromString(serializer, payload)
        } catch (e: Exception) {
            throw PayloadDecodeException(kind = serializer.descriptor.serialName, message = e.message ?: "decode failed", cause = e)
        }

    public companion object {
        public val DEFAULT_JSON: Json =
            Json {
                ignoreUnknownKeys = true
                encodeDefaults = true
            }
    }
}
