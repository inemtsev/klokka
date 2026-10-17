package com.eventslooped.klokka

/**
 * Marks API meant for integration modules (klokka-exposed and similar bridges), not for
 * application code. It is public so bridges outside klokka-core can build on it, and it may
 * change between minor versions without the compatibility care given to the rest of the API.
 */
@RequiresOptIn(
    message = "Klokka bridge API: meant for integration modules, not application code. It may change between minor versions.",
    level = RequiresOptIn.Level.ERROR,
)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY)
public annotation class KlokkaBridgeApi
