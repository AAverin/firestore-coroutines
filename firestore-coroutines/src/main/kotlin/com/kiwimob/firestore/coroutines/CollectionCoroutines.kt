package com.kiwimob.firestore.coroutines

import com.google.firebase.firestore.*
import kotlinx.coroutines.experimental.NonCancellable
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.sendBlocking
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.suspendCancellableCoroutine
import kotlin.coroutines.experimental.coroutineContext

suspend fun <T : Any> CollectionReference.await(clazz: Class<T>): List<T> {
    return await { documentSnapshot -> documentSnapshot.toObject(clazz) as T }
}

suspend fun <T : Any> CollectionReference.await(parser: (documentSnapshot: DocumentSnapshot) -> T): List<T> {
    return suspendCancellableCoroutine { continuation ->
        get().addOnCompleteListener {
            if (it.isSuccessful) {
                val list = arrayListOf<T>()
                it.result.forEach { list.add(parser.invoke(it)) }

                continuation.resume(list)
            } else {
                continuation.resumeWithException(it.exception!!)
            }
        }

        continuation.invokeOnCancellation {
            if (continuation.isCancelled)
                try {
                    NonCancellable.cancel()
                } catch (ex: Throwable) {
                    //Ignore cancel exception
                }
        }
    }
}

suspend fun <T : Any> Query.listen(clazz: Class<T>): ReceiveChannel<List<T>> = listen { documentSnapshot ->
    documentSnapshot.toObject(clazz) as T
}


suspend fun <T : Any> Query.listen(parser: (documentSnapshot: DocumentSnapshot) -> T): ReceiveChannel<List<T>> {
    val channel = Channel<List<T>>()

    addSnapshotListener { querySnapshot, firebaseFirestoreException ->
        firebaseFirestoreException?.let {
            channel.close(it)
            return@addSnapshotListener
        }
        if (querySnapshot == null) {
            channel.close()
            return@addSnapshotListener
        }

        val list = arrayListOf<T>()
        querySnapshot.forEach {
            list.add(parser.invoke(it))
        }
        channel.sendBlocking(list)
    }

    return channel
}

suspend fun CollectionReference.await(): QuerySnapshot {
    return suspendCancellableCoroutine { continuation ->

        get().addOnCompleteListener() {
            if (it.isSuccessful) {
                continuation.resume(it.result)
            } else {
                continuation.resumeWithException(it.exception!!)
            }
        }

        continuation.invokeOnCancellation {
            if (continuation.isCancelled)
                try {
                    NonCancellable.cancel()
                } catch (ex: Throwable) {
                    //Ignore cancel exception
                    ex.printStackTrace()
                }
        }

    }
}

suspend fun CollectionReference.addAwait(value: Any): DocumentReference {
    return suspendCancellableCoroutine { continuation ->
        add(value).addOnCompleteListener {
            if (it.isSuccessful) {
                continuation.resume(it.result)
            } else {
                continuation.resumeWithException(it.exception!!)
            }
        }

        continuation.invokeOnCancellation {
            if (continuation.isCancelled)
                try {
                    NonCancellable.cancel()
                } catch (ex: Throwable) {
                    //Ignore cancel exception
                }
        }
    }
}

suspend fun CollectionReference.addAwait(value: Map<String, Any>): DocumentReference {
    return suspendCancellableCoroutine { continuation ->
        add(value).addOnCompleteListener {
            if (it.isSuccessful) {
                continuation.resume(it.result)
            } else {
                continuation.resumeWithException(it.exception!!)
            }
        }

        continuation.invokeOnCancellation {
            if (continuation.isCancelled)
                try {
                    NonCancellable.cancel()
                } catch (ex: Throwable) {
                    //Ignore cancel exception
                }
        }
    }
}