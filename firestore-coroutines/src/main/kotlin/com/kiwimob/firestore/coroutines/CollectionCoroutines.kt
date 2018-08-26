package com.kiwimob.firestore.coroutines

import com.google.firebase.firestore.CollectionReference
import com.google.firebase.firestore.DocumentReference
import com.google.firebase.firestore.DocumentSnapshot
import com.google.firebase.firestore.QuerySnapshot
import kotlinx.coroutines.experimental.NonCancellable
import kotlinx.coroutines.experimental.suspendCancellableCoroutine

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