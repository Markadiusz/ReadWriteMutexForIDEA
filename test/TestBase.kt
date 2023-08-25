/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER", "CANNOT_OVERRIDE_INVISIBLE_MEMBER")

import kotlinx.coroutines.*
import kotlin.test.*
import kotlinx.coroutines.scheduling.*
import java.io.*
import java.util.*
import java.util.concurrent.atomic.*
import kotlin.coroutines.*

private val VERBOSE = false

/**
 * Is `true` when running in a nightly stress test mode.
 */
public val isStressTest = System.getProperty("stressTest")?.toBoolean() ?: false

public val stressTestMultiplierSqrt = if (isStressTest) 5 else 1

private const val SHUTDOWN_TIMEOUT = 1_000L // 1s at most to wait per thread

public val isNative = false

/**
 * Multiply various constants in stress tests by this factor, so that they run longer during nightly stress test.
 */
public val stressTestMultiplier = stressTestMultiplierSqrt * stressTestMultiplierSqrt


@Suppress("ACTUAL_WITHOUT_EXPECT")
public typealias TestResult = Unit

public open class TestBase(private var disableOutCheck: Boolean)  {

    constructor(): this(false)

    public val isBoundByJsTestTimeout = false
    private var actionIndex = AtomicInteger()
    private var finished = AtomicBoolean()
    private var error = AtomicReference<Throwable>()

    // Shutdown sequence
    private lateinit var threadsBefore: Set<Thread>
    private val uncaughtExceptions = Collections.synchronizedList(ArrayList<Throwable>())
    private var originalUncaughtExceptionHandler: Thread.UncaughtExceptionHandler? = null
    /*
     * System.out that we redefine in order to catch any debugging/diagnostics
     * 'println' from main source set.
     * NB: We do rely on the name 'previousOut' in the FieldWalker in order to skip its
     * processing
     */
    private lateinit var previousOut: PrintStream

    /**
     * Throws [IllegalStateException] like `error` in stdlib, but also ensures that the test will not
     * complete successfully even if this exception is consumed somewhere in the test.
     */
    @Suppress("ACTUAL_FUNCTION_WITH_DEFAULT_ARGUMENTS")
    public fun error(message: Any, cause: Throwable? = null): Nothing {
        throw makeError(message, cause)
    }

    public fun hasError() = error.get() != null

    private fun makeError(message: Any, cause: Throwable? = null): IllegalStateException =
        IllegalStateException(message.toString(), cause).also {
            setError(it)
        }

    private fun setError(exception: Throwable) {
        error.compareAndSet(null, exception)
    }

    private fun printError(message: String, cause: Throwable) {
        setError(cause)
        println("$message: $cause")
        cause.printStackTrace(System.out)
        println("--- Detected at ---")
        Throwable().printStackTrace(System.out)
    }

    /**
     * Throws [IllegalStateException] when `value` is false like `check` in stdlib, but also ensures that the
     * test will not complete successfully even if this exception is consumed somewhere in the test.
     */
    public inline fun check(value: Boolean, lazyMessage: () -> Any) {
        if (!value) error(lazyMessage())
    }

    /**
     * Asserts that this invocation is `index`-th in the execution sequence (counting from one).
     */
    public fun expect(index: Int) {
        val wasIndex = actionIndex.incrementAndGet()
        if (VERBOSE) println("expect($index), wasIndex=$wasIndex")
        check(index == wasIndex) { "Expecting action index $index but it is actually $wasIndex" }
    }

    /**
     * Asserts that this line is never executed.
     */
    public fun expectUnreached() {
        error("Should not be reached, current action index is ${actionIndex.get()}")
    }

    /**
     * Asserts that this is the last action in the test. It must be invoked by any test that used [expect].
     */
    public fun finish(index: Int) {
        expect(index)
        check(!finished.getAndSet(true)) { "Should call 'finish(...)' at most once" }
    }

    /**
     * Asserts that [finish] was invoked
     */
    public fun ensureFinished() {
        require(finished.get()) { "finish(...) should be caller prior to this check" }
    }

    public fun reset() {
        check(actionIndex.get() == 0 || finished.get()) { "Expecting that 'finish(...)' was invoked, but it was not" }
        actionIndex.set(0)
        finished.set(false)
    }

    private object TestOutputStream : PrintStream(object : OutputStream() {
        override fun write(b: Int) {
            error("Detected unexpected call to 'println' from source code")
        }
    })

    fun println(message: Any?) {
        if (disableOutCheck) kotlin.io.println(message)
        else previousOut.println(message)
    }

    @BeforeTest
    fun before() {
        initPoolsBeforeTest()
        threadsBefore = currentThreads()
        originalUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { t, e ->
            println("Exception in thread $t: $e") // The same message as in default handler
            e.printStackTrace()
            uncaughtExceptions.add(e)
        }
        if (!disableOutCheck) {
            previousOut = System.out
            System.setOut(TestOutputStream)
        }
    }

    @AfterTest
    fun onCompletion() {
        // onCompletion should not throw exceptions before it finishes all cleanup, so that other tests always
        // start in a clear, restored state
        if (actionIndex.get() != 0 && !finished.get()) {
            makeError("Expecting that 'finish(${actionIndex.get() + 1})' was invoked, but it was not")
        }
        // Shutdown all thread pools
        shutdownPoolsAfterTest()
        // Check that are now leftover threads
        runCatching {
            checkTestThreads(threadsBefore)
        }.onFailure {
            setError(it)
        }
        // Restore original uncaught exception handler
        Thread.setDefaultUncaughtExceptionHandler(originalUncaughtExceptionHandler)
        if (!disableOutCheck) {
            System.setOut(previousOut)
        }
        if (uncaughtExceptions.isNotEmpty()) {
            makeError("Expected no uncaught exceptions, but got $uncaughtExceptions")
        }
        // The very last action -- throw error if any was detected
        error.get()?.let { throw it }
    }

    fun initPoolsBeforeTest() {
        DefaultScheduler.usePrivateScheduler()
    }

    fun shutdownPoolsAfterTest() {
        DefaultScheduler.shutdown(SHUTDOWN_TIMEOUT)
        DefaultExecutor.shutdownForTests(SHUTDOWN_TIMEOUT)
        DefaultScheduler.restore()
    }

    @Suppress("ACTUAL_WITHOUT_EXPECT", "ACTUAL_FUNCTION_WITH_DEFAULT_ARGUMENTS")
    public fun runTest(
        expected: ((Throwable) -> Boolean)? = null,
        unhandled: List<(Throwable) -> Boolean> = emptyList(),
        block: suspend CoroutineScope.() -> Unit
    ): TestResult {
        var exCount = 0
        var ex: Throwable? = null
        try {
            runBlocking(block = block, context = CoroutineExceptionHandler { _, e ->
                if (e is CancellationException) return@CoroutineExceptionHandler // are ignored
                exCount++
                when {
                    exCount > unhandled.size ->
                        printError("Too many unhandled exceptions $exCount, expected ${unhandled.size}, got: $e", e)
                    !unhandled[exCount - 1](e) ->
                        printError("Unhandled exception was unexpected: $e", e)
                }
            })
        } catch (e: Throwable) {
            ex = e
            if (expected != null) {
                if (!expected(e))
                    error("Unexpected exception: $e", e)
            } else {
                throw e
            }
        } finally {
            if (ex == null && expected != null) error("Exception was expected but none produced")
        }
        if (exCount < unhandled.size)
            error("Too few unhandled exceptions $exCount, expected ${unhandled.size}")
    }

    protected inline fun <reified T: Throwable> assertFailsWith(block: () -> Unit): T {
        val result = runCatching(block)
        assertTrue(result.exceptionOrNull() is T, "Expected ${T::class}, but had $result")
        return result.exceptionOrNull()!! as T
    }

    protected suspend fun currentDispatcher() = coroutineContext[ContinuationInterceptor]!!
}

private const val WAIT_LOST_THREADS = 10_000L // 10s
private val ignoreLostThreads = mutableSetOf<String>()

fun ignoreLostThreads(vararg s: String) { ignoreLostThreads += s }

fun currentThreads(): Set<Thread> {
    var estimate = 0
    while (true) {
        estimate = estimate.coerceAtLeast(Thread.activeCount() + 1)
        val arrayOfThreads = Array<Thread?>(estimate) { null }
        val n = Thread.enumerate(arrayOfThreads)
        if (n >= estimate) {
            estimate = n + 1
            continue // retry with a better size estimate
        }
        val threads = hashSetOf<Thread>()
        for (i in 0 until n)
            threads.add(arrayOfThreads[i]!!)
        return threads
    }
}

fun List<Thread>.dumpThreads(header: String) {
    println("=== $header")
    forEach { thread ->
        println("Thread \"${thread.name}\" ${thread.state}")
        val trace = thread.stackTrace
        for (t in trace) println("\tat ${t.className}.${t.methodName}(${t.fileName}:${t.lineNumber})")
        println()
    }
    println("===")
}

fun checkTestThreads(threadsBefore: Set<Thread>) {
    // give threads some time to shutdown
    val waitTill = System.currentTimeMillis() + WAIT_LOST_THREADS
    var diff: List<Thread>
    do {
        val threadsAfter = currentThreads()
        diff = (threadsAfter - threadsBefore).filter { thread ->
            ignoreLostThreads.none { prefix -> thread.name.startsWith(prefix) }
        }
        if (diff.isEmpty()) break
    } while (System.currentTimeMillis() <= waitTill)
    ignoreLostThreads.clear()
    if (diff.isEmpty()) return
    val message = "Lost threads ${diff.map { it.name }}"
    println("!!! $message")
    diff.dumpThreads("Dumping lost thread stack traces")
    error(message)
}

@OptIn(InternalCoroutinesApi::class)
fun <T> CancellableContinuation<T>.tryResume0(value: T, onCancellation: (Throwable?) -> Unit): Boolean {
    tryResume(value, null, onCancellation).let {
        if (it == null) return false
        completeResume(it)
        return true
    }
}