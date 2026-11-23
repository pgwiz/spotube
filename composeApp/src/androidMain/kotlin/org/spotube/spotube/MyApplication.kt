/*
 * Copyright (C) 2026 Kingkor Roy Tirtho and Spotube Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.spotube.spotube

import android.app.Application
import android.os.Build
import android.util.Log
import org.spotube.spotube.core.di.initKoin
import org.spotube.spotube.core.paths.Paths
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.koin.android.ext.koin.androidContext
import org.koin.core.component.KoinComponent
import java.io.File
import java.util.Date

class MyApplication : Application(), KoinComponent {
    private val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    companion object {
        private const val TAG = "SpotubeApp"
    }

    override fun onCreate() {
        super.onCreate()
        setupCrashHandler()

        try {
            Paths.init(this)
        } catch (e: Throwable) {
            Log.e(TAG, "Paths.init failed during application onCreate", e)
        }

        try {
            initKoin {
                androidContext(this@MyApplication)
            }
        } catch (e: Throwable) {
            Log.e(TAG, "initKoin failed during application onCreate", e)
        }
    }

    private fun setupCrashHandler() {
        val defaultHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
            try {
                val crashReport = buildString {
                    appendLine("=== SPOTUBE FATAL CRASH REPORT ===")
                    appendLine("Time: ${Date()}")
                    appendLine("Device: ${Build.MANUFACTURER} ${Build.MODEL} (${Build.PRODUCT})")
                    appendLine("Android OS: ${Build.VERSION.RELEASE} (SDK ${Build.VERSION.SDK_INT})")
                    appendLine("Thread: ${thread.name} (id: ${thread.id})")
                    appendLine()
                    appendLine("Exception: ${throwable::class.qualifiedName}: ${throwable.message}")
                    appendLine()
                    appendLine("Stacktrace:")
                    appendLine(throwable.stackTraceToString())
                    var cause = throwable.cause
                    var depth = 0
                    while (cause != null && depth < 5) {
                        appendLine()
                        appendLine("Caused by: ${cause::class.qualifiedName}: ${cause.message}")
                        appendLine(cause.stackTraceToString())
                        cause = cause.cause
                        depth++
                    }
                }

                Log.e(TAG, crashReport, throwable)

                // Persist crash report to storage
                runCatching {
                    File(filesDir, "last_crash.txt").writeText(crashReport)
                }

                // If not already in crash activity process, launch CrashActivity
                runCatching {
                    val intent = CrashActivity.createIntent(this@MyApplication, crashReport)
                    startActivity(intent)
                }
            } catch (t: Throwable) {
                Log.e(TAG, "Error in UncaughtExceptionHandler", t)
            } finally {
                // Terminate crashing process or pass to system default handler
                defaultHandler?.uncaughtException(thread, throwable)
                    ?: android.os.Process.killProcess(android.os.Process.myPid())
            }
        }
    }

    override fun onTerminate() {
        appScope.cancel()
        super.onTerminate()
    }
}
