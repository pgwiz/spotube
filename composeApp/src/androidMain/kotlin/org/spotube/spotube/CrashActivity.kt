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

import android.app.Activity
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.graphics.Color
import android.graphics.Typeface
import android.os.Bundle
import android.widget.Button
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import java.io.File

class CrashActivity : Activity() {
    companion object {
        const val EXTRA_CRASH_INFO = "extra_crash_info"

        fun createIntent(context: Context, crashInfo: String): Intent {
            return Intent(context, CrashActivity::class.java).apply {
                putExtra(EXTRA_CRASH_INFO, crashInfo)
                flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val crashInfo = intent.getStringExtra(EXTRA_CRASH_INFO)
            ?: runCatching {
                File(filesDir, "last_crash.txt").readText()
            }.getOrDefault("No crash details available.")

        val rootLayout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(Color.parseColor("#121212"))
            setPadding(48, 64, 48, 48)
        }

        val titleView = TextView(this).apply {
            text = "Spotube Startup Error"
            textSize = 22f
            setTypeface(null, Typeface.BOLD)
            setTextColor(Color.parseColor("#FF5252"))
            setPadding(0, 0, 0, 16)
        }
        rootLayout.addView(titleView)

        val subtitleView = TextView(this).apply {
            text = "Spotube encountered an unexpected error while launching. You can copy the details below to report or debug the issue."
            textSize = 14f
            setTextColor(Color.parseColor("#E0E0E0"))
            setPadding(0, 0, 0, 24)
        }
        rootLayout.addView(subtitleView)

        val buttonBar = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, 0, 0, 24)
        }

        val copyButton = Button(this).apply {
            text = "Copy Error Details"
            setBackgroundColor(Color.parseColor("#1DB954"))
            setTextColor(Color.WHITE)
            setOnClickListener {
                try {
                    val clipboard = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
                    val clip = ClipData.newPlainText("Spotube Crash Report", crashInfo)
                    clipboard.setPrimaryClip(clip)
                    Toast.makeText(this@CrashActivity, "Crash details copied to clipboard!", Toast.LENGTH_SHORT).show()
                } catch (e: Throwable) {
                    Toast.makeText(this@CrashActivity, "Failed to copy: ${e.message}", Toast.LENGTH_SHORT).show()
                }
            }
        }
        val copyParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f).apply {
            marginEnd = 16
        }
        buttonBar.addView(copyButton, copyParams)

        val restartButton = Button(this).apply {
            text = "Restart App"
            setBackgroundColor(Color.parseColor("#333333"))
            setTextColor(Color.WHITE)
            setOnClickListener {
                try {
                    val restartIntent = Intent(this@CrashActivity, MainActivity::class.java).apply {
                        flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK
                    }
                    startActivity(restartIntent)
                } finally {
                    finish()
                }
            }
        }
        val restartParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
        buttonBar.addView(restartButton, restartParams)

        rootLayout.addView(buttonBar)

        val clearCacheButton = Button(this).apply {
            text = "Clear Cache & Restart"
            setBackgroundColor(Color.parseColor("#262626"))
            setTextColor(Color.parseColor("#B0B0B0"))
            setOnClickListener {
                runCatching {
                    cacheDir.deleteRecursively()
                    File(filesDir, "spotube_cookies.preferences_pb").delete()
                    File(filesDir, "last_crash.txt").delete()
                }
                Toast.makeText(this@CrashActivity, "Cache cleared! Restarting...", Toast.LENGTH_SHORT).show()
                try {
                    val restartIntent = Intent(this@CrashActivity, MainActivity::class.java).apply {
                        flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK
                    }
                    startActivity(restartIntent)
                } finally {
                    finish()
                }
            }
        }
        val clearCacheParams = LinearLayout.LayoutParams(
            LinearLayout.LayoutParams.MATCH_PARENT,
            LinearLayout.LayoutParams.WRAP_CONTENT
        ).apply {
            bottomMargin = 24
        }
        rootLayout.addView(clearCacheButton, clearCacheParams)

        val scrollView = ScrollView(this).apply {
            setBackgroundColor(Color.parseColor("#1E1E1E"))
            setPadding(24, 24, 24, 24)
        }

        val logView = TextView(this).apply {
            text = crashInfo
            textSize = 12f
            setTypeface(Typeface.MONOSPACE)
            setTextColor(Color.parseColor("#CCCCCC"))
            setTextIsSelectable(true)
        }
        scrollView.addView(logView)

        val scrollParams = LinearLayout.LayoutParams(
            LinearLayout.LayoutParams.MATCH_PARENT,
            0,
            1f
        )
        rootLayout.addView(scrollView, scrollParams)

        setContentView(rootLayout)
    }
}
