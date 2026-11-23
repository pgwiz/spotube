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

import android.content.Intent
import android.os.Bundle
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import org.spotube.spotube.core.deeplink.ExternalUriHandler
import org.spotube.spotube.core.newpipe.NewPipeDownloader
import org.spotube.spotube.core.paths.Paths
import org.spotube.spotube.media.PlaybackService
import io.github.vinceglb.filekit.FileKit
import io.github.vinceglb.filekit.dialogs.init

class MainActivity : ComponentActivity() {
    companion object {
        private const val TAG = "MainActivity"
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        try {
            enableEdgeToEdge()
        } catch (e: Throwable) {
            Log.w(TAG, "enableEdgeToEdge failed", e)
        }

        super.onCreate(savedInstanceState)

        try {
            FileKit.init(this)
        } catch (e: Throwable) {
            Log.e(TAG, "FileKit.init failed", e)
        }

        try {
            NewPipeDownloader.init(Paths())
        } catch (e: Throwable) {
            Log.e(TAG, "NewPipeDownloader.init failed", e)
        }
        
        // Plain start from the foreground; Media3 promotes PlaybackService itself once playback starts.
        // Catch ForegroundServiceStartNotAllowedException, SecurityException, or any lifecycle restriction.
        try {
            startService(Intent(this, PlaybackService::class.java))
        } catch (e: Throwable) {
            Log.w(TAG, "startService(PlaybackService) failed or disallowed", e)
        }
        
        try {
            intent?.dataString?.let(ExternalUriHandler::onNewUri)
        } catch (e: Throwable) {
            Log.w(TAG, "ExternalUriHandler failed on initial intent", e)
        }

        try {
            setContent {
                App()
            }
        } catch (e: Throwable) {
            Log.e(TAG, "Fatal exception during setContent initialization", e)
            try {
                val intent = CrashActivity.createIntent(this, buildString {
                    appendLine("Fatal Compose Error in MainActivity:")
                    appendLine(e.stackTraceToString())
                })
                startActivity(intent)
            } finally {
                finish()
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        try {
            intent.dataString?.let(ExternalUriHandler::onNewUri)
        } catch (e: Throwable) {
            Log.w(TAG, "ExternalUriHandler failed on onNewIntent", e)
        }
    }
}
