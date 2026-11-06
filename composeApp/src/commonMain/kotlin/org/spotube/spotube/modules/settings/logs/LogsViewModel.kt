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

package org.spotube.spotube.modules.settings.logs

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import co.touchlab.kermit.Severity
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.stateIn
import org.spotube.spotube.core.audioplayer.AudioPlayerQueue
import org.spotube.spotube.core.audioplayer.QueueEntry
import org.spotube.spotube.core.logger.AppLogBuffer
import org.spotube.spotube.core.logger.LogEntry
import org.spotube.spotube.core.server.LocalServer
import org.spotube.spotube.modules.plugin.PluginManager

enum class LogFilter {
    ALL,
    ERRORS,
    WARNINGS
}

data class SystemDiagnosticsState(
    val proxyPort: Int?,
    val isProxyActive: Boolean,
    val audioPluginId: String?,
    val currentTrackTitle: String?
)

data class LogsUiState(
    val diagnostics: SystemDiagnosticsState = SystemDiagnosticsState(
        proxyPort = null,
        isProxyActive = false,
        audioPluginId = null,
        currentTrackTitle = null
    ),
    val filteredLogs: List<LogEntry> = emptyList(),
    val searchQuery: String = "",
    val selectedFilter: LogFilter = LogFilter.ALL,
    val totalCount: Int = 0,
    val errorCount: Int = 0,
    val warningCount: Int = 0
)

class LogsViewModel(
    pluginManager: PluginManager,
    audioPlayerQueue: AudioPlayerQueue,
    localServer: LocalServer? = null
) : ViewModel() {

    private val searchQueryState = MutableStateFlow("")
    private val selectedFilterState = MutableStateFlow(LogFilter.ALL)

    private val localServerPort: StateFlow<Int?> =
        localServer?.port ?: MutableStateFlow(null)

    private val diagnosticsFlow: Flow<SystemDiagnosticsState> = combine(
        localServerPort,
        pluginManager.selectedAudioPlugin,
        audioPlayerQueue.currentQueueEntryFlow
    ) { port, audioPlugin, queueEntry ->
        val trackTitle = queueEntry?.let { entry ->
            when (entry) {
                is QueueEntry.StreamingTrack -> entry.track.title
                is QueueEntry.LocalTrack -> entry.name
            }
        }
        SystemDiagnosticsState(
            proxyPort = port,
            isProxyActive = port != null,
            audioPluginId = audioPlugin?.pluginId,
            currentTrackTitle = trackTitle
        )
    }

    private val filterStateFlow = combine(
        searchQueryState,
        selectedFilterState
    ) { query, filter ->
        query to filter
    }

    val state: StateFlow<LogsUiState> = combine(
        AppLogBuffer.logs,
        filterStateFlow,
        diagnosticsFlow
    ) { logs: List<LogEntry>, (searchQuery, selectedFilter), diagnostics ->
        val trimmedQuery = searchQuery.trim().lowercase()

        val totalCount = logs.size
        val errorCount = logs.count { it.severity == Severity.Error || it.severity == Severity.Assert }
        val warningCount = logs.count { it.severity == Severity.Warn }

        val filtered = logs.filter { entry ->
            val matchesSeverity = when (selectedFilter) {
                LogFilter.ALL -> true
                LogFilter.ERRORS -> entry.severity == Severity.Error || entry.severity == Severity.Assert
                LogFilter.WARNINGS -> entry.severity == Severity.Warn || entry.severity == Severity.Error || entry.severity == Severity.Assert
            }
            val matchesSearch = if (trimmedQuery.isBlank()) {
                true
            } else {
                entry.tag.lowercase().contains(trimmedQuery) || entry.message.lowercase().contains(trimmedQuery)
            }
            matchesSeverity && matchesSearch
        }.asReversed()

        LogsUiState(
            diagnostics = diagnostics,
            filteredLogs = filtered,
            searchQuery = searchQuery,
            selectedFilter = selectedFilter,
            totalCount = totalCount,
            errorCount = errorCount,
            warningCount = warningCount
        )
    }.stateIn(
        scope = viewModelScope,
        started = SharingStarted.WhileSubscribed(5_000),
        initialValue = LogsUiState()
    )

    fun onSearchQueryChanged(query: String) {
        searchQueryState.value = query
    }

    fun onFilterChanged(filter: LogFilter) {
        selectedFilterState.value = filter
    }

    fun onClearLogs() {
        AppLogBuffer.clear()
    }

    fun getFormattedLogsForExport(): String {
        return AppLogBuffer.formatAllLogs()
    }
}
