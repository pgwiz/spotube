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

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.input.rememberTextFieldState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import co.touchlab.kermit.Severity
import com.composeunstyled.Text
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.jetbrains.compose.resources.stringResource
import org.koin.compose.viewmodel.koinViewModel
import org.spotube.millennium.composables.MillenniumButton
import org.spotube.millennium.composables.MillenniumButtonStyle
import org.spotube.millennium.composables.MillenniumIcon
import org.spotube.millennium.composables.MillenniumIconButton
import org.spotube.millennium.composables.MillenniumIconButtonStyle
import org.spotube.millennium.composables.MillenniumTextField
import org.spotube.spotube.core.logger.LogEntry
import org.spotube.spotube.core.ui.component.ApplicationMainBar
import org.spotube.spotube.core.ui.theming.AppTheme
import org.spotube.spotube.modules.shell.LocalAppShellBottomInset
import org.spotube.spotube.resources.iconsax.Iconsax
import org.spotube.spotube.resources.iconsax.IconsaxDocumentText
import org.spotube.spotube.resources.iconsax.IconsaxShare
import org.spotube.spotube.resources.iconsax.IconsaxTrash
import spotube.composeapp.generated.resources.Res
import spotube.composeapp.generated.resources.settings_logs_audio_plugin
import spotube.composeapp.generated.resources.settings_logs_builtin_plugin
import spotube.composeapp.generated.resources.settings_logs_clear
import spotube.composeapp.generated.resources.settings_logs_copied
import spotube.composeapp.generated.resources.settings_logs_copy
import spotube.composeapp.generated.resources.settings_logs_current_track
import spotube.composeapp.generated.resources.settings_logs_diagnostics_title
import spotube.composeapp.generated.resources.settings_logs_empty
import spotube.composeapp.generated.resources.settings_logs_filter_all
import spotube.composeapp.generated.resources.settings_logs_filter_errors
import spotube.composeapp.generated.resources.settings_logs_filter_warnings
import spotube.composeapp.generated.resources.settings_logs_hide_stacktrace
import spotube.composeapp.generated.resources.settings_logs_no_track
import spotube.composeapp.generated.resources.settings_logs_proxy_server
import spotube.composeapp.generated.resources.settings_logs_search_hint
import spotube.composeapp.generated.resources.settings_logs_show_stacktrace
import spotube.composeapp.generated.resources.settings_logs_status_active
import spotube.composeapp.generated.resources.settings_logs_status_starting
import spotube.composeapp.generated.resources.settings_logs_title

@Composable
fun LogsScreen(
    viewModel: LogsViewModel = koinViewModel()
) {
    Column(modifier = Modifier.fillMaxSize()) {
        ApplicationMainBar(
            backButton = true,
            title = { Text(stringResource(Res.string.settings_logs_title)) }
        )
        LogsContent(
            viewModel = viewModel,
            modifier = Modifier.weight(1f)
        )
    }
}

@Composable
internal fun LogsContent(
    viewModel: LogsViewModel = koinViewModel(),
    modifier: Modifier = Modifier
) {
    val state by viewModel.state.collectAsStateWithLifecycle()
    val shellBottomInset = LocalAppShellBottomInset.current
    val clipboardManager = LocalClipboardManager.current
    val coroutineScope = rememberCoroutineScope()

    val searchState = rememberTextFieldState(state.searchQuery)
    LaunchedEffect(searchState.text) {
        viewModel.onSearchQueryChanged(searchState.text.toString())
    }

    var copiedFeedback by remember { mutableStateOf(false) }
    val listState = rememberLazyListState()

    Column(modifier = modifier.fillMaxSize().padding(horizontal = 16.dp)) {
        // Diagnostic status overview card
        Box(
            modifier = Modifier
                .fillMaxWidth()
                .padding(vertical = 8.dp)
                .clip(RoundedCornerShape(12.dp))
                .background(AppTheme.colors.surfaceVariant.copy(alpha = 0.5f))
                .padding(12.dp)
        ) {
            Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
                Text(
                    text = stringResource(Res.string.settings_logs_diagnostics_title),
                    style = AppTheme.typography.labelLarge,
                    fontWeight = FontWeight.Bold,
                    color = AppTheme.colors.primary
                )
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    Text(
                        text = stringResource(Res.string.settings_logs_proxy_server),
                        style = AppTheme.typography.bodySmall,
                        color = AppTheme.colors.onSurfaceVariant
                    )
                    Text(
                        text = if (state.diagnostics.isProxyActive) {
                            "127.0.0.1:${state.diagnostics.proxyPort} (${stringResource(Res.string.settings_logs_status_active)})"
                        } else {
                            stringResource(Res.string.settings_logs_status_starting)
                        },
                        style = AppTheme.typography.bodySmall,
                        fontWeight = FontWeight.Medium,
                        color = if (state.diagnostics.isProxyActive) AppTheme.colors.primary else AppTheme.colors.onSurfaceVariant
                    )
                }
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    Text(
                        text = stringResource(Res.string.settings_logs_audio_plugin),
                        style = AppTheme.typography.bodySmall,
                        color = AppTheme.colors.onSurfaceVariant
                    )
                    Text(
                        text = state.diagnostics.audioPluginId ?: stringResource(Res.string.settings_logs_builtin_plugin),
                        style = AppTheme.typography.bodySmall,
                        fontWeight = FontWeight.Medium,
                        color = AppTheme.colors.onSurface
                    )
                }
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    Text(
                        text = stringResource(Res.string.settings_logs_current_track),
                        style = AppTheme.typography.bodySmall,
                        color = AppTheme.colors.onSurfaceVariant
                    )
                    Text(
                        text = state.diagnostics.currentTrackTitle ?: stringResource(Res.string.settings_logs_no_track),
                        style = AppTheme.typography.bodySmall,
                        fontWeight = FontWeight.Medium,
                        color = AppTheme.colors.onSurface
                    )
                }
            }
        }

        // Action & Filter row
        Row(
            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            MillenniumTextField(
                state = searchState,
                placeholder = stringResource(Res.string.settings_logs_search_hint),
                modifier = Modifier.weight(1f)
            )

            MillenniumButton(
                onClick = {
                    val fullLogs = viewModel.getFormattedLogsForExport()
                    clipboardManager.setText(AnnotatedString(fullLogs))
                    copiedFeedback = true
                    coroutineScope.launch {
                        delay(2000)
                        copiedFeedback = false
                    }
                },
                style = MillenniumButtonStyle.Secondary
            ) {
                MillenniumIcon(
                    imageVector = Iconsax.IconsaxShare,
                    contentDescription = null,
                    modifier = Modifier.size(16.dp)
                )
                Spacer(modifier = Modifier.width(4.dp))
                Text(
                    text = if (copiedFeedback) {
                        stringResource(Res.string.settings_logs_copied)
                    } else {
                        stringResource(Res.string.settings_logs_copy)
                    },
                    style = AppTheme.typography.labelSmall
                )
            }

            MillenniumIconButton(
                onClick = { viewModel.onClearLogs() },
                style = MillenniumIconButtonStyle.Secondary,
                contentDescription = stringResource(Res.string.settings_logs_clear)
            ) {
                MillenniumIcon(
                    imageVector = Iconsax.IconsaxTrash,
                    contentDescription = null,
                    modifier = Modifier.size(16.dp)
                )
            }
        }

        // Filter chips row
        Row(
            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            LogFilterChip(
                label = stringResource(Res.string.settings_logs_filter_all),
                selected = state.selectedFilter == LogFilter.ALL,
                count = state.totalCount,
                onClick = { viewModel.onFilterChanged(LogFilter.ALL) }
            )
            LogFilterChip(
                label = stringResource(Res.string.settings_logs_filter_errors),
                selected = state.selectedFilter == LogFilter.ERRORS,
                count = state.errorCount,
                chipColor = Color(0xFFE53935),
                onClick = { viewModel.onFilterChanged(LogFilter.ERRORS) }
            )
            LogFilterChip(
                label = stringResource(Res.string.settings_logs_filter_warnings),
                selected = state.selectedFilter == LogFilter.WARNINGS,
                count = state.warningCount,
                chipColor = Color(0xFFFB8C00),
                onClick = { viewModel.onFilterChanged(LogFilter.WARNINGS) }
            )
        }

        // Logs list
        if (state.filteredLogs.isEmpty()) {
            Box(
                modifier = Modifier.weight(1f).fillMaxWidth(),
                contentAlignment = Alignment.Center
            ) {
                Column(
                    horizontalAlignment = Alignment.CenterHorizontally,
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    MillenniumIcon(
                        imageVector = Iconsax.IconsaxDocumentText,
                        contentDescription = null,
                        tint = AppTheme.colors.onSurfaceVariant,
                        modifier = Modifier.size(36.dp)
                    )
                    Text(
                        text = stringResource(Res.string.settings_logs_empty),
                        style = AppTheme.typography.bodyMedium,
                        color = AppTheme.colors.onSurfaceVariant
                    )
                }
            }
        } else {
            LazyColumn(
                state = listState,
                modifier = Modifier.weight(1f).fillMaxWidth(),
                contentPadding = PaddingValues(top = 4.dp, bottom = 16.dp + shellBottomInset),
                verticalArrangement = Arrangement.spacedBy(4.dp)
            ) {
                items(state.filteredLogs, key = { it.id }) { logEntry ->
                    LogEntryCard(entry = logEntry)
                }
            }
        }
    }
}

@Composable
private fun LogFilterChip(
    label: String,
    selected: Boolean,
    count: Int,
    chipColor: Color = AppTheme.colors.primary,
    onClick: () -> Unit
) {
    Box(
        modifier = Modifier
            .clip(RoundedCornerShape(8.dp))
            .background(
                if (selected) chipColor.copy(alpha = 0.2f) else AppTheme.colors.surfaceVariant.copy(alpha = 0.3f)
            )
            .clickable(onClick = onClick)
            .padding(horizontal = 10.dp, vertical = 6.dp)
    ) {
        Text(
            text = "$label ($count)",
            style = AppTheme.typography.labelSmall,
            fontWeight = if (selected) FontWeight.Bold else FontWeight.Normal,
            color = if (selected) chipColor else AppTheme.colors.onSurface
        )
    }
}

@Composable
private fun LogEntryCard(entry: LogEntry) {
    var expanded by remember { mutableStateOf(false) }

    val severityColor = when (entry.severity) {
        Severity.Error, Severity.Assert -> Color(0xFFE53935)
        Severity.Warn -> Color(0xFFFB8C00)
        Severity.Info -> AppTheme.colors.primary
        Severity.Debug -> AppTheme.colors.onSurfaceVariant
        Severity.Verbose -> AppTheme.colors.onSurfaceVariant.copy(alpha = 0.7f)
    }

    Box(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(8.dp))
            .background(AppTheme.colors.surfaceVariant.copy(alpha = 0.35f))
            .clickable(enabled = entry.throwable != null) { expanded = !expanded }
            .padding(8.dp)
    ) {
        Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                // Severity label
                Box(
                    modifier = Modifier
                        .clip(RoundedCornerShape(4.dp))
                        .background(severityColor.copy(alpha = 0.18f))
                        .padding(horizontal = 4.dp, vertical = 1.dp)
                ) {
                    Text(
                        text = entry.severity.name.take(4),
                        fontSize = 10.sp,
                        fontWeight = FontWeight.Bold,
                        color = severityColor
                    )
                }

                // Timestamp
                Text(
                    text = entry.formattedTime,
                    fontSize = 11.sp,
                    fontFamily = FontFamily.Monospace,
                    color = AppTheme.colors.onSurfaceVariant
                )

                // Tag
                Text(
                    text = "[${entry.tag}]",
                    fontSize = 11.sp,
                    fontFamily = FontFamily.Monospace,
                    fontWeight = FontWeight.SemiBold,
                    color = AppTheme.colors.onSurface
                )
            }

            // Message
            Text(
                text = entry.message,
                fontSize = 12.sp,
                fontFamily = FontFamily.Monospace,
                color = AppTheme.colors.onSurface,
                lineHeight = 16.sp
            )

            // Throwable toggle / stack trace
            if (entry.throwable != null) {
                Text(
                    text = if (expanded) {
                        stringResource(Res.string.settings_logs_hide_stacktrace)
                    } else {
                        val className = entry.throwable::class.simpleName ?: "Throwable"
                        "${stringResource(Res.string.settings_logs_show_stacktrace)} ($className)"
                    },
                    fontSize = 11.sp,
                    fontWeight = FontWeight.Medium,
                    color = severityColor
                )
                AnimatedVisibility(visible = expanded) {
                    Text(
                        text = entry.throwable.stackTraceToString(),
                        fontSize = 10.sp,
                        fontFamily = FontFamily.Monospace,
                        color = AppTheme.colors.onSurfaceVariant,
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(top = 4.dp)
                            .clip(RoundedCornerShape(4.dp))
                            .background(Color.Black.copy(alpha = 0.3f))
                            .padding(6.dp)
                    )
                }
            }
        }
    }
}
