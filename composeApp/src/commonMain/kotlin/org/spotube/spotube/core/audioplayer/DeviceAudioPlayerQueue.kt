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

package org.spotube.spotube.core.audioplayer

import org.spotube.plugin_interfaces.plugin_apis.audio.StreamProtocol
import org.spotube.plugin_interfaces.plugin_apis.metadata.artist.MetadataArtist
import org.spotube.plugin_interfaces.plugin_apis.metadata.common.Thumbnail
import org.spotube.plugin_interfaces.plugin_apis.metadata.track.MetadataTrack
import org.spotube.spotube.core.di.injectLogger
import org.spotube.spotube.core.newpipe.NewPipeService
import org.spotube.spotube.core.newpipe.SearchKind
import org.spotube.spotube.core.server.LocalServer
import org.spotube.spotube.modules.blacklist.BlacklistRepository
import org.spotube.spotube.modules.library.local_tracks.media.LocalMediaCacheRepository
import org.spotube.spotube.modules.plugin.PluginProvider
import org.spotube.spotube.modules.settings.SettingsProvider
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.stateIn
import io.ktor.client.HttpClient
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.prepareRequest
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.utils.io.cancel
import io.ktor.utils.io.readAvailable
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.koin.core.component.KoinComponent
import org.koin.core.component.get
import org.koin.core.component.inject
import org.spotube.spotube.PlatformType
import org.spotube.spotube.core.network.HttpClientFactory
import org.spotube.spotube.core.server.RamHeadBufferPool
import org.spotube.spotube.core.server.StreamInfo
import org.spotube.spotube.core.server.StreamingUrlRepository
import org.spotube.spotube.core.server.applyStreamClientHeaders
import org.spotube.spotube.getPlatform
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

class DeviceAudioPlayerQueue(
    private val audioPlayer: AudioPlayerInterface,
    private val settingsProvider: SettingsProvider,
    private val repository: QueueStateRepository,
    private val pluginProvider: PluginProvider,
    private val blacklistRepository: BlacklistRepository) : AudioPlayerQueue, KoinComponent {

    private val logger by injectLogger<DeviceAudioPlayerQueue>()
    private val streamingUrlRepository by inject<StreamingUrlRepository>()
    private val newPipeService by inject<NewPipeService>()
    private val httpClientFactory by inject<HttpClientFactory>()
    private val httpClient: HttpClient by lazy {
        httpClientFactory.create {
            install(HttpTimeout) {
                requestTimeoutMillis = 15_000
                connectTimeoutMillis = 10_000
                socketTimeoutMillis = 15_000
            }
        }
    }

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var prefetchJob: Job? = null

    private val entryByMediaUrl = MutableStateFlow<Map<String, QueueEntry>>(emptyMap())
    private val currentCollectionEntryState = MutableStateFlow<QueueCollectionEntry?>(null)
    private val collectionHistoryState = MutableStateFlow<List<QueueCollectionEntry>>(emptyList())

    private var isRestoringPersistedState = true

    // Eagerly: callers read `.value` (the stream proxy looks up the track it's asked to play,
    // Jam follows the room). With WhileSubscribed, `.value` went stale whenever nothing on
    // screen was collecting, so the proxy answered "not in the queue" (404) for new tracks.
    override val queueFlow: StateFlow<List<QueueEntry>> = combine(
        audioPlayer.playlistFlow,
        entryByMediaUrl) { playlist, knownEntries ->
        playlist.map { mediaItem ->
            knownEntries[mediaItem.url] ?: mediaItem.toFallbackQueueEntry()
        }
    }.stateIn(scope, SharingStarted.Eagerly, emptyList())

    override val currentQueueEntryFlow: StateFlow<QueueEntry?> = combine(
        audioPlayer.currentMediaItemFlow,
        entryByMediaUrl) { mediaItem, knownEntries ->
        mediaItem?.let { knownEntries[it.url] ?: it.toFallbackQueueEntry() }
    }.stateIn(scope, SharingStarted.Eagerly, null)

    override val currentCollectionEntryFlow: StateFlow<QueueCollectionEntry?> =
        currentCollectionEntryState.asStateFlow()

    override val collectionHistoryFlow: StateFlow<List<QueueCollectionEntry>> =
        collectionHistoryState.asStateFlow()

    init {
        logger.d { "Initializing DeviceAudioPlayerQueue" }
        scope.launch {
            logger.d { "Starting restoration of persisted queue state" }
            restorePersistedState()
            isRestoringPersistedState = false
            logger.d { "Persisted queue state restoration completed" }
        }

        scope.launch {
            audioPlayer.playlistFlow.collect {
                logger.d { "Playlist changed, persisting queue state" }
                persistQueueStateIfReady()
            }
        }

        scope.launch {
            audioPlayer.errorFlow.collect { error ->
                logger.e(error) { "AudioPlayer error encountered in queue: ${error.message}" }
            }
        }

        scope.launch {
            audioPlayer.currentMediaItemFlow.collect {
                logger.d { "Current media item changed, persisting queue state" }
                persistQueueStateIfReady()
                lastPrefetchedTrackId = null
                cancelOngoingPrefetch()
            }
        }

        scope.launch {
            combine(
                audioPlayer.currentMediaItemFlow,
                audioPlayer.positionFlow,
                audioPlayer.durationFlow
            ) { currentMedia, position, duration ->
                Triple(currentMedia, position, duration)
            }.collect { (currentMedia, position, duration) ->
                checkAndPrefetchNextTrack(currentMedia, position, duration)
            }
        }

        scope.launch {
            combine(
                audioPlayer.currentMediaItemFlow,
                audioPlayer.playlistFlow,
                audioPlayer.loopStateFlow) { currentMedia, playlist, loopState ->
                Triple(currentMedia, playlist, loopState)
            }.collect { (currentMedia, playlist, loopState) ->
                if (currentMedia == null || playlist.isEmpty()) return@collect
                if (loopState == LoopState.ALL) return@collect

                val currentIndex = playlist.indexOfFirst { it.url == currentMedia.url }
                if (currentIndex < 0) return@collect

                val tracksAfterCurrent = playlist.size - currentIndex - 1
                if (tracksAfterCurrent <= 0) {
                    handleQueueCompletion()
                }
            }
        }
    }

    override suspend fun load(
        entries: List<QueueEntry>,
        autoPlay: Boolean,
        startPosition: Int,
        collectionEntry: QueueCollectionEntry?) {
        cancelOngoingPrefetch()
        lastPrefetchedTrackId = null
        logger.i { "Loading queue with ${entries.size} entries, autoPlay=$autoPlay, startPosition=$startPosition" }
        val resolved = resolveMediaItems(entries)
        val mediaItems = resolved.map { it.first }
        if (mediaItems.isEmpty()) {
            logger.w { "load: no valid media items after filtering blanks" }
        }
        val normalizedStartPosition =
            if (mediaItems.isEmpty()) 0 else startPosition.coerceIn(0, mediaItems.lastIndex)
        logger.d { "Normalized start position: $startPosition -> $normalizedStartPosition" }
        entryByMediaUrl.value = entriesByUrl(resolved)
        updateCollectionContext(collectionEntry)
        audioPlayer.load(mediaItems, autoPlay, normalizedStartPosition)
        persistQueueStateIfReady()
        logger.i { "Queue loaded successfully" }
    }

    override suspend fun addToQueue(entry: QueueEntry) {
        val mediaItem = entry.toMediaItem() ?: run {
            logger.w { "addToQueue: skipping entry with blank url" }
            return
        }
        logger.d { "Adding entry to queue: ${mediaItem.title}" }
        entryByMediaUrl.update { it + (mediaItem.url to entry) }
        audioPlayer.addMediaItem(mediaItem)
        persistQueueStateIfReady()
        logger.d { "Entry added successfully" }
    }

    override suspend fun addAllToQueue(entries: List<QueueEntry>, collectionEntry: QueueCollectionEntry?) {
        if (entries.isEmpty()) {
            logger.d { "addAllToQueue called with empty list, skipping" }
            return
        }

        logger.i { "Adding ${entries.size} entries to queue" }
        val paired = entries.mapNotNull { entry -> entry.toMediaItem()?.let { mediaItem -> mediaItem to entry } }
        if (paired.isEmpty()) {
            logger.w { "addAllToQueue: all entries had blank urls, skipping" }
            return
        }
        val mediaItems = paired.map { it.first }
        entryByMediaUrl.update { current ->
            current + paired.associate { (mediaItem, entry) -> mediaItem.url to entry }
        }

        updateCollectionContext(collectionEntry)

        mediaItems.forEach { mediaItem ->
            audioPlayer.addMediaItem(mediaItem)
        }

        persistQueueStateIfReady()
        logger.d { "All entries added successfully" }
    }

    override suspend fun addAllAfterCurrent(entries: List<QueueEntry>) {
        if (entries.isEmpty()) {
            logger.d { "addAllAfterCurrent called with empty list, skipping" }
            return
        }

        logger.i { "Adding ${entries.size} entries after current position" }
        val paired = entries.mapNotNull { entry -> entry.toMediaItem()?.let { mediaItem -> mediaItem to entry } }
        if (paired.isEmpty()) {
            logger.w { "addAllAfterCurrent: all entries had blank urls, skipping" }
            return
        }
        val mediaItems = paired.map { it.first }
        entryByMediaUrl.update { current ->
            current + paired.associate { (mediaItem, entry) -> mediaItem.url to entry }
        }

        mediaItems.forEach { mediaItem ->
            audioPlayer.insertMediaItemAtNextIndex(mediaItem)
        }

        persistQueueStateIfReady()
        logger.d { "All entries added after current successfully" }
    }

    override suspend fun removeFromQueue(entry: QueueEntry) {
        val mediaUrl = entryByMediaUrl.value.entries.firstOrNull { it.value == entry }?.key
            ?: entry.toMediaItem()?.url
            ?: return
        removeFromQueueByMediaUrl(mediaUrl)
    }

    override suspend fun removeFromQueueByMediaUrl(mediaUrl: String) {
        val mediaItem = audioPlayer.playlistFlow.value.firstOrNull { it.url == mediaUrl }
        if (mediaItem == null) {
            logger.w { "Attempted to remove non-existent media item: $mediaUrl" }
            return
        }
        logger.d { "Removing media item from queue: ${mediaItem.title}" }
        audioPlayer.removeMediaItem(mediaItem)
        entryByMediaUrl.update { it - mediaUrl }
        persistQueueStateIfReady()
        logger.d { "Media item removed successfully" }
    }

    override suspend fun move(fromIndex: Int, toIndex: Int) {
        logger.d { "Moving queue item from index $fromIndex to $toIndex" }
        audioPlayer.moveMediaItem(fromIndex, toIndex)
        persistQueueStateIfReady()
        logger.d { "Queue item moved successfully" }
    }

    override suspend fun jumpTo(index: Int, autoPlay: Boolean) {
        cancelOngoingPrefetch()
        lastPrefetchedTrackId = null
        val queueSize = audioPlayer.playlistFlow.value.size
        if (queueSize == 0) {
            logger.w { "jumpTo called with empty queue" }
            return
        }

        val normalizedIndex = index.coerceIn(0, queueSize - 1)
        logger.d { "Jumping to queue index $index (normalized: $normalizedIndex), autoPlay=$autoPlay" }
        audioPlayer.jumpTo(normalizedIndex)
        if (autoPlay) {
            audioPlayer.play()
        }
        persistQueueStateIfReady()
    }

    override suspend fun reloadCurrent() {
        val current = currentQueueEntryFlow.value ?: run {
            logger.w { "reloadCurrent called with no current entry" }
            return
        }
        val queue = queueFlow.value

        // Using separate current index entry as the current track's source url has changed so it won't match
        val index = queue.indexOfFirst { entry ->
            when (entry) {
                is QueueEntry.StreamingTrack if current is QueueEntry.StreamingTrack ->
                    entry.track.id == current.track.id

                is QueueEntry.LocalTrack if current is QueueEntry.LocalTrack ->
                    entry.url == current.url && entry.name == current.name

                else -> false
            }
        }
        if (index < 0) {
            logger.w { "reloadCurrent: current entry not found in queue" }
            return
        }
        logger.i { "Reloading current track at queue index $index" }
        jumpTo(index, autoPlay = true)
    }

    override suspend fun clear() {
        cancelOngoingPrefetch()
        lastPrefetchedTrackId = null
        logger.i { "Clearing queue and collection history" }
        entryByMediaUrl.value = emptyMap()
        currentCollectionEntryState.value = null
        collectionHistoryState.value = emptyList()
        audioPlayer.load(emptyList(), autoPlay = false, startPosition = 0)
        repository.clearState()
        logger.i { "Queue cleared successfully" }
    }

    override suspend fun getQueue(): List<QueueEntry> {
        val knownEntries = entryByMediaUrl.value
        return audioPlayer.playlistFlow.value.map { mediaItem ->
            knownEntries[mediaItem.url] ?: mediaItem.toFallbackQueueEntry()
        }
    }

    override suspend fun getCurrentQueueEntry(): QueueEntry? {
        val mediaItem = audioPlayer.currentMediaItemFlow.value ?: return null
        return entryByMediaUrl.value[mediaItem.url] ?: mediaItem.toFallbackQueueEntry()
    }

    override suspend fun getCurrentCollectionEntry(): QueueCollectionEntry? {
        return currentCollectionEntryState.value
    }

    override suspend fun getCollectionHistory(): List<QueueCollectionEntry> {
        return collectionHistoryState.value
    }

    private var isFetchingRecommendations = false
    private var endlessPlaybackSuppressed = false

    override fun setEndlessPlaybackSuppressed(suppressed: Boolean) {
        endlessPlaybackSuppressed = suppressed
    }

    private suspend fun handleQueueCompletion() {
        if (isFetchingRecommendations || endlessPlaybackSuppressed) return

        val settings = settingsProvider.settingsState.value ?: return
        if (!settings.enableEndlessPlayback) return

        val queue = getQueue()
        val streamingTracks = queue.filterIsInstance<QueueEntry.StreamingTrack>()
        if (streamingTracks.isEmpty()) return

        val seedTracks = if (streamingTracks.size <= 5) {
            streamingTracks
        } else {
            val shuffled = streamingTracks.shuffled(Random)
            shuffled.take(5)
        }

        val seedTrackIds = seedTracks.map { it.track.id }
        if (seedTrackIds.isEmpty()) return

        val validMetadataSeedIds = seedTrackIds.filter { !it.matches(Regex("^[a-zA-Z0-9_-]{11}$")) }

        isFetchingRecommendations = true
        logger.i { "Endless playback: fetching recommendations with ${seedTrackIds.size} seed tracks (${validMetadataSeedIds.size} metadata valid)" }

        val metadataService = pluginProvider.selectedMetadataPlugin.value
        var recommendations: List<MetadataTrack> = emptyList()

        if (metadataService != null && validMetadataSeedIds.isNotEmpty()) {
            recommendations = runCatchingNonCancellation {
                metadataService.use {
                    metadataTrackAPI.recommendationsBasedOnTracks(validMetadataSeedIds, limit = 20)
                }
            }.getOrDefault(emptyList())
        }

        if (recommendations.isEmpty()) {
            logger.i { "Endless playback: metadata recommendations unavailable or empty, falling back to native YouTube radio mix" }
            for (seedTrack in seedTracks) {
                val seed = seedTrack.track
                val videoId = if (seed.id.matches(Regex("^[a-zA-Z0-9_-]{11}$"))) {
                    seed.id
                } else {
                    seed.externalUri?.let { uri ->
                        Regex("""[?&]v=([a-zA-Z0-9_-]{11})(?:[&#]|$)""").find(uri)?.groupValues?.get(1)
                            ?: Regex("""youtu\.be/([a-zA-Z0-9_-]{11})(?:[?#/]|$)""").find(uri)?.groupValues?.get(1)
                            ?: Regex("""youtube\.com/(?:embed|v|shorts)/([a-zA-Z0-9_-]{11})(?:[?#/]|$)""").find(uri)?.groupValues?.get(1)
                    }
                }

                if (!videoId.isNullOrBlank()) {
                    val radioMix = runCatchingNonCancellation { newPipeService.getRadioMix(videoId) }.getOrDefault(emptyList())
                    val existingTrackIds = queue.filterIsInstance<QueueEntry.StreamingTrack>().map { it.track.id }.toSet()
                    val newRadioTracks = radioMix.filter { it.id !in existingTrackIds && it.id != videoId }
                    if (newRadioTracks.isNotEmpty()) {
                        logger.i { "Endless playback: got ${newRadioTracks.size} new tracks from YouTube radio mix for video $videoId" }
                        recommendations = newRadioTracks.map { video ->
                            MetadataTrack(
                                id = video.id,
                                title = video.title,
                                durationMs = video.durationMs,
                                trackNumber = null,
                                discNumber = null,
                                artists = listOf(
                                    MetadataArtist.Basic(
                                        id = video.uploader,
                                        name = video.uploader,
                                        thumbnails = emptyList(),
                                        externalUri = null
                                    )
                                ),
                                album = null,
                                thumbnails = if (video.thumbnailUrl.isNotBlank()) {
                                    listOf(Thumbnail(url = video.thumbnailUrl, width = 300, height = 300))
                                } else null,
                                explicit = null,
                                popularity = null,
                                isrcCode = null,
                                externalUri = "https://www.youtube.com/watch?v=${video.id}"
                            )
                        }
                        break
                    }
                }
            }
        }

        if (recommendations.isEmpty() && metadataService != null) {
            val albumId = seedTracks.mapNotNull { it.track.album?.id }.firstOrNull { it.isNotBlank() }
            if (!albumId.isNullOrBlank()) {
                logger.i { "Endless playback: radio mix unavailable or empty, falling back to album tracks for album $albumId" }
                val albumTracks = runCatching {
                    metadataService.use {
                        metadataAlbumAPI.getAlbumTracks(albumId).items
                    }
                }.getOrDefault(emptyList())

                val existingTrackIds = queue.filterIsInstance<QueueEntry.StreamingTrack>().map { it.track.id }.toSet()
                val newAlbumTracks = albumTracks.filter { it.id !in existingTrackIds }
                if (newAlbumTracks.isNotEmpty()) {
                    logger.i { "Endless playback: got ${newAlbumTracks.size} remaining album tracks for album $albumId" }
                    recommendations = newAlbumTracks
                }
            }
        }

        if (recommendations.isEmpty() && metadataService != null) {
            logger.i { "Endless playback: album tracks unavailable or empty, falling back to artist top tracks" }
            val existingTrackIds = queue.filterIsInstance<QueueEntry.StreamingTrack>().map { it.track.id }.toSet()
            val artistIds = seedTracks.flatMap { it.track.artists }.map { it.id }.filter { it.isNotBlank() && !it.contains(" ") && !it.matches(Regex("^[a-zA-Z0-9_-]{11}$")) }.distinct()
            for (artistId in artistIds) {
                val topTracks = runCatching {
                    metadataService.use {
                        metadataArtistAPI.getArtistTop10Tracks(artistId)
                    }
                }.getOrDefault(emptyList())

                val newTracks = topTracks.filter { it.id !in existingTrackIds }
                if (newTracks.isNotEmpty()) {
                    logger.i { "Endless playback: got ${newTracks.size} top tracks for artist $artistId" }
                    recommendations = newTracks
                    break
                }
            }
        }

        try {
            if (recommendations.isEmpty()) {
                logger.w { "Endless playback: no recommendations, radio mix, album tracks, or artist top tracks found" }
                isFetchingRecommendations = false
                return
            }

            val blacklistedTrackIds = blacklistRepository.blacklistedTracks.first().map { it.id }.toSet()
            val blacklistedArtistIds = blacklistRepository.blacklistedArtists.first().map { it.id }.toSet()

            val filteredRecommendations = recommendations.filter { track ->
                val trackBlacklisted = track.id in blacklistedTrackIds
                val artistBlacklisted = track.artists.any { it.id in blacklistedArtistIds }
                !trackBlacklisted && !artistBlacklisted
            }

            if (filteredRecommendations.isEmpty()) {
                logger.w { "Endless playback: all recommendations were blacklisted" }
                isFetchingRecommendations = false
                return
            }

            val newEntries = filteredRecommendations.map { track ->
                QueueEntry.StreamingTrack(track = track, url = "")
            }

            logger.i { "Endless playback: adding ${newEntries.size} recommended tracks to queue" }
            addAllToQueue(newEntries)
        } catch (e: Exception) {
            logger.e(e) { "Endless playback: failed to add recommendations to queue" }
        } finally {
            isFetchingRecommendations = false
        }
    }

    private suspend fun restorePersistedState() {
        val state = repository.getPersistedState()
        if (state == null) {
            logger.d { "No persisted state found" }
            return
        }

        logger.i { "Restoring persisted state with ${state.entries.size} entries, currentIndex=${state.currentIndex}" }
        currentCollectionEntryState.value = state.currentCollectionEntry
        collectionHistoryState.value = state.collectionHistory.ifEmpty {
            state.currentCollectionEntry?.let(::listOf) ?: emptyList()
        }

        if (state.entries.isEmpty()) {
            logger.d { "Persisted state has no entries" }
            return
        }

        val restored = resolveMediaItems(withLocalCovers(state.entries))
        if (restored.isEmpty()) {
            logger.w { "restorePersistedState: all entries had blank urls" }
            return
        }
        val mediaItems = restored.map { it.first }
        entryByMediaUrl.value = entriesByUrl(restored)
        val normalizedStartPosition = state.currentIndex.coerceIn(0, mediaItems.lastIndex)
        logger.d { "Loading restored playlist with normalized start position: ${state.currentIndex} -> $normalizedStartPosition" }
        audioPlayer.load(mediaItems, autoPlay = false, startPosition = normalizedStartPosition)
        logger.i { "Persisted state restored successfully" }
    }

    /**
     * Converts each entry once. Looking every item's entry up again by converting the whole list
     * per item was quadratic: ~900k suspending conversions to start a 1353-track playlist.
     */
    private suspend fun resolveMediaItems(entries: List<QueueEntry>): List<Pair<MediaItem, QueueEntry>> =
        entries.mapNotNull { entry -> entry.toMediaItem()?.let { it to entry } }

    /** First entry wins for a repeated url, as before. */
    private fun entriesByUrl(resolved: List<Pair<MediaItem, QueueEntry>>): Map<String, QueueEntry> = buildMap {
        resolved.forEach { (item, entry) -> if (item.url !in this) put(item.url, entry) }
    }

    /** Local covers aren't persisted with the queue; look them up in the local media index. */
    private suspend fun withLocalCovers(entries: List<QueueEntry>): List<QueueEntry> {
        if (entries.none { it is QueueEntry.LocalTrack }) return entries
        val covers = runCatching {
            get<LocalMediaCacheRepository>().cacheFlow.first().folders
                .flatMap { it.tracks }
                .mapNotNull { track -> track.coverBytes?.let { track.path to it } }
                .toMap()
        }.getOrElse {
            logger.w(it) { "Couldn't read local media index for queue covers" }
            emptyMap()
        }
        if (covers.isEmpty()) return entries
        return entries.map { entry ->
            if (entry is QueueEntry.LocalTrack && entry.coverBytes == null) {
                covers[entry.url]?.let { entry.copy(coverBytes = it) } ?: entry
            } else entry
        }
    }

    /**
     * Runs from fire-and-forget collectors on a scope without an exception handler, where a
     * failed write (disk full, I/O error) would otherwise crash the app. Losing one save is fine.
     */
    private suspend fun persistQueueStateIfReady() {
        try {
            persistQueueState()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.e(e) { "Failed to persist queue state" }
        }
    }

    private suspend fun persistQueueState() {
        if (isRestoringPersistedState) {
            logger.v { "Skipping persist: still restoring persisted state" }
            return
        }

        val queueEntries = getQueue()
        if (queueEntries.isEmpty()) {
            logger.d { "Persisting queue state: clearing state (queue is empty)" }
            repository.clearState()
            return
        }

        val playlist = audioPlayer.playlistFlow.value
        val currentMedia = audioPlayer.currentMediaItemFlow.value
        val currentIndex = currentMedia?.let { current ->
            playlist.indexOfFirst { it.url == current.url }.takeIf { it >= 0 }
        } ?: 0
        val normalizedCurrentIndex = currentIndex.coerceIn(0, queueEntries.lastIndex)

        logger.d { "Persisting queue state: ${queueEntries.size} entries, currentIndex=$currentIndex (normalized: $normalizedCurrentIndex)" }
        repository.saveState(
            PersistedQueueState(
                entries = queueEntries,
                currentIndex = normalizedCurrentIndex,
                currentCollectionEntry = currentCollectionEntryState.value,
                collectionHistory = collectionHistoryState.value)
        )
    }

    private fun updateCollectionContext(collectionEntry: QueueCollectionEntry?) {
        if (collectionEntry != null) {
            logger.d { "Updating collection context: ${collectionEntry.id}" }
        } else {
            logger.d { "Clearing collection context" }
        }

        currentCollectionEntryState.value = collectionEntry

        if (collectionEntry == null) return

        collectionHistoryState.update { history ->
            val updated = listOf(collectionEntry) + history.filterNot { it == collectionEntry }
            logger.v { "Collection history updated, size now: ${updated.size}" }
            updated
        }
    }

    private var lastPrefetchedTrackId: String? = null

    private fun cancelOngoingPrefetch() {
        prefetchJob?.let { job ->
            if (job.isActive) {
                logger.d { "Cancelling ongoing prefetch job for lookahead pre-buffering" }
                job.cancel()
            }
        }
        prefetchJob = null
    }

    private fun checkAndPrefetchNextTrack(
        currentMedia: MediaItem?,
        position: kotlin.time.Duration,
        duration: kotlin.time.Duration
    ) {
        if (currentMedia == null || duration <= kotlin.time.Duration.ZERO || duration.inWholeSeconds < 5) return

        val progress = position.inWholeMilliseconds.toDouble() / duration.inWholeMilliseconds.toDouble()
        val remainingSec = (duration - position).inWholeSeconds

        // Predictive Lookahead Pre-Buffering trigger:
        // Trigger at +3s after Track N starts or when progress >= 70% (or <=30s remaining).
        // Rapid skips (skipping within 3 seconds) return immediately before lookahead prefetching fires.
        val isWarmupTrigger = position.inWholeMilliseconds >= 3_000L
        val isProgressTrigger = progress >= 0.70 || remainingSec <= 30
        if (!isWarmupTrigger && !isProgressTrigger) return

        val playlist = audioPlayer.playlistFlow.value
        if (playlist.isEmpty()) return

        val currentIndex = playlist.indexOfFirst { it.url == currentMedia.url }
        if (currentIndex < 0) return

        val loopState = audioPlayer.loopStateFlow.value
        val nextIndex = when {
            currentIndex < playlist.size - 1 -> currentIndex + 1
            loopState == LoopState.ALL && playlist.size > 1 -> 0
            else -> -1
        }
        if (nextIndex < 0) return

        val nextMedia = playlist[nextIndex]
        val nextEntry = entryByMediaUrl.value[nextMedia.url] ?: queueFlow.value.getOrNull(nextIndex)
        if (nextEntry !is QueueEntry.StreamingTrack) return

        val nextTrack = nextEntry.track

        // If already prefetched into RamHeadBufferPool or job is actively running for this track, skip
        if (lastPrefetchedTrackId == nextTrack.id && (RamHeadBufferPool.hasHead(nextTrack.id) || prefetchJob?.isActive == true)) {
            return
        }

        lastPrefetchedTrackId = nextTrack.id
        cancelOngoingPrefetch()

        prefetchJob = scope.launch {
            logger.i { "Prefetching stream info & 512KB audio head for upcoming track: '${nextTrack.title}' (${nextTrack.id})" }
            runCatchingNonCancellation {
                val streamInfo = streamingUrlRepository.resolveStreamInfo(nextTrack) ?: run {
                    logger.d { "Prefetch skipped: unable to resolve stream info for track ${nextTrack.id}" }
                    return@runCatchingNonCancellation
                }

                if (RamHeadBufferPool.hasHead(nextTrack.id)) {
                    logger.d { "Prefetch: 512KB head already cached in RamHeadBufferPool for track ${nextTrack.id}" }
                    return@runCatchingNonCancellation
                }

                if (streamInfo.protocol == StreamProtocol.PROGRESSIVE) {
                    prefetchAudioHead(nextTrack, streamInfo)
                }
            }.onFailure { e ->
                logger.d { "Prefetch skipped or failed for track ${nextTrack.id}: ${e.message}" }
            }
        }
    }

    private suspend fun prefetchAudioHead(nextTrack: MetadataTrack, streamInfo: StreamInfo) {
        if (streamInfo.url.isBlank()) return
        val success = runCatchingNonCancellation {
            fetchAndStoreHeadDirect(nextTrack.id, streamInfo.url)
        }.getOrElse { e ->
            logger.d { "Direct prefetch head fetch failed from ${streamInfo.url} for track ${nextTrack.id}: ${e.message}" }
            false
        }
        if (success) {
            logger.i { "Successfully prefetched 512KB audio head directly for '${nextTrack.title}' (${nextTrack.id})" }
        }
    }

    private suspend fun fetchAndStoreHeadDirect(trackId: String, url: String): Boolean {
        return httpClient.prepareRequest(url) {
            method = HttpMethod.Get
            header(HttpHeaders.Range, "bytes=0-${RamHeadBufferPool.HEAD_BUFFER_SIZE - 1}")
            applyStreamClientHeaders(url)
        }.execute { response ->
            if (response.status != HttpStatusCode.PartialContent && response.status != HttpStatusCode.OK) {
                logger.d { "Direct prefetch returned unexpected HTTP status ${response.status} for url $url" }
                return@execute false
            }

            val channel = response.bodyAsChannel()
            val buffer = ByteArray(RamHeadBufferPool.HEAD_BUFFER_SIZE)
            var totalRead = 0
            try {
                while (totalRead < RamHeadBufferPool.HEAD_BUFFER_SIZE && !channel.isClosedForRead) {
                    val read = channel.readAvailable(buffer, totalRead, RamHeadBufferPool.HEAD_BUFFER_SIZE - totalRead)
                    if (read <= 0) break
                    totalRead += read
                }
            } finally {
                runCatching { channel.cancel() }
            }

            if (totalRead <= 0) return@execute false

            val data = if (totalRead == RamHeadBufferPool.HEAD_BUFFER_SIZE) buffer else buffer.copyOf(totalRead)
            val contentType = response.headers[HttpHeaders.ContentType]
            val totalLength = response.headers[HttpHeaders.ContentRange]
                ?.substringAfterLast('/')
                ?.trim()
                ?.toLongOrNull()
                ?: response.headers[HttpHeaders.ContentLength]?.toLongOrNull()

            RamHeadBufferPool.putHead(
                trackId = trackId,
                data = data,
                contentType = contentType,
                totalLength = totalLength,
                isLookahead = true
            )
            true
        }
    }

    private suspend fun QueueEntry.toMediaItem(): MediaItem? {
        return when (this) {
            is QueueEntry.StreamingTrack -> {
                val resolvedUrl = url.ifBlank { buildStreamingUrl(track.id, protocol) }
                if (resolvedUrl.isBlank()) return null
                MediaItem(
                    title = track.title,
                    artist = track.artists.joinToString(", ") { artist -> artist.name },
                    album = track.album?.title.orEmpty(),
                    duration = track.durationMs.milliseconds,
                    coverURL = artworkUrl.orEmpty(),
                    url = resolvedUrl,
                    protocol = protocol)
            }

            is QueueEntry.LocalTrack -> {
                if (url.isBlank()) return null
                MediaItem(
                    title = name,
                    artist = artists.joinToString(", "),
                    album = album.orEmpty(),
                    duration = duration.milliseconds,
                    coverURL = "",
                    url = url,
                    coverBytes = coverBytes?.takeIf { it.isNotEmpty() })
            }
        }
    }

    private fun MediaItem.toFallbackQueueEntry(): QueueEntry {
        return QueueEntry.LocalTrack(
            name = title,
            artists = artist.split(',').map { it.trim() }.filter { it.isNotEmpty() },
            duration = duration.inWholeMilliseconds,
            album = album.ifBlank { null },
            coverBytes = null,
            url = url)
    }

    private inline fun <T> runCatchingNonCancellation(block: () -> T): Result<T> =

        runCatching(block).onFailure { if (it is kotlinx.coroutines.CancellationException) throw it }



    private val localServerOrNull: LocalServer?
        get() = runCatching { getKoin().getOrNull<LocalServer>() }.getOrNull()

    private suspend fun buildStreamingUrl(trackId: String, protocol: StreamProtocol): String {
        val baseUrl = localServerOrNull?.port?.value?.let { "http://127.0.0.1:$it" }
            ?: localServerOrNull?.awaitBaseUrl(500.milliseconds)
            ?: run {
                val port = settingsProvider.settingsState.value?.playbackProxyServerPort ?: 0
                if (port > 0) "http://127.0.0.1:$port" else "http://127.0.0.1:14769"
            }
        return when (protocol) {
            StreamProtocol.HLS, StreamProtocol.DASH -> "${baseUrl.trimEnd('/')}/manifest/$trackId"
            StreamProtocol.PROGRESSIVE -> "${baseUrl.trimEnd('/')}/stream/$trackId"
        }
    }
}
