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

package org.spotube.spotube.core.server

import org.spotube.plugin_interfaces.plugin_apis.metadata.track.MetadataTrack
import org.spotube.spotube.core.paths.Paths
import org.spotube.spotube.modules.settings.SettingsRepository
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HeadersBuilder
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import io.ktor.utils.io.writeFully
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import okio.BufferedSink
import okio.FileSystem
import okio.Path
import okio.Path.Companion.toPath
import okio.SYSTEM
import okio.buffer
import okio.use
import kotlin.time.Duration.Companion.milliseconds

@Serializable
data class CacheIndex(
    val entries: List<CacheEntry> = emptyList()
)

@Serializable
data class CacheEntry(
    val trackId: String,
    val filename: String,
    val sizeBytes: Long,
    val createdAtMs: Long,
    val contentType: String = "application/octet-stream",
    val lastAccessedAtMs: Long = createdAtMs
)

class CacheManager(
    private val paths: Paths,
    private val settingsRepository: SettingsRepository,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())) {
    private val fileSystem: FileSystem = FileSystem.SYSTEM
    private val cacheMutex: Mutex = Mutex()
    private val json: Json = Json { ignoreUnknownKeys = true }
    private val cacheIndexRef: AtomicRef<CacheIndex> = atomic(loadCacheIndexFromDisk())
    private val dirtyWriteBackChannel = Channel<Unit>(Channel.CONFLATED)

    init {
        scope.launch {
            for (signal in dirtyWriteBackChannel) {
                delay(100.milliseconds)
                persistIndexToDisk()
            }
        }
    }

    interface AsyncSink : AutoCloseable {
        suspend fun write(bytes: ByteArray, offset: Int, length: Int)
        suspend fun complete(totalBytesWritten: Long, success: Boolean)
    }

    fun openAsyncCacheSink(
        filename: String,
        trackId: String,
        contentType: String,
        expectedLength: Long?
    ): AsyncSink {
        val cacheDir = resolveCacheDir()
        fileSystem.createDirectories(cacheDir)
        val cacheFile = cacheDir / filename.toPath()

        return object : AsyncSink {
            // Channel with capacity 64 chunks (up to 4 MB in memory) to absorb slow flash write spikes
            private val channel = Channel<ByteArray>(capacity = 64)
            @kotlin.jvm.Volatile
            private var writeFailed = false

            private val writeJob = scope.launch(Dispatchers.IO) {
                var sink: BufferedSink? = null
                var writtenBytes = 0L
                try {
                    sink = fileSystem.sink(cacheFile).buffer()
                    for (chunk in channel) {
                        sink.write(chunk)
                        writtenBytes += chunk.size
                    }
                    sink.flush()
                } catch (e: Throwable) {
                    writeFailed = true
                } finally {
                    sink?.runCatching { close() }
                    if (writeFailed && fileSystem.exists(cacheFile)) {
                        runCatching { fileSystem.delete(cacheFile) }
                    }
                }
            }

            override suspend fun write(bytes: ByteArray, offset: Int, length: Int) {
                if (length <= 0 || writeFailed) return
                val copy = bytes.copyOfRange(offset, offset + length)
                try {
                    channel.send(copy)
                } catch (e: Throwable) {
                    writeFailed = true
                }
            }

            override suspend fun complete(totalBytesWritten: Long, success: Boolean) {
                channel.close()
                writeJob.join()

                val completed = success && !writeFailed && (expectedLength == null || expectedLength == totalBytesWritten)
                if (completed) {
                    commitCacheEntry(
                        trackId = trackId,
                        filename = filename,
                        sizeBytes = totalBytesWritten,
                        contentType = contentType
                    )
                    scope.launch { evictIfNeeded() }
                } else {
                    if (fileSystem.exists(cacheFile)) {
                        runCatching { fileSystem.delete(cacheFile) }
                    }
                }
            }

            override fun close() {
                channel.close()
            }
        }
    }

    companion object {
        private val cacheIndexFileName = "cache_index.json".toPath()
    }

    private fun resolveCacheFolder(): String? {
        return settingsRepository.userSettings.value.cacheFolder
    }

    private fun resolveSizeLimitMB(): Long {
        return settingsRepository.userSettings.value.cacheSizeLimitMB
    }

    fun resolveCacheDir(): Path {
        val folder = resolveCacheFolder()
        return if (!folder.isNullOrBlank()) {
            folder.toPath()
        } else {
            paths.getMusicCacheDirPath().toPath()
        }
    }

    private fun getCacheIndexPath(): Path = resolveCacheDir() / cacheIndexFileName

    private fun loadCacheIndexFromDisk(): CacheIndex {
        return runCatching {
            val indexPath = getCacheIndexPath()
            if (!fileSystem.exists(indexPath)) return CacheIndex()
            fileSystem.source(indexPath).buffer().use { source ->
                json.decodeFromString<CacheIndex>(source.readUtf8())
            }
        }.getOrDefault(CacheIndex())
    }

    private fun persistIndexToDisk() {
        val index = cacheIndexRef.value
        val indexPath = getCacheIndexPath()
        runCatching {
            fileSystem.createDirectories(indexPath.parent ?: resolveCacheDir())
            fileSystem.sink(indexPath).buffer().use { sink ->
                sink.writeUtf8(json.encodeToString(index))
            }
        }
    }

    fun readCacheIndex(): CacheIndex = cacheIndexRef.value

    fun writeCacheIndex(index: CacheIndex) {
        cacheIndexRef.value = index
        persistIndexToDisk()
    }

    fun findCachedEntry(trackId: String): Pair<Path, CacheEntry>? {
        val index = cacheIndexRef.value
        val entry = index.entries.firstOrNull { it.trackId == trackId } ?: return null
        val filePath = resolveCacheDir() / entry.filename.toPath()
        return if (fileSystem.exists(filePath)) filePath to entry else null
    }

    suspend fun invalidateCacheEntry(trackId: String) {
        cacheMutex.withLock {
            val index = cacheIndexRef.value
            val entry = index.entries.firstOrNull { it.trackId == trackId } ?: return
            val filePath = resolveCacheDir() / entry.filename.toPath()
            if (fileSystem.exists(filePath)) {
                runCatching { fileSystem.delete(filePath) }
            }
            while (true) {
                val cur = cacheIndexRef.value
                val next = cur.copy(entries = cur.entries.filter { it.trackId != trackId })
                if (cacheIndexRef.compareAndSet(cur, next)) break
            }
            persistIndexToDisk()
        }
    }

    fun resolveCacheFilename(track: MetadataTrack, contentType: String?): String {
        val artists = track.artists.joinToString(", ") { it.name }.sanitizeFilenamePart()
        val title = track.title.sanitizeFilenamePart()
        val ext = contentTypeToExtension(contentType)
        return "$artists - $title.$ext"
    }

    suspend fun evictIfNeeded() {
        val limitMB = resolveSizeLimitMB()
        if (limitMB <= 0L) return

        cacheMutex.withLock {
            val cacheDir = resolveCacheDir()
            if (!fileSystem.exists(cacheDir)) return

            val index = cacheIndexRef.value
            val sorted = index.entries.sortedBy { it.lastAccessedAtMs }.toMutableList()

            var totalSize = sorted.sumOf { it.sizeBytes }
            val limitBytes = limitMB * 1024 * 1024
            var evictedAny = false

            while (totalSize > limitBytes && sorted.isNotEmpty()) {
                val entry = sorted.removeFirst()
                val filePath = cacheDir / entry.filename.toPath()
                if (fileSystem.exists(filePath)) {
                    runCatching { fileSystem.delete(filePath) }
                }
                totalSize -= entry.sizeBytes
                evictedAny = true
            }

            if (evictedAny) {
                val next = index.copy(entries = sorted)
                cacheIndexRef.value = next
                persistIndexToDisk()
            }
        }
    }

    suspend fun touchCacheEntry(trackId: String) {
        val now = kotlin.time.Clock.System.now().toEpochMilliseconds()
        var updated = false
        while (true) {
            val cur = cacheIndexRef.value
            val entryIndex = cur.entries.indexOfFirst { it.trackId == trackId }
            if (entryIndex == -1) break
            val updatedEntry = cur.entries[entryIndex].copy(lastAccessedAtMs = now)
            val updatedList = cur.entries.toMutableList().apply { set(entryIndex, updatedEntry) }
            val next = cur.copy(entries = updatedList)
            if (cacheIndexRef.compareAndSet(cur, next)) {
                updated = true
                break
            }
        }
        if (updated) {
            dirtyWriteBackChannel.trySend(Unit)
        }
    }

    suspend fun commitCacheEntry(
        trackId: String,
        filename: String,
        sizeBytes: Long,
        contentType: String) {
        val now = kotlin.time.Clock.System.now().toEpochMilliseconds()
        val entry = CacheEntry(
            trackId = trackId,
            filename = filename,
            sizeBytes = sizeBytes,
            createdAtMs = now,
            contentType = contentType,
            lastAccessedAtMs = now
        )
        while (true) {
            val cur = cacheIndexRef.value
            val next = cur.copy(entries = cur.entries.filter { it.trackId != trackId } + entry)
            if (cacheIndexRef.compareAndSet(cur, next)) break
        }
        dirtyWriteBackChannel.trySend(Unit)
    }

    suspend fun serveCacheHead(call: ApplicationCall, entry: CacheEntry) {
        touchCacheEntry(entry.trackId)
        call.respond(object : OutgoingContent.NoContent() {
            override val status: HttpStatusCode = HttpStatusCode.OK
            override val headers: Headers = HeadersBuilder().apply {
                append(HttpHeaders.AcceptRanges, "bytes")
            }.build()
            override val contentLength: Long = entry.sizeBytes
            override val contentType: ContentType = entry.toContentType()
        })
    }

    suspend fun serveCacheGet(call: ApplicationCall, filePath: Path, entry: CacheEntry) {
        touchCacheEntry(entry.trackId)
        val fileSize = entry.sizeBytes
        val rangeHeader = call.request.headers[HttpHeaders.Range]

        if (rangeHeader != null) {
            val range = parseRange(rangeHeader, fileSize)
            if (range != null) {
                val (start, end) = range
                val length = end - start + 1
                val contentRange = "bytes $start-$end/$fileSize"
                call.respond(object : OutgoingContent.WriteChannelContent() {
                    override val status: HttpStatusCode = HttpStatusCode.PartialContent
                    override val contentLength: Long = length
                    override val contentType: ContentType = entry.toContentType()
                    override val headers: Headers = HeadersBuilder().apply {
                        append(HttpHeaders.ContentRange, contentRange)
                        append(HttpHeaders.AcceptRanges, "bytes")
                    }.build()
                    override suspend fun writeTo(channel: io.ktor.utils.io.ByteWriteChannel) {
                        streamFileRange(fileSystem, filePath, start, length, channel)
                    }
                })
                return
            }
            call.respond(object : OutgoingContent.NoContent() {
                override val status: HttpStatusCode = HttpStatusCode.RequestedRangeNotSatisfiable
                override val headers: Headers = HeadersBuilder().apply {
                    append(HttpHeaders.ContentRange, "bytes */$fileSize")
                }.build()
            })
            return
        }

        call.respond(object : OutgoingContent.WriteChannelContent() {
            override val status: HttpStatusCode = HttpStatusCode.OK
            override val contentLength: Long = fileSize
            override val contentType: ContentType = entry.toContentType()
            override val headers: Headers = HeadersBuilder().apply {
                append(HttpHeaders.AcceptRanges, "bytes")
            }.build()
            override suspend fun writeTo(channel: io.ktor.utils.io.ByteWriteChannel) {
                streamFileRange(fileSystem, filePath, 0, fileSize, channel)
            }
        })
    }

}

private const val STREAM_CHUNK_BYTES = 64 * 1024

/**
 * Copies [length] bytes from [start] in fixed-size chunks. Cached tracks are tens of MB and the
 * player re-requests them on every seek, so they must never be read into the heap whole.
 */
internal suspend fun streamFileRange(
    fileSystem: FileSystem,
    filePath: Path,
    start: Long,
    length: Long,
    channel: io.ktor.utils.io.ByteWriteChannel) {
    fileSystem.source(filePath).buffer().use { source ->
        source.skip(start)
        val chunk = ByteArray(STREAM_CHUNK_BYTES)
        var remaining = length
        while (remaining > 0) {
            val read = source.read(chunk, 0, minOf(chunk.size.toLong(), remaining).toInt())
            if (read == -1) break
            channel.writeFully(chunk, 0, read)
            channel.flush()
            remaining -= read
        }
    }
}

internal fun parseRange(rangeHeader: String, fileSize: Long): Pair<Long, Long>? {
    val match = Regex("""bytes=(\d*)-(\d*)""").find(rangeHeader) ?: return null
    val startStr = match.groupValues[1]
    val endStr = match.groupValues[2]
    if (startStr.isEmpty() && endStr.isEmpty()) return null

    val start = startStr.toLongOrNull() ?: 0L
    val end = if (endStr.isNotEmpty()) endStr.toLongOrNull() ?: (fileSize - 1) else (fileSize - 1)

    if (start >= fileSize || start > end) return null
    return start to end.coerceAtMost(fileSize - 1)
}

internal fun CacheEntry.toContentType(): ContentType =
    runCatching { ContentType.parse(contentType) }.getOrDefault(ContentType.Application.OctetStream)

internal fun contentTypeToExtension(contentType: String?): String {
    return when {
        contentType == null -> "dat"
        "webm" in contentType -> "webm"
        "ogg" in contentType || "opus" in contentType -> "ogg"
        "mp4" in contentType || "m4a" in contentType || "aac" in contentType -> "m4a"
        "mpeg" in contentType || "mp3" in contentType -> "mp3"
        "flac" in contentType -> "flac"
        "wav" in contentType -> "wav"
        else -> "dat"
    }
}

internal fun String.sanitizeFilenamePart(): String {
    return this.replace(Regex("""[/\\:*?"<>|]"""), "_").take(200).trim()
}
