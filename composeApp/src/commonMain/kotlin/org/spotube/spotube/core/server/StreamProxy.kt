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

import org.spotube.plugin_interfaces.plugin_apis.audio.StreamProtocol
import org.spotube.plugin_interfaces.plugin_apis.metadata.track.MetadataTrack
import org.spotube.spotube.core.audioplayer.AudioPlayerQueue
import org.spotube.spotube.core.audioplayer.QueueEntry
import org.spotube.spotube.core.di.injectLogger
import io.ktor.client.HttpClient
import io.ktor.client.request.prepareRequest
import io.ktor.client.request.request
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsChannel
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HeadersBuilder
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.Url
import io.ktor.http.content.OutgoingContent
import io.ktor.http.decodeURLQueryComponent
import io.ktor.http.encodeURLParameter
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import io.ktor.util.cio.ChannelWriteException
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.ClosedWriteChannelException
import io.ktor.utils.io.cancel
import io.ktor.utils.io.copyTo
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import okio.FileSystem
import okio.IOException
import okio.Path.Companion.toPath
import okio.SYSTEM
import okio.buffer
import okio.use
import org.koin.core.component.KoinComponent
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

internal class StreamProxy(
    private val httpClient: HttpClient,
    private val streamingUrlRepository: StreamingUrlRepository,
    private val cacheManager: CacheManager,
    private val audioPlayerQueue: AudioPlayerQueue,
    private val isCachingEnabled: () -> Boolean,
    private val activePort: () -> Int?,
    private val scope: CoroutineScope): KoinComponent {
    val logger by injectLogger<StreamProxy>()

    companion object {
        const val HOST = "127.0.0.1"
        const val YOUTUBE_REFERER = "https://www.youtube.com/"
        const val BROWSER_USER_AGENT =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        const val ANDROID_USER_AGENT =
            "com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip"
        const val IOS_USER_AGENT =
            "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)"
        const val VISIONOS_USER_AGENT =
            "com.google.visionos.youtube/1.04(RealityDevice17,1; U; CPU visionOS 26_6_0 like Mac OS X; US)"
    }

    private val fileSystem = FileSystem.SYSTEM
    private val streamingInProgress = mutableSetOf<String>()
    private val streamingMutex = Mutex()

    private fun getTrackMetadata(trackId: String): MetadataTrack? {
        return audioPlayerQueue.queueFlow.value
            .filterIsInstance<QueueEntry.StreamingTrack>()
            .firstOrNull { it.track.id == trackId }
            ?.track
    }

    suspend fun handleStreamRequest(call: ApplicationCall, requestMethod: HttpMethod) {
        val trackId = call.parameters["trackId"]?.trim().orEmpty()
        if (trackId.isBlank()) {
            logger.w { "Rejecting /stream request with missing track id" }
            call.respond(HttpStatusCode.BadRequest, "Missing track id")
            return
        }

        logger.i {
            "[Req] $requestMethod /stream/$trackId from=${call.request.local.remoteHost} " +
                "version=${call.request.local.version} range=${call.request.headers[HttpHeaders.Range]} " +
                "ua=${call.request.headers[HttpHeaders.UserAgent]} " +
                "connection=${call.request.headers[HttpHeaders.Connection]} caching=${isCachingEnabled()}"
        }

        // 1. Check persistent LRU disk cache
        if (isCachingEnabled()) {
            val cachedEntry = cacheManager.findCachedEntry(trackId)
            if (cachedEntry != null) {
                val (filePath, entry) = cachedEntry
                logger.d { "Serving track $trackId from cache" }
                when (requestMethod) {
                    HttpMethod.Head -> cacheManager.serveCacheHead(call, entry)
                    HttpMethod.Get -> cacheManager.serveCacheGet(call, filePath, entry)
                    else -> call.respond(HttpStatusCode.MethodNotAllowed)
                }
                return
            }
        }

        // 2. Check Tier 1 RAM Head Buffer for instant startup (<2ms TTFB)
        val rangeHeader = call.request.headers[HttpHeaders.Range]
        val isRequestFromByte0 = rangeHeader == null || rangeHeader.startsWith("bytes=0-") || rangeHeader == "bytes=0-"
        if (requestMethod == HttpMethod.Get && isRequestFromByte0 && RamHeadBufferPool.hasHead(trackId)) {
            logger.d { "Serving track $trackId via RAM head buffer splicing" }
            val spliced = runCatching { handleSplicedStream(call, trackId) }.getOrElse { throwable ->
                if (throwable is CancellationException) throw throwable
                if (throwable.isClientDisconnect()) {
                    logger.i(throwable) { "Player disconnected during spliced playback for $trackId" }
                    return
                }
                logger.w(throwable) { "Spliced playback failed for $trackId, falling back to upstream" }
                false
            }
            if (spliced) return
        }

        // 3. Fall through to non-blocking upstream streaming
        doStreamFromUpstream(call, trackId, requestMethod)
    }

    private suspend fun doStreamFromUpstream(call: ApplicationCall, trackId: String, requestMethod: HttpMethod) {
        val resolveStart = TimeSource.Monotonic.markNow()
        val streamInfo = streamingUrlRepository.resolveStreamInfo(trackId)
        logger.i {
            "[Req] resolved stream for $trackId in ${resolveStart.elapsedNow()}: " +
                "protocol=${streamInfo?.protocol} codec=${streamInfo?.codec} container=${streamInfo?.container} " +
                "url=${streamInfo?.url?.take(120)}"
        }
        if (streamInfo == null) {
            logger.w { "Unable to resolve stream for track $trackId" }
            call.respond(HttpStatusCode.NotFound, "Unable to resolve stream for track $trackId")
            return
        }

        if (streamInfo.protocol != StreamProtocol.PROGRESSIVE) {
            val port = activePort() ?: 8080
            val manifestUrl = "http://$HOST:$port/manifest/${trackId}"
            logger.d { "Redirecting $trackId to manifest URL: $manifestUrl" }
            call.response.headers.append(HttpHeaders.Location, manifestUrl)
            call.respond(HttpStatusCode.Found)
            return
        }

        proxyStream(call, streamInfo.url, trackId, requestMethod)
    }

    suspend fun handleManifestRequest(call: ApplicationCall) {
        val trackId = call.parameters["trackId"]?.trim().orEmpty()
        if (trackId.isBlank()) {
            logger.w { "Rejecting /manifest request with missing track id" }
            call.respond(HttpStatusCode.BadRequest, "Missing track id")
            return
        }

        val streamInfo = streamingUrlRepository.resolveStreamInfo(trackId)
        if (streamInfo == null) {
            logger.w { "Unable to resolve stream for track $trackId" }
            call.respond(HttpStatusCode.NotFound, "Unable to resolve stream for track $trackId")
            return
        }

        if (streamInfo.protocol == StreamProtocol.PROGRESSIVE) {
            logger.w { "Track $trackId is not a manifest stream" }
            call.respond(HttpStatusCode.BadRequest, "Not a manifest stream")
            return
        }

        proxyManifest(call, streamInfo.url, trackId, streamInfo.protocol)
    }

    suspend fun handleSegmentRequest(call: ApplicationCall) {
        val trackId = call.parameters["trackId"]?.trim().orEmpty()
        val segmentUrl = call.parameters["url"]?.let { decodeUrl(it) }

        if (trackId.isBlank() || segmentUrl.isNullOrBlank()) {
            logger.w { "Rejecting /segment request with missing parameters" }
            call.respond(HttpStatusCode.BadRequest, "Missing track id or segment url")
            return
        }

        proxySegment(call, segmentUrl, trackId)
    }

    private suspend fun proxyManifest(
        call: ApplicationCall,
        manifestUrl: String,
        trackId: String,
        protocol: StreamProtocol
    ) {
        logger.d { "Proxying manifest for track $trackId ($protocol): $manifestUrl" }

        val response = runCatching {
            httpClient.request(manifestUrl) {
                method = HttpMethod.Get
                applyUpstreamHeaders(call, this, manifestUrl, includeRange = false)
            }
        }.getOrElse { throwable ->
            logger.w(throwable) { "Failed to fetch manifest for track $trackId" }
            call.respond(HttpStatusCode.BadGateway, "Failed to fetch manifest")
            return
        }

        if (response.status.value >= 400) {
            logger.w { "Manifest request failed for track $trackId with status ${response.status.value}" }
            call.respond(HttpStatusCode.BadGateway, "Failed to fetch manifest")
            return
        }

        val manifestText = response.bodyAsText()
        val port = activePort() ?: 8080
        val serverUrl = "http://$HOST:$port"
        val rewrittenManifest = when (protocol) {
            StreamProtocol.HLS -> rewriteHlsManifest(manifestText, manifestUrl, serverUrl, trackId)
            StreamProtocol.DASH -> rewriteDashManifest(manifestText, manifestUrl, serverUrl, trackId)
            else -> manifestText
        }

        val contentType = when (protocol) {
            StreamProtocol.HLS -> ContentType("application", "vnd.apple.mpegurl")
            StreamProtocol.DASH -> ContentType("application", "dash+xml")
            else -> ContentType.Application.OctetStream
        }

        call.respond(object : OutgoingContent.WriteChannelContent() {
            override val status: HttpStatusCode = HttpStatusCode.OK
            override val contentType: ContentType = contentType
            override val contentLength: Long = rewrittenManifest.encodeToByteArray().size.toLong()

            override suspend fun writeTo(channel: io.ktor.utils.io.ByteWriteChannel) {
                channel.writeFully(rewrittenManifest.encodeToByteArray())
            }
        })
    }

    private suspend fun proxySegment(
        call: ApplicationCall,
        segmentUrl: String,
        trackId: String
    ) {
        logger.d { "Proxying segment for track $trackId" }

        runCatching {
            httpClient.prepareRequest(segmentUrl) {
                method = HttpMethod.Get
                applyUpstreamHeaders(call, this, segmentUrl, includeRange = true)
            }.execute { response -> respondWithSegment(call, response, trackId) }
        }.onFailure { throwable ->
            if (throwable is CancellationException) throw throwable
            if (throwable.isClientDisconnect()) {
                logger.d { "Player closed the connection while streaming segment of track $trackId" }
                return
            }
            logger.w(throwable) { "Failed to fetch segment for track $trackId" }
            runCatching { call.respond(HttpStatusCode.BadGateway, "Failed to fetch segment") }
        }
    }

    private suspend fun respondWithSegment(call: ApplicationCall, response: HttpResponse, trackId: String) {

        if (response.status.value >= 400) {
            logger.w { "Segment request failed for track $trackId with status ${response.status.value}" }
            call.respond(HttpStatusCode.BadGateway, "Failed to fetch segment")
            return
        }

        val downstreamHeaders = upstreamHeadersToForward(response)
        val downstreamContentLength = downstreamHeaders[HttpHeaders.ContentLength]?.toLongOrNull()

        call.respond(object : OutgoingContent.WriteChannelContent() {
            override val status: HttpStatusCode = response.status
            override val headers: Headers = downstreamHeaders
            override val contentLength: Long? = downstreamContentLength
            override val contentType: ContentType? =
                downstreamHeaders[HttpHeaders.ContentType]?.let(ContentType.Companion::parse)

            override suspend fun writeTo(channel: io.ktor.utils.io.ByteWriteChannel) {
                response.bodyAsChannel().copyTo(channel)
            }
        })
    }

    private suspend fun proxyStream(
        call: ApplicationCall,
        streamUrl: String,
        trackId: String,
        requestMethod: HttpMethod
    ) {
        logger.d { "Proxying stream for track $trackId using $requestMethod" }

        var currentUrl = streamUrl
        var attemptedRefresh = false
        var attemptedFallback = false
        var retryCount = 0
        val maxRetries = 3
        val upstreamStart = TimeSource.Monotonic.markNow()
        var responseStarted = false

        // `request()` buffers the whole body before returning, which stalls the player until
        // the full track is downloaded. `execute {}` hands over the response as soon as headers
        // arrive so the body can be streamed.
        while (!responseStarted) {
            var retryUrl: String? = null
            val failure = runCatching {
                httpClient.prepareRequest(currentUrl) {
                    method = requestMethod
                    applyUpstreamHeaders(call, this, currentUrl, includeRange = true)
                }.execute { response ->
                    logger.i {
                        "[Req] upstream headers for $trackId after ${upstreamStart.elapsedNow()}: " +
                            "status=${response.status} version=${response.version} " +
                            "headers=${response.headers.entries().joinToString { "${it.key}=${it.value}" }}"
                    }
                    if (response.status.value < 400) {
                        responseStarted = true
                        respondWithUpstream(call, response, currentUrl, trackId, requestMethod)
                        return@execute
                    }

                    logger.w { "Upstream stream request failed for track $trackId with status ${response.status.value}" }
                    streamingUrlRepository.invalidateCachedStreamUrl(trackId, currentUrl)

                    if (!attemptedRefresh) {
                        val refreshedUrl =
                            streamingUrlRepository.resolveStreamInfo(trackId, forceRefresh = true)?.url
                        if (!refreshedUrl.isNullOrBlank() && refreshedUrl != currentUrl) {
                            retryUrl = refreshedUrl
                            attemptedRefresh = true
                            return@execute
                        }
                    }

                    if (!attemptedFallback) {
                        val fallbackUrl =
                            streamingUrlRepository.resolveStreamInfo(trackId, forceRefresh = true, fallbackToLowerQuality = true)?.url
                        if (!fallbackUrl.isNullOrBlank() && fallbackUrl != currentUrl) {
                            retryUrl = fallbackUrl
                            attemptedFallback = true
                            return@execute
                        }
                    }
                    responseStarted = true
                    respondWithUpstream(call, response, currentUrl, trackId, requestMethod)
                }
            }.exceptionOrNull()

            if (responseStarted) {
                // If respondWithUpstream was called, headers have been committed to the client.
                // Do not retry at the HTTP request level, as call.respond cannot be called twice.
                if (failure != null && failure !is CancellationException && !failure.isClientDisconnect()) {
                    logger.w(failure) { "Upstream stream ended with error for track $trackId" }
                }
                return
            }

            if (failure != null) {
                if (failure is CancellationException) throw failure
                if (failure.isClientDisconnect()) {
                    logger.i(failure) { "[Req] player closed the connection while streaming track $trackId" }
                    return
                }

                retryCount++
                if (retryCount <= maxRetries) {
                    logger.w(failure) {
                        "Transient failure connecting upstream for track $trackId (attempt $retryCount/$maxRetries). Retrying..."
                    }
                    if (retryCount >= 2 && !attemptedFallback) {
                        val fallbackUrl = streamingUrlRepository.resolveStreamInfo(trackId, forceRefresh = true, fallbackToLowerQuality = true)?.url
                        if (!fallbackUrl.isNullOrBlank() && fallbackUrl != currentUrl) {
                            currentUrl = fallbackUrl
                            attemptedFallback = true
                        }
                    }
                    val backoffMs = minOf(500L * (1L shl (retryCount - 1)), 3000L)
                    delay(backoffMs)
                    continue
                }

                logger.w(failure) { "Failed to fetch upstream stream for track $trackId after $maxRetries retries" }
                // Throws if the response already started streaming; nothing more to send then
                runCatching { call.respond(HttpStatusCode.BadGateway, "Failed to fetch upstream stream") }
                return
            }

            val next = retryUrl ?: return
            logger.d { "Retrying stream for track $trackId using refreshed URL" }
            currentUrl = next
            delay(200)
        }
    }

    private suspend fun handleSplicedStream(call: ApplicationCall, trackId: String): Boolean {
        val headData = RamHeadBufferPool.getHead(trackId) ?: return false
        val headInfo = RamHeadBufferPool.getHeadInfo(trackId)
        val headLength = headData.size
        if (headLength == 0) return false

        // Parse range header if present
        val requestRange = call.request.headers[HttpHeaders.Range]
        val rangeMatch = requestRange?.let { Regex("""bytes=(\d+)-(\d*)""").find(it) }
        val reqStart = rangeMatch?.groupValues?.get(1)?.toLongOrNull() ?: 0L
        val reqEnd = rangeMatch?.groupValues?.get(2)?.let { if (it.isNotEmpty()) it.toLongOrNull() else null }

        // Splicing from RAM buffer starting at 0 only applies if start is 0
        if (reqStart != 0L) return false

        val totalLength = headInfo?.totalLength ?: return false
        val downstreamContentType = headInfo.contentType ?: "audio/webm"

        // If player requested a sub-range that fits entirely within the head
        if (reqEnd != null && reqEnd < headLength) {
            val subLen = (reqEnd - reqStart + 1).toInt()
            val contentRange = "bytes 0-$reqEnd/$totalLength"
            call.respond(object : OutgoingContent.WriteChannelContent() {
                override val status: HttpStatusCode = HttpStatusCode.PartialContent
                override val contentLength: Long = subLen.toLong()
                override val contentType: ContentType = ContentType.parse(downstreamContentType)
                override val headers: Headers = HeadersBuilder().apply {
                    append(HttpHeaders.ContentRange, contentRange)
                    append(HttpHeaders.AcceptRanges, "bytes")
                }.build()

                override suspend fun writeTo(channel: io.ktor.utils.io.ByteWriteChannel) {
                    channel.writeFully(headData, 0, subLen)
                    channel.flush()
                }
            })
            return true
        }

        val targetEnd = reqEnd?.coerceAtMost(totalLength - 1) ?: (totalLength - 1)
        val downstreamLength = targetEnd - reqStart + 1
        val continuationStart = headLength.toLong()
        val continuationEnd = targetEnd
        val downstreamContentRange = "bytes 0-$targetEnd/$totalLength"

        val downstreamHeaders = HeadersBuilder().apply {
            append(HttpHeaders.ContentRange, downstreamContentRange)
            append(HttpHeaders.AcceptRanges, "bytes")
        }.build()

        // Only cache full track streams (open-ended or ending at totalLength - 1)
        val isFullTrackRequest = (reqEnd == null || targetEnd == totalLength - 1)
        val shouldCache = isFullTrackRequest && isCachingEnabled() && streamingMutex.withLock {
            if (trackId in streamingInProgress) false else {
                streamingInProgress.add(trackId)
                true
            }
        }
        val cacheTrack = if (shouldCache) getTrackMetadata(trackId) else null
        val cacheFilename = if (cacheTrack != null) cacheManager.resolveCacheFilename(cacheTrack, downstreamContentType) else null
        val asyncCacheSink = if (cacheFilename != null) {
            cacheManager.openAsyncCacheSink(cacheFilename, trackId, downstreamContentType, totalLength)
        } else null

        // 1. Concurrently launch upstream continuation fetch
        val continuationChannel = ByteChannel(autoFlush = true)
        val upstreamJob = scope.launch {
            try {
                if (continuationStart > continuationEnd) {
                    continuationChannel.close()
                    return@launch
                }

                val streamInfo = streamingUrlRepository.resolveStreamInfo(trackId)
                if (streamInfo == null || streamInfo.protocol != StreamProtocol.PROGRESSIVE) {
                    continuationChannel.cancel(IllegalStateException("No progressive stream for track $trackId"))
                    return@launch
                }

                var currentUrl = streamInfo.url
                var currentOffset = continuationStart
                var bytesRemaining = if (reqEnd != null) (continuationEnd - continuationStart + 1) else Long.MAX_VALUE
                var retryCount = 0
                val maxRetries = 5
                var attemptedRefresh = false
                val chunk = ByteArray(STREAM_CHUNK_BYTES)

                while (bytesRemaining > 0 && !continuationChannel.isClosedForWrite) {
                    var retryUrl: String? = null
                    val fetchResult = runCatching {
                        httpClient.prepareRequest(currentUrl) {
                            method = HttpMethod.Get
                            applyUpstreamHeaders(call, this, currentUrl, includeRange = false)
                            val rangeHeaderVal = if (reqEnd != null) {
                                "bytes=$currentOffset-$continuationEnd"
                            } else {
                                "bytes=$currentOffset-"
                            }
                            headers[HttpHeaders.Range] = rangeHeaderVal
                        }.execute { response ->
                            if (response.status.value >= 400) {
                                logger.w { "Upstream continuation failed for $trackId status=${response.status.value} offset=$currentOffset" }
                                streamingUrlRepository.invalidateCachedStreamUrl(trackId, currentUrl)
                                if (!attemptedRefresh) {
                                    val refreshedUrl = streamingUrlRepository.resolveStreamInfo(trackId, forceRefresh = true)?.url
                                    if (!refreshedUrl.isNullOrBlank() && refreshedUrl != currentUrl) {
                                        retryUrl = refreshedUrl
                                        attemptedRefresh = true
                                        return@execute
                                    }
                                }
                                throw IOException("Upstream status ${response.status.value}")
                            }

                            // Connection established successfully, reset transient connect retry count
                            retryCount = 0

                            val upstreamSource = response.bodyAsChannel()
                            if (response.status == HttpStatusCode.OK && currentOffset > 0) {
                                var toDiscard = currentOffset
                                val discardBuf = ByteArray(STREAM_CHUNK_BYTES)
                                while (toDiscard > 0 && !upstreamSource.isClosedForRead && !continuationChannel.isClosedForWrite) {
                                    val toRead = minOf(toDiscard, discardBuf.size.toLong()).toInt()
                                    val discarded = upstreamSource.readAvailable(discardBuf, 0, toRead)
                                    if (discarded < 0) break
                                    toDiscard -= discarded
                                }
                            }

                            while (bytesRemaining > 0 && !upstreamSource.isClosedForRead && !continuationChannel.isClosedForWrite) {
                                val toRead = minOf(chunk.size.toLong(), bytesRemaining).toInt()
                                val read = upstreamSource.readAvailable(chunk, 0, toRead)
                                if (read < 0) break
                                if (read == 0) continue
                                continuationChannel.writeFully(chunk, 0, read)
                                currentOffset += read
                                if (bytesRemaining != Long.MAX_VALUE) {
                                    bytesRemaining -= read
                                }
                            }
                        }
                    }

                    if (bytesRemaining <= 0) {
                        break
                    }

                    val failure = fetchResult.exceptionOrNull()
                    if (failure != null) {
                        if (failure is CancellationException) throw failure
                        if (continuationChannel.isClosedForWrite) {
                            // Downstream player socket closed or cancelled
                            return@launch
                        }

                        retryCount++
                        if (retryCount <= maxRetries) {
                            logger.w(failure) {
                                "Upstream continuation glitch for $trackId at offset $currentOffset " +
                                    "(attempt $retryCount/$maxRetries). Retrying..."
                            }

                            if (retryCount >= 2 && !attemptedRefresh) {
                                val refreshedUrl = streamingUrlRepository.resolveStreamInfo(trackId, forceRefresh = true)?.url
                                if (!refreshedUrl.isNullOrBlank() && refreshedUrl != currentUrl) {
                                    currentUrl = refreshedUrl
                                    attemptedRefresh = true
                                }
                            }

                            val backoffMs = minOf(500L * (1L shl (retryCount - 1)), 3000L)
                            delay(backoffMs)
                            continue
                        } else {
                            logger.e(failure) { "Upstream continuation fetch exhausted $maxRetries retries for $trackId at offset $currentOffset" }
                            continuationChannel.cancel(failure)
                            return@launch
                        }
                    }

                    if (retryUrl != null) {
                        currentUrl = retryUrl
                        delay(200)
                        continue
                    }

                    // If upstream closed before reading expected bytes, retry from currentOffset
                    if (bytesRemaining <= 0) {
                        break
                    }

                    if (reqEnd != null && currentOffset <= continuationEnd) {
                        retryCount++
                        if (retryCount <= maxRetries) {
                            val backoffMs = minOf(500L * (1L shl (retryCount - 1)), 3000L)
                            delay(backoffMs)
                            continue
                        }
                    }
                    break
                }
                continuationChannel.close()
            } catch (e: Throwable) {
                continuationChannel.cancel(e)
            }
        }

        // 2. Respond immediately to the player with HTTP 206 Partial Content without blocking
        call.respond(object : OutgoingContent.WriteChannelContent() {
            override val status: HttpStatusCode = HttpStatusCode.PartialContent
            override val headers: Headers = downstreamHeaders
            override val contentLength: Long = downstreamLength
            override val contentType: ContentType = ContentType.parse(downstreamContentType)

            override suspend fun writeTo(channel: io.ktor.utils.io.ByteWriteChannel) {
                val writeStart = TimeSource.Monotonic.markNow()
                logger.i { "[Req] start writing spliced body for $trackId cache=${cacheFilename != null} totalLength=$downstreamLength" }

                var totalBytesWritten = 0L
                var completed = false

                try {
                    // Step 1: Write 512KB RAM head buffer directly to player socket (<2ms TTFB)
                    val headToWrite = minOf(headLength.toLong(), downstreamLength).toInt()
                    channel.writeFully(headData, 0, headToWrite)
                    channel.flush()
                    totalBytesWritten += headToWrite
                    asyncCacheSink?.write(headData, 0, headToWrite)
                    logger.i { "[Req] Spliced $headToWrite bytes from RAM to player in ${writeStart.elapsedNow()} for $trackId" }

                    // Step 2: Stream remaining bytes seamlessly from continuationChannel
                    if (continuationStart <= continuationEnd) {
                        val buffer = ByteArray(STREAM_CHUNK_BYTES)
                        while (!continuationChannel.isClosedForRead) {
                            val read = continuationChannel.readAvailable(buffer, 0, buffer.size)
                            if (read < 0) break
                            if (read == 0) continue
                            channel.writeFully(buffer, 0, read)
                            channel.flush()
                            asyncCacheSink?.write(buffer, 0, read)
                            totalBytesWritten += read
                        }

                        continuationChannel.closedCause?.let { cause ->
                            throw cause
                        }
                    }

                    if (totalBytesWritten < downstreamLength) {
                        throw IOException("Premature end of spliced stream: wrote $totalBytesWritten of $downstreamLength bytes")
                    }

                    completed = (totalBytesWritten == downstreamLength)
                    logger.i { "[Req] Spliced playback finished for $trackId totalBytesWritten=$totalBytesWritten in ${writeStart.elapsedNow()}" }
                } catch (e: Throwable) {
                    if (e is CancellationException) throw e
                    if (e.isClientDisconnect()) {
                        logger.i(e) { "Player disconnected during spliced playback for track $trackId" }
                    } else {
                        logger.w(e) { "Error writing spliced stream for $trackId" }
                    }
                    throw e
                } finally {
                    continuationChannel.cancel(null)
                    upstreamJob.cancel()
                    asyncCacheSink?.complete(totalBytesWritten, success = completed)
                    if (shouldCache) {
                        streamingMutex.withLock { streamingInProgress.remove(trackId) }
                    }
                }
            }
        })
        return true
    }

    private suspend fun respondWithUpstream(
        call: ApplicationCall,
        upstream: HttpResponse,
        upstreamUrl: String,
        trackId: String,
        requestMethod: HttpMethod
    ) {
        if (upstream.status.value >= 400 && isCachingEnabled()) {
            val cachedEntry = cacheManager.findCachedEntry(trackId)
            if (cachedEntry != null) {
                val (filePath, entry) = cachedEntry
                logger.d { "Falling back to cache for track $trackId after upstream error" }
                if (requestMethod == HttpMethod.Get) {
                    cacheManager.serveCacheGet(call, filePath, entry)
                } else {
                    cacheManager.serveCacheHead(call, entry)
                }
                return
            }
        }

        val downstreamHeaders = upstreamHeadersToForward(upstream)
        val downstreamContentLength = downstreamHeaders[HttpHeaders.ContentLength]?.toLongOrNull()
        logger.v {
            "Proxy response headers for $trackId status=${upstream.status.value} " +
                    "contentType=${downstreamHeaders[HttpHeaders.ContentType]} " +
                    "contentLength=${downstreamHeaders[HttpHeaders.ContentLength]} " +
                    "contentRange=${downstreamHeaders[HttpHeaders.ContentRange]} " +
                    "acceptRanges=${downstreamHeaders[HttpHeaders.AcceptRanges]}"
        }

        if (requestMethod == HttpMethod.Head) {
            call.respond(object : OutgoingContent.NoContent() {
                override val status: HttpStatusCode = upstream.status
                override val headers: Headers = downstreamHeaders.passthroughForPlayer()
                override val contentLength: Long? = downstreamContentLength
            })
            return
        }

        // Support caching of both 200 OK and 206 Partial Content starting from byte 0
        val contentRangeHeader = downstreamHeaders[HttpHeaders.ContentRange]
        val parsedRange = contentRangeHeader?.let { parseContentRangeHeader(it) }
        val isFullRange206 = upstream.status == HttpStatusCode.PartialContent &&
                parsedRange != null && parsedRange.start == 0L &&
                (parsedRange.totalSize == null || parsedRange.end + 1 == parsedRange.totalSize)

        val isCacheableStatus = upstream.status == HttpStatusCode.OK || isFullRange206
        val expectedLength = if (isFullRange206) (parsedRange.totalSize ?: downstreamContentLength) else downstreamContentLength

        val shouldCache = requestMethod == HttpMethod.Get && isCachingEnabled() && isCacheableStatus && streamingMutex.withLock {
            if (trackId in streamingInProgress) {
                false
            } else {
                streamingInProgress.add(trackId)
                true
            }
        }
        val cacheTrack = if (shouldCache) getTrackMetadata(trackId) else null
        val cacheFilename = if (cacheTrack != null) {
            cacheManager.resolveCacheFilename(cacheTrack, downstreamHeaders[HttpHeaders.ContentType])
        } else {
            null
        }
        val trackIdForCache = trackId
        val cacheContentType = downstreamHeaders[HttpHeaders.ContentType] ?: "application/octet-stream"

        call.respond(object : OutgoingContent.WriteChannelContent() {
            override val status: HttpStatusCode = upstream.status
            override val headers: Headers = downstreamHeaders.passthroughForPlayer()
            override val contentLength: Long? = downstreamContentLength
            override val contentType: ContentType? =
                downstreamHeaders[HttpHeaders.ContentType]?.let(ContentType.Companion::parse)

            override suspend fun writeTo(channel: io.ktor.utils.io.ByteWriteChannel) {
                val writeStart = TimeSource.Monotonic.markNow()
                logger.i { "[Req] start writing body for $trackId status=${upstream.status} cache=${cacheFilename != null} expected=$expectedLength" }
                try {
                    writeBody(channel)
                    logger.i { "[Req] finished writing body for $trackId in ${writeStart.elapsedNow()}" }
                } catch (e: Throwable) {
                    logger.i(e) { "[Req] body write for $trackId aborted after ${writeStart.elapsedNow()}: ${e::class.simpleName}: ${e.message}" }
                    throw e
                }
            }

            private suspend fun writeBody(channel: io.ktor.utils.io.ByteWriteChannel) {
                var completed = false
                var totalBytes = 0L
                var headBytesBuffered = 0
                val headBuffer = ByteArray(RamHeadBufferPool.HEAD_BUFFER_SIZE)
                val asyncCacheSink = if (cacheFilename != null) {
                    cacheManager.openAsyncCacheSink(cacheFilename, trackIdForCache, cacheContentType, expectedLength)
                } else null

                val initialRangeStart = parsedRange?.start ?: 0L
                val chunkBuffer = ByteArray(STREAM_CHUNK_BYTES)
                var currentSource: io.ktor.utils.io.ByteReadChannel? = upstream.bodyAsChannel()
                var currentUrl = upstreamUrl
                var retryCount = 0
                val maxMidStreamRetries = 4

                try {
                    while (!channel.isClosedForWrite) {
                        var streamGlitch: Throwable? = null
                        val source = currentSource
                        if (source != null) {
                            try {
                                while (!channel.isClosedForWrite) {
                                    val read = source.readAvailable(chunkBuffer, 0, chunkBuffer.size)
                                    if (read < 0) break
                                    if (read == 0) continue

                                    channel.writeFully(chunkBuffer, 0, read)
                                    channel.flush()
                                    asyncCacheSink?.write(chunkBuffer, 0, read)
                                    totalBytes += read

                                    if (headBytesBuffered < headBuffer.size) {
                                        val toCopy = minOf(read, headBuffer.size - headBytesBuffered)
                                        chunkBuffer.copyInto(headBuffer, destinationOffset = headBytesBuffered, startIndex = 0, endIndex = toCopy)
                                        headBytesBuffered += toCopy
                                    }

                                    retryCount = 0

                                    if (expectedLength != null && totalBytes >= expectedLength) {
                                        break
                                    }
                                }
                            } catch (e: Throwable) {
                                if (e is CancellationException) throw e
                                if (e.isClientDisconnect() || channel.isClosedForWrite) {
                                    throw e
                                }
                                streamGlitch = e
                            }
                        }

                        val isFinished = expectedLength != null && totalBytes >= expectedLength
                        if (isFinished || (streamGlitch == null && expectedLength == null && totalBytes > 0)) {
                            completed = true
                            break
                        }

                        if (channel.isClosedForWrite) {
                            break
                        }

                        retryCount++
                        if (retryCount > maxMidStreamRetries) {
                            if (streamGlitch != null) throw streamGlitch
                            break
                        }

                        val resumeOffset = initialRangeStart + totalBytes
                        logger.w(streamGlitch) {
                            "Transient upstream read glitch for $trackId at offset $resumeOffset " +
                                "(attempt $retryCount/$maxMidStreamRetries). Reconnecting..."
                        }

                        val backoffMs = minOf(500L * (1L shl (retryCount - 1)), 3000L)
                        delay(backoffMs)

                        if (retryCount >= 2) {
                            val refreshedUrl = streamingUrlRepository.resolveStreamInfo(trackId, forceRefresh = true)?.url
                            if (!refreshedUrl.isNullOrBlank()) {
                                currentUrl = refreshedUrl
                            }
                        }

                        currentSource = null
                        val reconnectResult = runCatching {
                            httpClient.prepareRequest(currentUrl) {
                                method = HttpMethod.Get
                                applyUpstreamHeaders(call, this, currentUrl, includeRange = false)
                                headers[HttpHeaders.Range] = "bytes=$resumeOffset-"
                            }.execute { reconnectResponse ->
                                if (reconnectResponse.status.value >= 400) {
                                    throw IOException("Upstream continuation reconnect failed with status ${reconnectResponse.status.value}")
                                }
                                val resumedSource = reconnectResponse.bodyAsChannel()
                                if (reconnectResponse.status == HttpStatusCode.OK && resumeOffset > 0) {
                                    var toDiscard = resumeOffset
                                    val discardBuf = ByteArray(STREAM_CHUNK_BYTES)
                                    while (toDiscard > 0 && !resumedSource.isClosedForRead && !channel.isClosedForWrite) {
                                        val toRead = minOf(toDiscard, discardBuf.size.toLong()).toInt()
                                        val discarded = resumedSource.readAvailable(discardBuf, 0, toRead)
                                        if (discarded < 0) break
                                        toDiscard -= discarded
                                    }
                                }

                                while (!channel.isClosedForWrite && (expectedLength == null || totalBytes < expectedLength)) {
                                    val read = resumedSource.readAvailable(chunkBuffer, 0, chunkBuffer.size)
                                    if (read < 0) break
                                    if (read == 0) continue

                                    channel.writeFully(chunkBuffer, 0, read)
                                    channel.flush()
                                    asyncCacheSink?.write(chunkBuffer, 0, read)
                                    totalBytes += read

                                    if (headBytesBuffered < headBuffer.size) {
                                        val toCopy = minOf(read, headBuffer.size - headBytesBuffered)
                                        chunkBuffer.copyInto(headBuffer, destinationOffset = headBytesBuffered, startIndex = 0, endIndex = toCopy)
                                        headBytesBuffered += toCopy
                                    }
                                    retryCount = 0
                                }
                            }
                        }

                        if (expectedLength != null && totalBytes >= expectedLength) {
                            completed = true
                            break
                        }

                        val reconnectError = reconnectResult.exceptionOrNull()
                        if (reconnectError != null) {
                            if (reconnectError is CancellationException) throw reconnectError
                            if (reconnectError.isClientDisconnect() || channel.isClosedForWrite) {
                                throw reconnectError
                            }
                            logger.w(reconnectError) { "Upstream reconnect attempt failed for $trackId" }
                        }
                    }
                    completed = expectedLength == null || totalBytes >= expectedLength
                } finally {
                    asyncCacheSink?.complete(totalBytes, success = completed)
                    if (shouldCache) {
                        streamingMutex.withLock { streamingInProgress.remove(trackIdForCache) }
                    }
                    if (headBytesBuffered > 0 && !RamHeadBufferPool.hasHead(trackIdForCache)) {
                        RamHeadBufferPool.putHead(
                            trackId = trackIdForCache,
                            data = headBuffer.copyOf(headBytesBuffered),
                            contentType = cacheContentType,
                            totalLength = expectedLength ?: totalBytes
                        )
                    }
                }
            }
        })
    }

    private fun rewriteHlsManifest(
        manifest: String,
        baseUrl: String,
        serverUrl: String,
        trackId: String
    ): String {
        val manifestBase = baseUrl.substringBeforeLast('/')
        val lines = manifest.lines()
        val rewritten = lines.map { line ->
            when {
                line.isBlank() || line.startsWith("#") -> line
                line.startsWith("http") -> {
                    "${serverUrl}/segment/${trackId}?url=${encodeUrl(line)}"
                }
                else -> {
                    val absoluteUrl = if (line.startsWith("/")) {
                        val urlBase =
                            baseUrl.substringBefore("://") + "://" + baseUrl.substringAfter("://")
                                .substringBefore('/')
                        "$urlBase$line"
                    } else {
                        "$manifestBase/$line"
                    }
                    "${serverUrl}/segment/${trackId}?url=${encodeUrl(absoluteUrl)}"
                }
            }
        }
        return rewritten.joinToString("\n")
    }

    private fun rewriteDashManifest(
        manifest: String,
        baseUrl: String,
        serverUrl: String,
        trackId: String
    ): String {
        val manifestBase = baseUrl.substringBeforeLast('/')
        var rewritten = manifest

        val urlRegex = Regex("""(BaseURL|SegmentURL|Location)>([^<]+)""")
        rewritten = urlRegex.replace(rewritten) { match ->
            val tag = match.groupValues[1]
            val url = match.groupValues[2]
            val absoluteUrl = if (url.startsWith("http")) url else "$manifestBase/$url"
            "$tag>${serverUrl}/segment/${trackId}?url=${encodeUrl(absoluteUrl)}"
        }

        val srcRegex = Regex("""src="([^"]+)"""")
        rewritten = srcRegex.replace(rewritten) { match ->
            val url = match.groupValues[1]
            val absoluteUrl = if (url.startsWith("http")) url else "$manifestBase/$url"
            """src="${serverUrl}/segment/${trackId}?url=${encodeUrl(absoluteUrl)}"""
        }

        return rewritten
    }

    /**
     * True when the failure came from writing to the player's socket (seek, skip or stop closes
     * it mid-stream), as opposed to a failure fetching from upstream.
     */
    internal fun Throwable.isClientDisconnect(): Boolean =
        generateSequence(this) { it.cause }.take(10).any {
            it is ChannelWriteException || it is ClosedWriteChannelException ||
                (it is IOException && it.message?.contains("Broken pipe", ignoreCase = true) == true)
        }

    private fun encodeUrl(url: String): String = url.encodeURLParameter()
    private fun decodeUrl(encoded: String): String = encoded.decodeURLQueryComponent()

    private fun applyUpstreamHeaders(
        call: ApplicationCall,
        builder: io.ktor.client.request.HttpRequestBuilder,
        targetUrl: String,
        includeRange: Boolean
    ) {
        if (includeRange) {
            call.forwardRequestHeaderIfPresent(builder.headers, HttpHeaders.Range)
            call.forwardRequestHeaderIfPresent(builder.headers, HttpHeaders.IfRange)
        }
        call.forwardRequestHeaderIfPresent(builder.headers, HttpHeaders.Accept)

        if (isYouTubeOrGoogleVideo(targetUrl)) {
            builder.applyStreamClientHeaders(targetUrl)
        } else {
            call.forwardRequestHeaderIfPresent(builder.headers, HttpHeaders.UserAgent)
            call.forwardRequestHeaderIfPresent(builder.headers, HttpHeaders.Referrer)
        }
    }

    private fun ApplicationCall.forwardRequestHeaderIfPresent(
        builder: HeadersBuilder,
        headerName: String
    ) {
        request.headers.getAll(headerName)?.forEach { value ->
            builder.append(headerName, value)
        }
    }

    private fun upstreamHeadersToForward(upstream: HttpResponse): Headers {
        return HeadersBuilder().apply {
            upstream.headers.forEach { name, values ->
                if (!name.equals(HttpHeaders.TransferEncoding, ignoreCase = true) &&
                    !name.equals(HttpHeaders.Connection, ignoreCase = true)
                ) {
                    values.forEach { value -> append(name, value) }
                }
            }
        }.build()
    }

}

internal fun isYouTubeOrGoogleVideo(url: String): Boolean {
    val host = runCatching { Url(url).host.lowercase() }.getOrNull() ?: return false
    return host == "googlevideo.com" || host.endsWith(".googlevideo.com") ||
        host == "youtube.com" || host.endsWith(".youtube.com") ||
        host == "youtu.be" || host.endsWith(".youtu.be")
}

internal fun io.ktor.client.request.HttpRequestBuilder.applyStreamClientHeaders(targetUrl: String) {
    if (isYouTubeOrGoogleVideo(targetUrl)) {
        val clientParam = runCatching { Url(targetUrl).parameters["c"]?.uppercase() }.getOrNull()
            ?: Regex("""[?&]c=([^&#]+)""").find(targetUrl)?.groupValues?.get(1)?.uppercase()

        when (clientParam) {
            "ANDROID" -> {
                headers[HttpHeaders.UserAgent] = StreamProxy.ANDROID_USER_AGENT
                headers.remove(HttpHeaders.Referrer)
                headers.remove("Referer")
                headers.remove(HttpHeaders.Origin)
            }
            "IOS" -> {
                headers[HttpHeaders.UserAgent] = StreamProxy.IOS_USER_AGENT
                headers.remove(HttpHeaders.Referrer)
                headers.remove("Referer")
                headers.remove(HttpHeaders.Origin)
            }
            "VISIONOS" -> {
                headers[HttpHeaders.UserAgent] = StreamProxy.VISIONOS_USER_AGENT
                headers.remove(HttpHeaders.Referrer)
                headers.remove("Referer")
                headers.remove(HttpHeaders.Origin)
            }
            else -> {
                headers[HttpHeaders.Referrer] = StreamProxy.YOUTUBE_REFERER
                headers[HttpHeaders.Origin] = "https://www.youtube.com"
                headers[HttpHeaders.UserAgent] = StreamProxy.BROWSER_USER_AGENT
            }
        }
    }
}

private val PLAYER_PASSTHROUGH_HEADERS = listOf(
    HttpHeaders.ContentRange,
    HttpHeaders.AcceptRanges,
    HttpHeaders.ETag,
    HttpHeaders.LastModified,
)

/**
 * The upstream headers a player actually needs. Copying them all makes Ktor emit duplicate
 * Content-Length/Content-Type (also set through the OutgoingContent) and forwards hop-by-hop
 * headers like `Connection: keep-alive`, which VLC rejects by closing the socket right after
 * the response headers. Content-Range and Accept-Ranges must stay: without them the player
 * treats a 206 as invalid and the stream as unseekable, and e.g. WebM can't be demuxed.
 */
internal fun Headers.passthroughForPlayer(): Headers {
    val upstream = this
    return HeadersBuilder().apply {
        PLAYER_PASSTHROUGH_HEADERS.forEach { name ->
            upstream.getAll(name)?.forEach { value -> append(name, value) }
        }
    }.build()
}

private const val STREAM_CHUNK_BYTES = 64 * 1024

internal data class ParsedContentRange(
    val start: Long,
    val end: Long,
    val totalSize: Long?
)

internal fun parseContentRangeHeader(header: String): ParsedContentRange? {
    val match = Regex("""bytes\s+(\d+)-(\d+)/(\d+|\*)""", RegexOption.IGNORE_CASE).find(header.trim())
        ?: return null
    val start = match.groupValues[1].toLongOrNull() ?: return null
    val end = match.groupValues[2].toLongOrNull() ?: return null
    val totalStr = match.groupValues[3]
    val totalSize = if (totalStr == "*") null else totalStr.toLongOrNull()
    return ParsedContentRange(start, end, totalSize)
}
