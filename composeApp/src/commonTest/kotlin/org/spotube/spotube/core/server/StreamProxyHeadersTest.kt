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

import io.ktor.http.HeadersBuilder
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class StreamProxyHeadersTest {

    // Lowercase names and values as googlevideo sends them
    private val upstream = HeadersBuilder().apply {
        append("accept-ranges", "bytes")
        append("alt-svc", "h3=\":443\"")
        append("cache-control", "private, max-age=21298")
        append("connection", "keep-alive")
        append("content-length", "3523553")
        append("content-range", "bytes 0-3523552/3523553")
        append("content-type", "audio/webm")
        append("last-modified", "Tue, 18 Feb 2025 22:42:11 GMT")
    }.build()

    @Test
    fun `keeps the headers a player needs to seek`() {
        val forwarded = upstream.passthroughForPlayer()
        assertEquals("bytes 0-3523552/3523553", forwarded["Content-Range"])
        assertEquals("bytes", forwarded["Accept-Ranges"])
        assertEquals("Tue, 18 Feb 2025 22:42:11 GMT", forwarded["Last-Modified"])
    }

    @Test
    fun `drops headers that are duplicated or hop-by-hop`() {
        val forwarded = upstream.passthroughForPlayer()
        // Length and type are set on the OutgoingContent; forwarding them too emits them twice
        assertNull(forwarded["Content-Length"])
        assertNull(forwarded["Content-Type"])
        assertNull(forwarded["Connection"])
        assertNull(forwarded["Alt-Svc"])
        assertNull(forwarded["Cache-Control"])
    }

    @Test
    fun `correctly parses content-range header for 206 caching`() {
        val parsed = parseContentRangeHeader("bytes 0-3523552/3523553")
        assertEquals(0L, parsed?.start)
        assertEquals(3523552L, parsed?.end)
        assertEquals(3523553L, parsed?.totalSize)

        val partial = parseContentRangeHeader("bytes 524288-1048575/3523553")
        assertEquals(524288L, partial?.start)
        assertEquals(1048575L, partial?.end)
        assertEquals(3523553L, partial?.totalSize)

        val unknownTotal = parseContentRangeHeader("bytes 0-1000/*")
        assertEquals(0L, unknownTotal?.start)
        assertEquals(1000L, unknownTotal?.end)
        assertNull(unknownTotal?.totalSize)

        assertNull(parseContentRangeHeader("invalid-header"))
    }

    @Test
    fun `RamHeadBufferPool correctly stores and retrieves 512KB head buffer`() {
        RamHeadBufferPool.clear()
        val trackId = "track_test_1"
        val sampleData = ByteArray(1024) { (it % 128).toByte() }

        RamHeadBufferPool.putHead(
            trackId = trackId,
            data = sampleData,
            contentType = "audio/webm",
            totalLength = 5000000L,
            isLookahead = false
        )

        assertEquals(true, RamHeadBufferPool.hasHead(trackId))
        val retrieved = RamHeadBufferPool.getHead(trackId)
        assertEquals(sampleData.size, retrieved?.size)
        assertEquals(sampleData[0], retrieved?.get(0))
        assertEquals(sampleData[1023], retrieved?.get(1023))

        val info = RamHeadBufferPool.getHeadInfo(trackId)
        assertEquals(trackId, info?.trackId)
        assertEquals("audio/webm", info?.contentType)
        assertEquals(5000000L, info?.totalLength)

        RamHeadBufferPool.evict(trackId)
        assertEquals(false, RamHeadBufferPool.hasHead(trackId))
        assertNull(RamHeadBufferPool.getHead(trackId))
    }
}
