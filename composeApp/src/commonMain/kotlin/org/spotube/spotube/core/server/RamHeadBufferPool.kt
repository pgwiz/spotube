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

import kotlin.time.Clock

/**
 * Fixed 1.0 MB max memory budget pool for pre-buffering audio track heads.
 *
 * Maintains two pre-allocated 512KB buffers:
 * - Slot 0: Active track head (currently playing)
 * - Slot 1: Lookahead prefetch track head (track N+1)
 *
 * Pre-allocated buffers ensure zero runtime memory allocations and constant heap delta.
 */
object RamHeadBufferPool {
    const val HEAD_BUFFER_SIZE: Int = 512 * 1024 // 524,288 bytes (512 KB)

    data class HeadInfo(
        val trackId: String,
        val length: Int,
        val contentType: String? = null,
        val totalLength: Long? = null
    )

    private class Slot(val index: Int) {
        val buffer = ByteArray(HEAD_BUFFER_SIZE)
        var trackId: String? = null
        var length: Int = 0
        var contentType: String? = null
        var totalLength: Long? = null
        var lastAccessedMs: Long = 0L

        fun clear() {
            trackId = null
            length = 0
            contentType = null
            totalLength = null
            lastAccessedMs = 0L
        }
    }

    private val lock = Any()
    // Exactly 2 pre-allocated 512KB slots = 1.0 MB total RAM budget
    private val slots = arrayOf(Slot(0), Slot(1))

    fun putHead(
        trackId: String,
        data: ByteArray,
        contentType: String? = null,
        totalLength: Long? = null,
        isLookahead: Boolean = false
    ) {
        if (trackId.isBlank()) return
        synchronized(lock) {
            // Find existing slot for this track, or select target slot
            val targetSlot = slots.firstOrNull { it.trackId == trackId } ?: run {
                if (isLookahead) {
                    when {
                        slots[1].trackId == null -> slots[1]
                        slots[0].trackId == null -> slots[0]
                        else -> slots[1] // Overwrite lookahead slot
                    }
                } else {
                    when {
                        slots[0].trackId == null -> slots[0]
                        slots[1].trackId == null -> slots[1]
                        else -> slots.minByOrNull { it.lastAccessedMs } ?: slots[0]
                    }
                }
            }

            val copyLength = minOf(data.size, HEAD_BUFFER_SIZE)
            data.copyInto(targetSlot.buffer, destinationOffset = 0, startIndex = 0, endIndex = copyLength)
            targetSlot.trackId = trackId
            targetSlot.length = copyLength
            targetSlot.contentType = contentType
            targetSlot.totalLength = totalLength
            targetSlot.lastAccessedMs = Clock.System.now().toEpochMilliseconds()
        }
    }

    fun putHead(trackId: String, data: ByteArray) {
        putHead(trackId = trackId, data = data, contentType = null, totalLength = null, isLookahead = false)
    }

    fun getHead(trackId: String): ByteArray? {
        if (trackId.isBlank()) return null
        synchronized(lock) {
            val slot = slots.firstOrNull { it.trackId == trackId && it.length > 0 } ?: return null
            slot.lastAccessedMs = Clock.System.now().toEpochMilliseconds()
            return slot.buffer.copyOf(slot.length)
        }
    }

    fun getHeadInfo(trackId: String): HeadInfo? {
        if (trackId.isBlank()) return null
        synchronized(lock) {
            val slot = slots.firstOrNull { it.trackId == trackId && it.length > 0 } ?: return null
            slot.lastAccessedMs = Clock.System.now().toEpochMilliseconds()
            return HeadInfo(
                trackId = slot.trackId!!,
                length = slot.length,
                contentType = slot.contentType,
                totalLength = slot.totalLength
            )
        }
    }

    fun hasHead(trackId: String): Boolean {
        if (trackId.isBlank()) return false
        synchronized(lock) {
            return slots.any { it.trackId == trackId && it.length > 0 }
        }
    }

    fun evict(trackId: String): Boolean {
        if (trackId.isBlank()) return false
        synchronized(lock) {
            var evicted = false
            for (slot in slots) {
                if (slot.trackId == trackId) {
                    slot.clear()
                    evicted = true
                }
            }
            return evicted
        }
    }

    fun clear() {
        synchronized(lock) {
            for (slot in slots) {
                slot.clear()
            }
        }
    }
}
