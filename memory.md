# Project Memory Bank & Architecture Decision Records (ADR)
_Durable knowledge — decisions, gotchas, conventions for Spotube streaming and playback._

## Architecture decisions
- **Two-Tier Smart Caching & RamHeadBufferPool**:
  - 1.0 MB fixed budget with two 512 KB pre-allocated byte array slots (Slot 0 for active track, Slot 1 for lookahead prefetch).
  - Immediate HTTP 206 response served in <2ms from RAM head while background continuation asynchronously streams `bytes=524288-`.
  - HTTP 206 caching writes to disk on `Content-Range: bytes 0-` using an in-memory dirty cache index and non-blocking asynchronous flash writing (`openAsyncCacheSink`).
- **Slow Network Resilience & Adaptive Buffering**:
  - Android ExoPlayer rebuffer threshold tuned to 2,500ms (`bufferForPlaybackAfterRebufferMs = 2_500`) while keeping initial playback startup instant at 500ms (`bufferForPlaybackMs = 500`). This stops stuttering loops on narrow or fluctuating mobile networks.
  - VLC caching on Desktop and iOS tuned to 2,000ms network caching, 1,000ms file caching, and 500ms clock jitter tolerance to absorb packet drops.
  - Upstream continuation in `StreamProxy` implements an exponential backoff reconnect loop (up to 5 retries from the exact failed byte offset) and falls back to lower/medium bitrate audio streams (e.g. 70–128 kbps Opus/AAC) when high-bitrate streams experience consecutive timeouts or stalls.
- **Bandwidth-Aware Prefetch Throttling**:
  - In `DeviceAudioPlayerQueue`, lookahead prefetching for Track N+1 is paused or cancelled immediately whenever the player enters `BUFFERING`.
  - Warmup prefetch (+5s) requires $\ge 20\text{s}$ buffer headroom ahead on Track N; near-end prefetch requires $\ge 8\text{s}$ buffer cushion.
- **YouTube 403 Hardening (PR #3150)**:
  - Dynamically inspects client query parameter `c=` (`ANDROID`, `IOS`, `VISIONOS`, `WEB`).
  - Mobile streams inject matching mobile User-Agents and explicitly strip both `Origin` and `Referer` headers to satisfy Google Video CDN SABR requirements.

## Gotchas
- **Duplicate Headers in Ktor Partial Content Responses**: VLC player terminates sockets when responses have duplicate `Content-Length` or `Content-Type` headers. Do not manually append these headers to `HeadersBuilder`; let Ktor's `OutgoingContent` properties supply them.
- **Gobley and Cargo 1.100+**: Cargo 1.100 changed directory layouts for build scripts, causing Gobley 0.3.7 to throw `FileNotFoundException`. Always pin `dtolnay/rust-toolchain@1.99.1` in CI workflows.
- **Local Proxy Loopback on Native**: Pre-buffering directly through `httpClient` instead of loopback proxy prevents socket lockups and preserves the proxy pool for active player requests.

## Conventions
- Run `./gradlew :composeApp:jvmTest -x cargoBuildAndroidArm64Debug -x cargoBuildAndroidArmV7Debug -x cargoBuildAndroidX64Debug -x cargoBuildAndroidX86Debug --tests "org.spotube.spotube.core.server.*"` to verify streaming engine changes locally on Windows without Android NDK cross-compilation errors.
