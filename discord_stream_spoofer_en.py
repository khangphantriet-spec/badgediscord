#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
====================================================================
███╗   ███╗ █████╗  ██████╗███████╗██████╗     ██████╗ ██╗   ██╗
████╗ ████║██╔══██╗██╔════╝██╔════╝██╔══██╗    ██╔══██╗╚██╗ ██╔╝
██╔████╔██║███████║██║     █████╗  ██████╔╝    ██████╔╝ ╚████╔╝ 
██║╚██╔╝██║██╔══██║██║     ██╔══╝  ██╔══██╗    ██╔══██╗  ╚██╔╝  
██║ ╚═╝ ██║██║  ██║╚██████╗███████╗██║  ██║    ██████╔╝   ██║   
╚═╝     ╚═╝╚═╝  ╚═╝ ╚═════╝╚══════╝╚═╝  ╚═╝    ╚═════╝    ╚═╝   

       ███████╗ ██████╗    ██╗    █████╗ 
       ██╔════╝██╔════╝    ██║   ██╔══██╗
       ███████╗██║  ███╗   ╚═╝   ╚██████║
       ╚════██║██║   ██║   ██╗    ╚═══██║
       ███████║╚██████╔╝   ██║    █████╔╝
       ╚══════╝ ╚═════╝    ╚═╝    ╚════╝ 
====================================================================
Discord Screen Share / Stream Spoofer (PC Interactive Edition)
All Rights Reserved to Discord User: sg.9
Official Discord Server: https://discord.gg/5jFFHHj2gy
====================================================================
Automatically joins a Discord voice channel and starts a Go Live screen share
to accumulate streaming hours for the Discord Streaming Profile Badges.

- Zero external dependencies (Pure Python 3.10+ Standard Library).
- Accepts a full channel URL (https://discord.com/channels/GUILD_ID/CHANNEL_ID)
  or a raw Channel ID.
- Supports an optional secondary viewer token to automatically join the same
  channel and watch the stream so hours count toward badge progression.
- Saves configuration locally to stream_state.json for quick reuse.

Disclaimer: Automating user accounts violates Discord's Terms of Service.
Use at your own risk.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import ssl
import struct
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Optional

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

AUTHOR_DISCORD = "sg.9"
SERVER_INVITE = "https://discord.gg/5jFFHHj2gy"

CREDITS_BANNER = r"""
====================================================================
       ███████╗ ██████╗       █████╗ 
       ██╔════╝██╔════╝      ██╔══██╗
       ███████╗██║  ███╗     ╚██████║
       ╚════██║██║   ██║      ╚═══██║
       ███████║╚██████╔╝ ██╗  █████╔╝
       ╚══════╝ ╚═════╝  ╚═╝  ╚════╝ 

   ★ DEVELOPED & COPYRIGHTED BY DISCORD: sg.9 ★
   ★ OFFICIAL DISCORD SERVER: https://discord.gg/5jFFHHj2gy ★
   ★ EDITION: PC Interactive Tool (تعمل على الكمبيوتر PC) ★
====================================================================
"""

STATE_FILE = Path("stream_state.json")
DISCORD_API = "https://discord.com/api/v10"
GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "discord/1.0.9168 Chrome/128.0.6613.186 Electron/32.2.2 Safari/537.36"
)

STREAM_BADGE_TIERS = [
    (1, "Tier 1: Newcomer (1 hour)"),
    (5, "Tier 2: Fledgling (5 hours)"),
    (20, "Tier 3: Breakout (20 hours)"),
    (75, "Tier 4: Standout (75 hours)"),
    (150, "Tier 5: Trendsetter (150 hours)"),
    (300, "Tier 6: Headliner (300 hours)"),
    (500, "Tier 7: Star (500 hours)"),
    (1000, "Tier 8: Sensation (1,000 hours)"),
    (2000, "Tier 9: Visionary (2,000 hours)"),
    (5000, "Tier 10: Phenomenon (5,000+ hours)"),
]


def make_super_properties() -> str:
    props = {
        "os": "Windows",
        "browser": "Discord Client",
        "release_channel": "stable",
        "client_version": "1.0.9168",
        "os_version": "10.0.22631",
        "os_arch": "x64",
        "app_arch": "x64",
        "system_locale": "en-US",
        "browser_user_agent": USER_AGENT,
        "browser_version": "32.2.2",
        "client_build_number": 343349,
        "native_build_number": 53915,
        "client_event_source": None,
    }
    return base64.b64encode(json.dumps(props, separators=(",", ":")).encode()).decode()


# =====================================================================
# Pure Python RFC 6455 Async WebSocket Client (Zero External Packages)
# =====================================================================
class AsyncWebSocket:
    """Lightweight RFC 6455 WebSocket client built with the Python standard library."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.closed = False
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        parsed = re.match(r"^(wss?|https?)://([^/:]+)(?::(\d+))?(/.*)?$", self.url)
        if not parsed:
            raise ValueError(f"Invalid WebSocket URL: {self.url}")

        scheme, host, port_str, path = parsed.groups()
        use_ssl = scheme in ("wss", "https")
        port = int(port_str) if port_str else (443 if use_ssl else 80)
        path = path or "/"

        ssl_ctx = ssl.create_default_context() if use_ssl else None
        self.reader, self.writer = await asyncio.open_connection(
            host, port, ssl=ssl_ctx, server_hostname=host if use_ssl else None
        )

        ws_key = base64.b64encode(os.urandom(16)).decode("ascii")
        handshake = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {ws_key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"User-Agent: {USER_AGENT}\r\n"
            f"Origin: https://discord.com\r\n"
            f"\r\n"
        )
        self.writer.write(handshake.encode("utf-8"))
        await self.writer.drain()

        status_line = await self.reader.readline()
        if not status_line.startswith(b"HTTP/1.1 101"):
            raise ConnectionError(
                f"WebSocket upgrade failed: {status_line.decode(errors='ignore').strip()}"
            )

        while True:
            header_line = await self.reader.readline()
            if header_line in (b"\r\n", b"\n", b""):
                break

    async def send_json(self, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        await self._send_frame(0x1, data)

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self.closed or not self.writer:
            return
        async with self._write_lock:
            header = bytearray()
            header.append(0x80 | (opcode & 0x0F))
            length = len(payload)
            mask_bit = 0x80
            if length < 126:
                header.append(mask_bit | length)
            elif length < 65536:
                header.append(mask_bit | 126)
                header.extend(struct.pack("!H", length))
            else:
                header.append(mask_bit | 127)
                header.extend(struct.pack("!Q", length))

            mask_key = os.urandom(4)
            header.extend(mask_key)
            masked_payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
            self.writer.write(header + masked_payload)
            await self.writer.drain()

    async def recv_json(self) -> Optional[dict[str, Any]]:
        if not self.reader or self.closed:
            return None

        fragments = bytearray()
        message_opcode: Optional[int] = None

        while True:
            first_two = await self.reader.readexactly(2)
            b1, b2 = first_two[0], first_two[1]
            fin = bool(b1 & 0x80)
            opcode = b1 & 0x0F
            masked = bool(b2 & 0x80)
            length = b2 & 0x7F

            if length == 126:
                ext = await self.reader.readexactly(2)
                length = struct.unpack("!H", ext)[0]
            elif length == 127:
                ext = await self.reader.readexactly(8)
                length = struct.unpack("!Q", ext)[0]

            mask_key = await self.reader.readexactly(4) if masked else b""
            payload = await self.reader.readexactly(length) if length > 0 else b""
            if masked:
                payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))

            if opcode == 0x8:  # Close frame
                close_code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1000
                self.closed = True
                raise ConnectionResetError(f"Server closed connection (Close Code: {close_code})")
            elif opcode == 0x9:  # Ping
                await self._send_frame(0xA, payload)
                continue
            elif opcode == 0xA:  # Pong
                continue
            elif opcode in (0x1, 0x2):
                message_opcode = opcode
                fragments.extend(payload)
            elif opcode == 0x0:
                fragments.extend(payload)

            if fin:
                if message_opcode == 0x1:
                    return json.loads(fragments.decode("utf-8", errors="replace"))
                # Ignore binary frames (e.g., DAVE MLS binary messages on voice socket)
                fragments.clear()
                message_opcode = None

    async def close(self) -> None:
        if self.closed:
            return
        if self.writer:
            try:
                await self._send_frame(0x8, struct.pack("!H", 1000))
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        self.closed = True


# =====================================================================
# Discord REST Helpers (Token Validation & Channel / Link Resolution)
# =====================================================================
def discord_api_get(endpoint: str, token: str, api_base: str = DISCORD_API) -> dict[str, Any]:
    req = urllib.request.Request(
        f"{api_base}{endpoint}",
        headers={
            "Authorization": token.strip(),
            "User-Agent": USER_AGENT,
            "X-Super-Properties": make_super_properties(),
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_room_input(
    raw_input: str, token: str, api_base: str = DISCORD_API
) -> tuple[Optional[str], str, str]:
    """
    Parses a Discord channel URL or raw Channel ID and returns:
    (guild_id, channel_id, channel_label)
    Supports:
    1. Guild channel link: https://discord.com/channels/GUILD_ID/CHANNEL_ID
    2. Private/DM call link: https://discord.com/channels/@me/CHANNEL_ID
    3. Raw Channel ID: 123456789012345678 (resolves guild_id via REST API)
    """
    text = raw_input.strip()

    # Check full Discord channel URL
    match_url = re.search(r"discord(?:app)?\.com/channels/(@me|\d+)/(\d+)", text)
    if match_url:
        g_part, c_id = match_url.group(1), match_url.group(2)
        guild_id = None if g_part == "@me" else g_part
        try:
            ch_info = discord_api_get(f"/channels/{c_id}", token, api_base=api_base)
            ch_name = ch_info.get("name") or f"DM/Call ({c_id})"
            guild_id = ch_info.get("guild_id", guild_id)
            return guild_id, c_id, ch_name
        except Exception:
            return guild_id, c_id, f"Channel {c_id}"

    # Check GUILD_ID/CHANNEL_ID or GUILD_ID:CHANNEL_ID pair
    match_pair = re.match(r"^(\d{15,22})[:/\s]+(\d{15,22})$", text)
    if match_pair:
        g_id, c_id = match_pair.group(1), match_pair.group(2)
        return g_id, c_id, f"Channel {c_id}"

    # Check raw Channel ID
    match_id = re.search(r"(\d{15,22})", text)
    if not match_id:
        raise ValueError(
            "Invalid room ID or URL. Please provide a valid Channel ID or full link:\n"
            "https://discord.com/channels/GUILD_ID/CHANNEL_ID"
        )

    channel_id = match_id.group(1)
    try:
        ch_info = discord_api_get(f"/channels/{channel_id}", token, api_base=api_base)
        guild_id = ch_info.get("guild_id")
        ch_name = ch_info.get("name") or f"Private Call ({channel_id})"
        return guild_id, channel_id, ch_name
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise ValueError("Invalid Discord token (401 Unauthorized). Check your token.") from e
        elif e.code == 403:
            raise ValueError(
                "Access denied to this channel (403 Forbidden). Make sure your account has permission."
            ) from e
        elif e.code == 404:
            raise ValueError(
                "Channel not found (404 Not Found). Check the Channel ID and ensure your account is in the server."
            ) from e
        raise ValueError(f"Failed to fetch channel info (HTTP {e.code}).") from e


# =====================================================================
# Voice / Stream RTC WebSocket Keep-Alive Session
# =====================================================================
class RTCVoiceSession:
    """
    Connects to Discord's Voice Server or Stream Server WebSocket to keep
    the voice and Go Live session active without timing out after 60 seconds.
    """

    def __init__(
        self,
        label: str,
        endpoint: str,
        server_id: str,
        channel_id: str,
        user_id: str,
        session_id: str,
        voice_token: str,
        is_stream: bool = False,
    ) -> None:
        self.label = label
        self.endpoint = endpoint
        self.server_id = server_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.session_id = session_id
        self.voice_token = voice_token
        self.is_stream = is_stream
        self.ws: Optional[AsyncWebSocket] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._hb_task: Optional[asyncio.Task[None]] = None
        self.running = False

    def start(self) -> None:
        self.running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self.running = False
        if self._hb_task:
            self._hb_task.cancel()
        if self.ws:
            await self.ws.close()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        url = self.endpoint
        if not url.startswith("wss://") and not url.startswith("ws://"):
            url = f"wss://{url}"
        if "?v=" not in url:
            url = f"{url.rstrip('/')}/?v=8"

        try:
            self.ws = AsyncWebSocket(url)
            await self.ws.connect()

            identify_payload: dict[str, Any] = {
                "op": 0,
                "d": {
                    "server_id": str(self.server_id),
                    "channel_id": str(self.channel_id),
                    "user_id": str(self.user_id),
                    "session_id": self.session_id,
                    "token": self.voice_token,
                    "video": True,
                    "streams": [
                        {
                            "type": "screen" if self.is_stream else "video",
                            "rid": "100",
                            "quality": 100,
                        }
                    ],
                },
            }
            await self.ws.send_json(identify_payload)

            while self.running:
                msg = await self.ws.recv_json()
                if not msg:
                    break
                op = msg.get("op")
                d = msg.get("d") or {}

                if op == 8:  # Hello
                    interval = d.get("heartbeat_interval", 15000) / 1000.0
                    if self._hb_task:
                        self._hb_task.cancel()
                    self._hb_task = asyncio.create_task(self._heartbeat_loop(interval))

                elif op == 2:  # Ready
                    modes = d.get("modes") or ["aead_xchacha20_poly1305_rtpsize"]
                    selected_mode = (
                        "aead_aes256_gcm_rtpsize"
                        if "aead_aes256_gcm_rtpsize" in modes
                        else modes[0]
                    )
                    select_protocol = {
                        "op": 1,
                        "d": {
                            "protocol": "udp",
                            "data": {
                                "address": d.get("ip", "127.0.0.1"),
                                "port": d.get("port", 1337),
                                "mode": selected_mode,
                            },
                            "codecs": [
                                {
                                    "name": "opus",
                                    "type": "audio",
                                    "priority": 1000,
                                    "payload_type": 120,
                                },
                                {
                                    "name": "H264",
                                    "type": "video",
                                    "priority": 1000,
                                    "payload_type": 101,
                                    "rtx_payload_type": 102,
                                    "encode": True,
                                    "decode": True,
                                },
                            ],
                            "rtc_connection_id": str(uuid.uuid4()),
                        },
                    }
                    await self.ws.send_json(select_protocol)

                    # Announce active video/screen stream SSRCs if assigned
                    streams = d.get("streams") or []
                    ssrc = d.get("ssrc", 1)
                    video_ssrc = streams[0].get("ssrc", ssrc + 1) if streams else ssrc + 1
                    rtx_ssrc = (
                        streams[0].get("rtx_ssrc", video_ssrc + 1) if streams else video_ssrc + 1
                    )
                    await self.ws.send_json(
                        {
                            "op": 12,
                            "d": {
                                "audio_ssrc": ssrc,
                                "video_ssrc": video_ssrc,
                                "rtx_ssrc": rtx_ssrc,
                                "streams": [
                                    {
                                        "type": "video",
                                        "rid": "100",
                                        "ssrc": video_ssrc,
                                        "active": True,
                                        "quality": 100,
                                        "rtx_ssrc": rtx_ssrc,
                                        "max_bitrate": 2500000,
                                        "max_framerate": 30,
                                        "max_resolution": {
                                            "type": "fixed",
                                            "width": 1280,
                                            "height": 720,
                                        },
                                    }
                                ],
                            },
                        }
                    )
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _heartbeat_loop(self, interval: float) -> None:
        try:
            while self.running and self.ws and not self.ws.closed:
                nonce = int(time.time() * 1000)
                await self.ws.send_json({"op": 3, "d": {"t": nonce, "seq_ack": -1}})
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass


# =====================================================================
# Main Gateway Client (Streamer or Viewer Account)
# =====================================================================
class DiscordStreamClient:
    """
    Manages a Discord Gateway WebSocket session:
    - Joins the target voice channel (Opcode 4)
    - Starts a Go Live screen share (Opcode 18) and unpauses it (Opcode 22 paused=False)
    - Or watches the stream when running as a secondary viewer account (Opcode 20)
    """

    def __init__(
        self,
        token: str,
        guild_id: Optional[str],
        channel_id: str,
        role: str = "streamer",  # "streamer" or "viewer"
        self_mute: bool = True,
        self_deaf: bool = True,
        self_video: bool = False,
        gateway_url: str = GATEWAY_URL,
    ) -> None:
        self.token = token.strip()
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.role = role
        self.self_mute = self_mute
        self.self_deaf = self_deaf
        self.self_video = self_video
        self.gateway_url = gateway_url

        self.ws: Optional[AsyncWebSocket] = None
        self.seq: Optional[int] = None
        self.user_id: Optional[str] = None
        self.username: str = "Unknown"
        self.voice_session_id: Optional[str] = None
        self.stream_key: Optional[str] = None
        self.stream_active: bool = False
        self.running: bool = False

        self.voice_rtc: Optional[RTCVoiceSession] = None
        self.stream_rtc: Optional[RTCVoiceSession] = None
        self.ready_event = asyncio.Event()
        self.stream_created_event = asyncio.Event()

    def build_stream_key(self, target_user_id: Optional[str] = None) -> str:
        uid = target_user_id or self.user_id
        if self.guild_id:
            return f"guild:{self.guild_id}:{self.channel_id}:{uid}"
        return f"call:{self.channel_id}:{uid}"

    async def run_forever(self) -> None:
        self.running = True
        backoff = 3
        while self.running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if not self.running:
                    break
                print(f"\n[!] Connection lost ({self.role} - {self.username}): {exc}")
                print(f"[*] Reconnecting in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _connect_and_listen(self) -> None:
        self.ws = AsyncWebSocket(self.gateway_url)
        await self.ws.connect()

        hb_task: Optional[asyncio.Task[None]] = None
        keepalive_task: Optional[asyncio.Task[None]] = None

        try:
            while self.running:
                msg = await self.ws.recv_json()
                if not msg:
                    break

                op = msg.get("op")
                t = msg.get("t")
                s = msg.get("s")
                d = msg.get("d")

                if s is not None:
                    self.seq = s

                if op == 10:  # Hello
                    interval = (d or {}).get("heartbeat_interval", 41250) / 1000.0
                    hb_task = asyncio.create_task(self._gateway_heartbeat_loop(interval))
                    await self._send_identify()

                elif op == 1:  # Heartbeat Request
                    await self.ws.send_json({"op": 1, "d": self.seq})

                elif op == 7:  # Reconnect
                    raise ConnectionResetError("Discord requested a reconnect (Opcode 7)")

                elif op == 9:  # Invalid Session
                    raise ConnectionResetError("Invalid Gateway session (Opcode 9)")

                elif op == 0:  # Dispatch
                    await self._handle_dispatch(t, d or {})
                    if t == "READY" and not keepalive_task:
                        keepalive_task = asyncio.create_task(self._periodic_stream_refresh())
        finally:
            if hb_task:
                hb_task.cancel()
            if keepalive_task:
                keepalive_task.cancel()
            if self.voice_rtc:
                await self.voice_rtc.stop()
            if self.stream_rtc:
                await self.stream_rtc.stop()
            if self.ws:
                await self.ws.close()

    async def _send_identify(self) -> None:
        assert self.ws is not None
        identify = {
            "op": 2,
            "d": {
                "token": self.token,
                "capabilities": 16381,
                "properties": {
                    "os": "Windows",
                    "browser": "Discord Client",
                    "release_channel": "stable",
                    "client_version": "1.0.9168",
                    "os_version": "10.0.22631",
                    "os_arch": "x64",
                    "app_arch": "x64",
                    "system_locale": "en-US",
                    "browser_user_agent": USER_AGENT,
                    "browser_version": "32.2.2",
                    "client_build_number": 343349,
                    "native_build_number": 53915,
                    "client_event_source": None,
                },
                "presence": {
                    "status": "online",
                    "since": 0,
                    "activities": [],
                    "afk": False,
                },
                "compress": False,
                "client_state": {
                    "guild_versions": {},
                    "highest_last_message_id": "0",
                    "read_state_version": 0,
                    "user_guild_settings_version": -1,
                    "user_settings_version": -1,
                    "private_channels_version": "0",
                    "api_code_version": 0,
                },
            },
        }
        await self.ws.send_json(identify)

    async def _handle_dispatch(self, event_name: Optional[str], d: dict[str, Any]) -> None:
        assert self.ws is not None

        if event_name == "READY":
            user = d.get("user") or {}
            self.user_id = user.get("id")
            self.username = user.get("username") or "User"
            self.voice_session_id = d.get("session_id")
            print(f"[+] Logged in ({self.role}): {self.username} (ID: {self.user_id})")

            # 1. Send Opcode 4 to join the voice channel
            await self.join_voice_channel()
            self.ready_event.set()

            # 2. If this account is the streamer, start Go Live screen share (Opcode 18 + Opcode 22)
            if self.role == "streamer":
                await asyncio.sleep(1.2)
                await self.start_screen_share()

        elif event_name == "VOICE_STATE_UPDATE":
            if str(d.get("user_id")) == str(self.user_id):
                self.voice_session_id = d.get("session_id") or self.voice_session_id
                joined_ch = d.get("channel_id")
                if joined_ch is None and self.running:
                    print(
                        f"[!] Account ({self.username}) was disconnected from voice! Rejoining in 3s..."
                    )
                    await asyncio.sleep(3)
                    await self.join_voice_channel()
                    if self.role == "streamer":
                        await asyncio.sleep(1.0)
                        await self.start_screen_share()

        elif event_name == "VOICE_SERVER_UPDATE":
            endpoint = d.get("endpoint")
            voice_token = d.get("token")
            server_id = d.get("guild_id") or d.get("channel_id") or self.channel_id
            if endpoint and voice_token and self.user_id and self.voice_session_id:
                if self.voice_rtc:
                    await self.voice_rtc.stop()
                self.voice_rtc = RTCVoiceSession(
                    label=f"Voice-{self.username}",
                    endpoint=endpoint,
                    server_id=str(server_id),
                    channel_id=str(self.channel_id),
                    user_id=str(self.user_id),
                    session_id=self.voice_session_id,
                    voice_token=voice_token,
                    is_stream=False,
                )
                self.voice_rtc.start()

        elif event_name == "STREAM_CREATE":
            s_key = d.get("stream_key")
            if s_key:
                self.stream_key = s_key
                self.stream_active = True
                self.stream_created_event.set()
                if self.role == "streamer":
                    await self.set_stream_paused(False)
                    print(f"[+] Screen Share (Go Live) is active! Stream Key: {self.stream_key}")

        elif event_name == "STREAM_SERVER_UPDATE":
            endpoint = d.get("endpoint")
            stream_token = d.get("token")
            s_key = d.get("stream_key") or self.stream_key
            if endpoint and stream_token and self.user_id and self.voice_session_id and s_key:
                if self.stream_rtc:
                    await self.stream_rtc.stop()
                self.stream_rtc = RTCVoiceSession(
                    label=f"Stream-{self.username}",
                    endpoint=endpoint,
                    server_id=str(self.guild_id or self.channel_id),
                    channel_id=str(self.channel_id),
                    user_id=str(self.user_id),
                    session_id=self.voice_session_id,
                    voice_token=stream_token,
                    is_stream=True,
                )
                self.stream_rtc.start()

        elif event_name == "STREAM_DELETE":
            if self.role == "streamer" and self.running:
                reason = d.get("reason", "unknown")
                print(f"[!] Stream ended (reason: {reason}). Restarting Screen Share...")
                await asyncio.sleep(2.5)
                await self.start_screen_share()

    async def join_voice_channel(self) -> None:
        """Opcode 4: Voice State Update to join the target voice channel."""
        if not self.ws or self.ws.closed:
            return
        payload = {
            "op": 4,
            "d": {
                "guild_id": self.guild_id,
                "channel_id": self.channel_id,
                "self_mute": self.self_mute,
                "self_deaf": self.self_deaf,
                "self_video": self.self_video,
            },
        }
        await self.ws.send_json(payload)
        print(f"[*] ({self.username}) Joining voice channel ({self.channel_id})...")

    async def start_screen_share(self) -> None:
        """Opcode 18: Stream Create (Go Live) followed by Opcode 22 (Set Stream Paused = False)."""
        if not self.ws or self.ws.closed or not self.user_id:
            return
        stream_type = "guild" if self.guild_id else "call"
        create_stream_payload = {
            "op": 18,
            "d": {
                "type": stream_type,
                "guild_id": self.guild_id,
                "channel_id": self.channel_id,
                "preferred_region": None,
            },
        }
        await self.ws.send_json(create_stream_payload)
        self.stream_key = self.build_stream_key()
        await asyncio.sleep(0.5)
        await self.set_stream_paused(False)

    async def set_stream_paused(self, paused: bool = False) -> None:
        """Opcode 22: Set Stream Paused to ensure the screen share remains active."""
        if not self.ws or self.ws.closed or not self.user_id:
            return
        s_key = self.stream_key or self.build_stream_key()
        await self.ws.send_json(
            {
                "op": 22,
                "d": {
                    "stream_key": s_key,
                    "paused": paused,
                },
            }
        )

    async def watch_stream(self, target_stream_key: str) -> None:
        """Opcode 20: Watch Stream for the secondary viewer account."""
        if not self.ws or self.ws.closed:
            return
        await self.ws.send_json(
            {
                "op": 20,
                "d": {
                    "stream_key": target_stream_key,
                },
            }
        )
        print(f"[+] ({self.username}) Now watching stream ({target_stream_key})!")

    async def _periodic_stream_refresh(self) -> None:
        """Periodically sends Opcode 22 every 60 seconds to keep the stream state active."""
        try:
            while self.running and self.ws and not self.ws.closed:
                await asyncio.sleep(60)
                if self.role == "streamer" and self.stream_key:
                    await self.set_stream_paused(False)
        except asyncio.CancelledError:
            pass

    async def _gateway_heartbeat_loop(self, interval: float) -> None:
        try:
            while self.running and self.ws and not self.ws.closed:
                await self.ws.send_json({"op": 1, "d": self.seq})
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def stop(self) -> None:
        self.running = False
        if self.ws and not self.ws.closed:
            try:
                if self.role == "streamer" and self.stream_key:
                    await self.ws.send_json({"op": 19, "d": {"stream_key": self.stream_key}})
                await self.ws.send_json(
                    {
                        "op": 4,
                        "d": {
                            "guild_id": self.guild_id,
                            "channel_id": None,
                            "self_mute": True,
                            "self_deaf": True,
                            "self_video": False,
                        },
                    }
                )
            except Exception:
                pass
            await self.ws.close()
        if self.voice_rtc:
            await self.voice_rtc.stop()
        if self.stream_rtc:
            await self.stream_rtc.stop()


# =====================================================================
# Configuration & Interactive CLI Menu
# =====================================================================
def load_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "token": "",
        "viewer_token": "",
        "room_input": "",
        "self_mute": True,
        "self_deaf": True,
        "self_video": False,
        "total_streamed_seconds": 0,
    }


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def format_duration(seconds: float) -> str:
    total_sec = int(seconds)
    hrs, rem = divmod(total_sec, 3600)
    mins, secs = divmod(rem, 60)
    return f"{hrs:02d}h:{mins:02d}m:{secs:02d}s"


def get_next_badge_milestone(hours: float) -> str:
    for req_hrs, title in STREAM_BADGE_TIERS:
        if hours < req_hrs:
            remaining = req_hrs - hours
            return f"{title} ({remaining:.1f}h remaining)"
    return "Tier 10: Phenomenon (Max 5,000+ hours badge reached!)"


async def start_streaming_session(state: dict[str, Any]) -> None:
    token = state.get("token", "").strip()
    room_input = state.get("room_input", "").strip()

    if not token:
        print("[!] Please set your main account token first.")
        return
    if not room_input:
        print("[!] Please set a target Channel ID or channel URL first.")
        return

    print("\n[*] Validating token and resolving channel details...")
    try:
        guild_id, channel_id, channel_name = parse_room_input(room_input, token)
    except ValueError as err:
        print(f"[!] Error: {err}")
        return

    print(
        f"[+] Target Channel: {channel_name} | Channel ID: {channel_id} | Guild ID: {guild_id or 'Private/DM'}"
    )

    streamer = DiscordStreamClient(
        token=token,
        guild_id=guild_id,
        channel_id=channel_id,
        role="streamer",
        self_mute=bool(state.get("self_mute", True)),
        self_deaf=bool(state.get("self_deaf", True)),
        self_video=bool(state.get("self_video", False)),
    )

    viewer_token = state.get("viewer_token", "").strip()
    viewer: Optional[DiscordStreamClient] = None
    if viewer_token:
        viewer = DiscordStreamClient(
            token=viewer_token,
            guild_id=guild_id,
            channel_id=channel_id,
            role="viewer",
            self_mute=True,
            self_deaf=True,
            self_video=False,
        )

    tasks = [asyncio.create_task(streamer.run_forever())]
    if viewer:
        tasks.append(asyncio.create_task(viewer.run_forever()))

    session_start = time.time()
    prior_seconds = float(state.get("total_streamed_seconds", 0))

    try:
        # If a viewer account is configured, wait for stream creation and watch it
        if viewer:
            try:
                await asyncio.wait_for(streamer.stream_created_event.wait(), timeout=15.0)
                await asyncio.wait_for(viewer.ready_event.wait(), timeout=15.0)
                await asyncio.sleep(1.5)
                if streamer.stream_key:
                    await viewer.watch_stream(streamer.stream_key)
            except asyncio.TimeoutError:
                pass

        print("\n" + "=" * 68)
        print(" [LIVE] Screen Share is running! Keep this window open to farm hours.")
        print(f" [CREDITS] By Discord: {AUTHOR_DISCORD} | Server: {SERVER_INVITE}")
        print(" [INFO] Press Ctrl+C at any time to stop the stream and leave safely.")
        print("=" * 68)

        while True:
            await asyncio.sleep(5)
            elapsed = time.time() - session_start
            total_sec = prior_seconds + elapsed
            total_hours = total_sec / 3600.0
            state["total_streamed_seconds"] = int(total_sec)
            save_state(state)

            status_label = "LIVE" if streamer.stream_active else "CONNECTING"
            next_tier = get_next_badge_milestone(total_hours)
            sys.stdout.write(
                f"\r[{status_label} | By {AUTHOR_DISCORD}] Session: {format_duration(elapsed)} | "
                f"Total: {total_hours:.2f}h | Next: {next_tier}   "
            )
            sys.stdout.flush()

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n\n[*] Stopping Screen Share and leaving voice channel...")
    finally:
        await streamer.stop()
        if viewer:
            await viewer.stop()
        for t in tasks:
            t.cancel()
        save_state(state)
        print("[+] Disconnected cleanly. Progress saved to stream_state.json.")


def interactive_menu() -> None:
    state = load_state()

    print(CREDITS_BANNER)

    if not state.get("token"):
        state["token"] = input("Enter your Discord Account Token: ").strip()
        save_state(state)

    if not state.get("room_input"):
        state["room_input"] = input("Enter Voice Channel ID or Channel URL: ").strip()
        save_state(state)

    while True:
        masked_tok = (
            state["token"][:10] + "..." + state["token"][-4:]
            if len(state.get("token", "")) > 15
            else "Not set"
        )
        viewer_status = "Enabled" if state.get("viewer_token") else "None (Optional)"
        total_hrs = float(state.get("total_streamed_seconds", 0)) / 3600.0

        print("\n" + "-" * 68)
        print(f" * Developer       : Discord @{AUTHOR_DISCORD} | {SERVER_INVITE}")
        print(f" * Main Token      : {masked_tok}")
        print(f" * Target Room     : {state.get('room_input') or 'Not set'}")
        print(f" * Viewer Account  : {viewer_status}")
        print(
            f" * Voice Settings  : Mute={state.get('self_mute', True)} | "
            f"Deaf={state.get('self_deaf', True)} | Video={state.get('self_video', False)}"
        )
        print(f" * Logged Hours    : {total_hrs:.2f}h ({get_next_badge_milestone(total_hrs)})")
        print("-" * 68)
        print(" [1] Start Voice Join & Screen Share (Go Live)")
        print(" [2] Change Target Room ID or Channel URL")
        print(" [3] Change Main Account Token")
        print(" [4] Set/Clear Optional Secondary Viewer Token")
        print(" [5] Configure Voice State (Mute / Deafen / Camera)")
        print(" [6] View All 10 Streaming Badge Tiers")
        print(" [7] Exit")

        choice = input("\nSelect an option (1-7): ").strip()

        if choice == "1":
            try:
                asyncio.run(start_streaming_session(state))
            except KeyboardInterrupt:
                pass

        elif choice == "2":
            new_room = input(
                "Enter Voice Channel ID or full URL (https://discord.com/channels/...): "
            ).strip()
            if new_room:
                state["room_input"] = new_room
                save_state(state)
                print("[+] Target room updated.")

        elif choice == "3":
            new_tok = input("Enter new Main Account Token: ").strip()
            if new_tok:
                state["token"] = new_tok
                save_state(state)
                print("[+] Main token updated.")

        elif choice == "4":
            print(
                "\n[i] Note: Discord's Streaming Badge requires streaming to at least one other"
                "\n    user in the voice channel. You can provide a second account token here so"
                "\n    it automatically joins the same room and watches your stream."
            )
            v_tok = input("Enter Viewer Account Token (or press Enter to clear): ").strip()
            state["viewer_token"] = v_tok
            save_state(state)
            print("[+] Viewer token settings saved.")

        elif choice == "5":
            m = input("Self-Mute microphone? [y/n] (default: y): ").strip().lower()
            d = input("Self-Deafen audio? [y/n] (default: y): ").strip().lower()
            v = input("Enable virtual camera state? [y/n] (default: n): ").strip().lower()
            state["self_mute"] = m != "n"
            state["self_deaf"] = d != "n"
            state["self_video"] = v == "y"
            save_state(state)
            print("[+] Voice and video settings saved.")

        elif choice == "6":
            print("\nStreaming Progression Badge Tiers:")
            for hrs, name in STREAM_BADGE_TIERS:
                mark = "[x]" if total_hrs >= hrs else "[ ]"
                print(f"  {mark} {name}")

        elif choice == "7":
            print(f"Goodbye! Join our Discord: {SERVER_INVITE} (By {AUTHOR_DISCORD})")
            break
        else:
            print("[!] Invalid choice. Please enter a number from 1 to 7.")


if __name__ == "__main__":
    try:
        interactive_menu()
    except KeyboardInterrupt:
        print(f"\nGoodbye! Join our Discord: {SERVER_INVITE} (By {AUTHOR_DISCORD})")