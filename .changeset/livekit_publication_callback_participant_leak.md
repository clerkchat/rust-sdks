---
livekit: patch
livekit-capture: patch
livekit-ffi: patch
---

Fix a leak of remote participants and their tracks when a room closes while they are still in it, caused by publication callbacks holding strong references to their own participant
