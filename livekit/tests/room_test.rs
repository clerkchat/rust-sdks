// Copyright 2025 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(feature = "__lk-e2e-test")]
use {
    anyhow::{anyhow, Ok, Result},
    chrono::{TimeDelta, TimeZone, Utc},
    common::test_rooms,
    libwebrtc::{
        audio_source::native::NativeAudioSource,
        prelude::{AudioSourceOptions, PeerConnectionState, RtcAudioSource},
    },
    livekit::{
        options::TrackPublishOptions,
        track::{LocalAudioTrack, LocalTrack},
        ConnectionState, ParticipantKind, RoomEvent,
    },
    std::time::Duration,
    tokio::time::{self, timeout},
};

mod common;

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_connect() -> Result<()> {
    let (room, _) = test_rooms(1).await?.pop().unwrap();

    assert_eq!(room.connection_state(), ConnectionState::Connected);
    assert!(room.name().starts_with("test_room_"));
    assert!(room.remote_participants().is_empty());

    let creation_time = Utc.timestamp_millis_opt(room.creation_time()).unwrap();
    assert!(creation_time.signed_duration_since(Utc::now()).abs() <= TimeDelta::seconds(10));

    let local_participant = room.local_participant();
    assert!(local_participant.sid().as_str().starts_with("PA_"));
    assert_eq!(local_participant.identity().as_str(), "p0");
    assert_eq!(local_participant.name(), "Participant 0");
    assert_eq!(local_participant.kind(), ParticipantKind::Standard);

    Ok(())
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_connect_multiple() -> Result<()> {
    let mut rooms = test_rooms(2).await?;

    let (second, _) = rooms.pop().unwrap();
    let (first, _) = rooms.pop().unwrap();

    assert_eq!(first.name(), second.name(), "Participants are in different rooms");

    assert!(second.remote_participants().get(&first.local_participant().identity()).is_some());
    assert!(first.remote_participants().get(&second.local_participant().identity()).is_some());

    Ok(())
}

#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_participant_disconnect() -> Result<()> {
    let mut rooms = test_rooms(2).await?;
    let (second, _) = rooms.pop().unwrap();
    let second_sid = second.local_participant().sid();
    let second_name = second.local_participant().name();

    let (_, mut first_event_rx) = rooms.pop().unwrap();

    tokio::spawn(async move {
        time::sleep(Duration::from_millis(400)).await;
        second.close().await?;
        Ok(())
    });

    let wait_for_disconnected = async move {
        while let Some(event) = first_event_rx.recv().await {
            let RoomEvent::ParticipantDisconnected(participant) = event else { continue };
            assert_eq!(participant.sid(), second_sid);
            assert_eq!(participant.name(), second_name);
            break;
        }
        Ok(())
    };
    timeout(Duration::from_secs(15), wait_for_disconnected).await??;
    Ok(())
}

/// A cancelled `close()` must still have closed the peer connections.
///
/// `SessionInner::close` ends with two unbounded awaits, so a caller bounding it with a
/// timeout used to drop the future before the transports were closed — and nothing else
/// closes them, so their ICE sockets stayed bound for the process's lifetime. Cancelling at
/// the first `.await` makes this deterministic: the assertion holds only if the close ran
/// before the future suspended.
#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_cancelled_close_still_closes_peer_connections() -> Result<()> {
    let (room, _) = test_rooms(1).await?.pop().unwrap();
    assert_eq!(
        room.publisher_connection_state(),
        PeerConnectionState::Connected,
        "publisher should be connected before teardown, or the assertion below proves nothing"
    );

    // Poll the close exactly once, then drop it. `Duration::ZERO` elapses on the first poll,
    // so `timeout` yields `Err` the moment the inner future suspends — cancelling it at the
    // earliest possible point rather than at some arbitrary later one.
    let cancelled = timeout(Duration::ZERO, room.close()).await;
    assert!(cancelled.is_err(), "close should have been cancelled at its first suspension point");

    assert_eq!(
        room.publisher_connection_state(),
        PeerConnectionState::Closed,
        "a cancelled close must not leave the publisher transport open: its ICE sockets are \
         only released by PeerConnection::close, so anything else leaks them permanently"
    );
    Ok(())
}

/// Closing and dropping a room must release the internal room session.
///
/// Stored callbacks owned by the room must not strongly capture the room session. Such a
/// reference cycle prevents the session, RTC engine, and WebRTC runtime from being destroyed.
#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_close_releases_room_session() -> Result<()> {
    let (room, events) = test_rooms(1).await?.pop().unwrap();
    let session_dropped = room.drop_probe();

    drop(events);
    room.close().await?;
    drop(room);

    assert!(session_dropped(), "room callbacks retained the room session after close");
    Ok(())
}

/// Closing and dropping a room must release remote participants still in it, and their
/// subscribed publications.
///
/// Callbacks a participant stores on its own publications, and a publication on its own track,
/// must not strongly capture their owner. Such a cycle is only broken when the participant leaves
/// while we are connected, so every participant still present at close would leak.
#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_close_releases_remote_participants() -> Result<()> {
    let mut rooms = test_rooms(2).await?;
    let (pub_room, _) = rooms.pop().unwrap();
    let (sub_room, mut sub_events) = rooms.pop().unwrap();

    let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1000);
    let track = LocalAudioTrack::create_audio_track("audio", RtcAudioSource::Native(source));
    pub_room
        .local_participant()
        .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
        .await?;

    // Subscribed, so the publication holds a track and both kinds of callback are registered.
    let (participant_dropped, publication_dropped) = timeout(Duration::from_secs(10), async {
        while let Some(event) = sub_events.recv().await {
            if let RoomEvent::TrackSubscribed { participant, publication, .. } = event {
                return Ok((participant.drop_probe(), publication.drop_probe()));
            }
        }
        Err(anyhow!("event channel closed before TrackSubscribed"))
    })
    .await??;

    let session_dropped = sub_room.drop_probe();
    sub_room.close().await?;
    // Queued events hold participant and publication clones.
    drop(sub_events);
    drop(sub_room);
    assert!(session_dropped(), "room session retained after close");

    let released = |probe: Box<dyn Fn() -> bool>| async move {
        timeout(Duration::from_secs(10), async {
            while !probe() {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    };
    assert!(
        released(Box::new(participant_dropped)).await,
        "remote participant retained after room close"
    );
    assert!(
        released(Box::new(publication_dropped)).await,
        "remote publication retained after room close"
    );

    // Still connected until now, so the participant was present when the other side closed.
    pub_room.close().await?;
    Ok(())
}

/// `close()` must leave the room Disconnected and report it, every time.
///
/// The room's event task used to be the only place that applied the engine's `Disconnected`
/// event, and it races that event against the close signal `close()` sends right after; when
/// both are ready the close can win and the event is dropped, leaving the room in its previous
/// state. Each iteration hits that race with roughly even odds.
#[cfg(feature = "__lk-e2e-test")]
#[test_log::test(tokio::test)]
async fn test_close_leaves_room_disconnected() -> Result<()> {
    for _ in 0..5 {
        let (room, mut events) = test_rooms(1).await?.pop().unwrap();
        room.close().await?;

        assert_eq!(room.connection_state(), ConnectionState::Disconnected);
        let mut disconnected = false;
        while let Some(event) = events.try_recv().ok() {
            disconnected |= matches!(
                event,
                RoomEvent::Disconnected { reason: livekit::DisconnectReason::ClientInitiated }
            );
        }
        assert!(disconnected, "close() did not emit RoomEvent::Disconnected");
    }
    Ok(())
}
