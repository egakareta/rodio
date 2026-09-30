use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "crossbeam-channel")]
use crossbeam_channel::{Receiver, Sender};
use dasp_sample::FromSample;
#[cfg(not(feature = "crossbeam-channel"))]
use std::sync::mpsc::{Receiver, Sender};

use crate::mixer::Mixer;
use crate::playback_controls::{AtomicOption, AtomicPosition, AtomicVolume};
use crate::source::SeekError;
use crate::Float;
use crate::{queue, source::Done, Source};

/// Handle to a device that outputs sounds.
///
/// Dropping the `Player` stops all its sounds. You can use `detach` if you want the sounds to continue
/// playing.
pub struct Player {
    queue_tx: Arc<queue::SourcesQueueInput>,
    sleep_until_end: AtomicOption<Receiver<()>>,

    controls: Arc<Controls>,
    sound_count: Arc<AtomicUsize>,

    detached: bool,
}

struct SeekOrder {
    pos: Duration,
    feedback: Sender<Result<(), SeekError>>,
}

impl SeekOrder {
    fn new(pos: Duration) -> (Self, Receiver<Result<(), SeekError>>) {
        #[cfg(not(feature = "crossbeam-channel"))]
        let (tx, rx) = {
            use std::sync::mpsc;
            mpsc::channel()
        };

        #[cfg(feature = "crossbeam-channel")]
        let (tx, rx) = {
            use crossbeam_channel::bounded;
            bounded(1)
        };
        (Self { pos, feedback: tx }, rx)
    }

    fn attempt<S>(self, maybe_seekable: &mut S, position: &AtomicPosition)
    where
        S: Source,
    {
        let res = maybe_seekable.try_seek(self.pos);
        if res.is_ok() {
            // Keep the sample generator as the sole position writer. Publish before
            // acknowledging the seek so get_pos observes it when try_seek returns.
            position.store(self.pos);
        }
        let _ignore_receiver_dropped = self.feedback.send(res);
    }
}

struct Controls {
    pause: AtomicBool,
    volume: AtomicVolume,
    stopped: AtomicBool,
    speed: AtomicU32,
    to_clear: AtomicU32,
    seek: AtomicOption<SeekOrder>,
    position: AtomicPosition,
}

impl Player {
    /// Builds a new `Player`, beginning playback on a stream.
    #[inline]
    pub fn connect_new(mixer: &Mixer) -> Player {
        let (sink, source) = Player::new();
        mixer.add(source);
        sink
    }

    /// Builds a new `Player`.
    #[inline]
    pub fn new() -> (Player, queue::SourcesQueueOutput) {
        let (queue_tx, queue_rx) = queue::queue(true);

        let sink = Player {
            queue_tx,
            sleep_until_end: AtomicOption::new(),
            controls: Arc::new(Controls {
                pause: AtomicBool::new(false),
                volume: AtomicVolume::new(1.0),
                stopped: AtomicBool::new(false),
                speed: AtomicU32::new(1.0f32.to_bits()),
                to_clear: AtomicU32::new(0),
                seek: AtomicOption::new(),
                position: AtomicPosition::new(Duration::ZERO),
            }),
            sound_count: Arc::new(AtomicUsize::new(0)),
            detached: false,
        };
        (sink, queue_rx)
    }

    /// Appends a sound to the queue of sounds to play.
    #[inline]
    pub fn append<S>(&self, source: S)
    where
        S: Source + Send + 'static,
        f32: FromSample<S::Item>,
    {
        // Wait for the queue to flush then resume stopped playback
        if self.controls.stopped.load(Ordering::SeqCst) {
            if self.sound_count.load(Ordering::SeqCst) > 0 {
                self.sleep_until_end();
            }
            self.controls.stopped.store(false, Ordering::SeqCst);
        }

        let controls = self.controls.clone();

        let start_played = AtomicBool::new(false);
        let sound_count_clone = self.sound_count.clone();

        let source = Done::new(
            source
                .speed(1.0)
                // Must be placed before pausable but after speed & delay
                .track_position()
                .pausable(false)
                .amplify(1.0)
                .skippable()
                .stoppable(),
            move |src| {
                if !src.inner().skipped() {
                    sound_count_clone.fetch_sub(1, Ordering::Relaxed);
                }
            },
        )
        // If you change the duration update the docs for try_seek!
        .periodic_access(Duration::from_millis(5), move |src| {
            if controls.stopped.load(Ordering::SeqCst) {
                src.inner_mut().stop();
                controls.position.store(Duration::ZERO);
            }
            if controls
                .to_clear
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |pending| {
                    pending.checked_sub(1)
                })
                .is_ok()
            {
                src.inner_mut().inner_mut().skip();
                controls.position.store(Duration::ZERO);
            } else {
                controls
                    .position
                    .store(src.inner().inner().inner().inner().inner().get_pos());
            }
            let amp = src.inner_mut().inner_mut().inner_mut();
            amp.set_factor(controls.volume.load());
            amp.inner_mut()
                .set_paused(controls.pause.load(Ordering::SeqCst));
            amp.inner_mut()
                .inner_mut()
                .inner_mut()
                .set_factor(f32::from_bits(controls.speed.load(Ordering::Relaxed)));
            if let Some(seek) = controls.seek.take() {
                seek.attempt(amp, &controls.position)
            }
            start_played.store(true, Ordering::SeqCst);
        });

        self.sound_count.fetch_add(1, Ordering::Relaxed);
        self.sleep_until_end
            .replace(self.queue_tx.append_with_signal(source));
    }

    /// Gets the volume of the sound.
    ///
    /// The value `1.0` is the "normal" volume (unfiltered input). Any value other than 1.0 will
    /// multiply each sample by this value.
    #[inline]
    pub fn volume(&self) -> Float {
        self.controls.volume.load()
    }

    /// Changes the volume of the sound.
    ///
    /// The value `1.0` is the "normal" volume (unfiltered input). Any value other than `1.0` will
    /// multiply each sample by this value.
    #[inline]
    pub fn set_volume(&self, value: Float) {
        self.controls.volume.store(value);
    }

    /// Gets the speed of the sound.
    ///
    /// See [`Player::set_speed`] for details on what *speed* means.
    #[inline]
    pub fn speed(&self) -> f32 {
        f32::from_bits(self.controls.speed.load(Ordering::Relaxed))
    }

    /// Changes the play speed of the sound. Does not adjust the samples, only the playback speed.
    ///
    /// # Note:
    /// 1. **Increasing the speed will increase the pitch by the same factor**
    /// - If you set the speed to 0.5 this will halve the frequency of the sound
    ///   lowering its pitch.
    /// - If you set the speed to 2 the frequency will double raising the
    ///   pitch of the sound.
    /// 2. **Change in the speed affect the total duration inversely**
    /// - If you set the speed to 0.5, the total duration will be twice as long.
    /// - If you set the speed to 2 the total duration will be halve of what it
    ///   was.
    ///
    #[inline]
    pub fn set_speed(&self, value: f32) {
        self.controls
            .speed
            .store(value.to_bits(), Ordering::Relaxed);
    }

    /// Resumes playback of a paused player.
    ///
    /// No effect if not paused.
    #[inline]
    pub fn play(&self) {
        self.controls.pause.store(false, Ordering::SeqCst);
    }

    // There is no `can_seek()` method as it is impossible to use correctly. Between
    // checking if a source supports seeking and actually seeking the sink can
    // switch to a new source.

    /// Attempts to seek to a given position in the current source.
    ///
    /// This blocks between 0 and ~5 milliseconds.
    ///
    /// As long as the duration of the source is known, seek is guaranteed to saturate
    /// at the end of the source. For example given a source that reports a total duration
    /// of 42 seconds calling `try_seek()` with 60 seconds as argument will seek to
    /// 42 seconds.
    ///
    /// # Errors
    /// This function will return [`SeekError::NotSupported`] if one of the underlying
    /// sources does not support seeking.
    ///
    /// It will return an error if an implementation ran
    /// into one during the seek.
    ///
    /// When seeking beyond the end of a source this
    /// function might return an error if the duration of the source is not known.
    pub fn try_seek(&self, pos: Duration) -> Result<(), SeekError> {
        let (order, feedback) = SeekOrder::new(pos);
        self.controls.seek.replace(order);

        if self.sound_count.load(Ordering::Acquire) == 0 {
            // No sound is playing, seek will not be performed
            return Ok(());
        }

        match feedback.recv() {
            Ok(seek_res) => seek_res,
            // The feedback channel closed. Probably another SeekOrder was set
            // invalidating this one and closing the feedback channel
            // ... or the audio thread panicked.
            Err(_) => Ok(()),
        }
    }

    /// Pauses playback of this player.
    ///
    /// No effect if already paused.
    ///
    /// A paused sink can be resumed with `play()`.
    pub fn pause(&self) {
        self.controls.pause.store(true, Ordering::SeqCst);
    }

    /// Gets if a sink is paused
    ///
    /// Players can be paused and resumed using `pause()` and `play()`. This returns `true` if the
    /// sink is paused.
    pub fn is_paused(&self) -> bool {
        self.controls.pause.load(Ordering::SeqCst)
    }

    /// Removes all currently loaded `Source`s from the `Player`, and pauses it.
    ///
    /// See `pause()` for information about pausing a `Player`.
    pub fn clear(&self) {
        let len = self.sound_count.load(Ordering::SeqCst) as u32;
        self.controls.to_clear.store(len, Ordering::SeqCst);
        self.sound_count.store(0, Ordering::Relaxed);
        self.pause();
    }

    /// Skips to the next `Source` in the `Player`
    ///
    /// If there are more `Source`s appended to the `Player` at the time,
    /// it will play the next one. Otherwise, the `Player` will finish as if
    /// it had finished playing a `Source` all the way through.
    pub fn skip_one(&self) {
        let len = self.sound_count.load(Ordering::SeqCst) as u32;
        if self
            .controls
            .to_clear
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |pending| {
                (len > pending).then(|| pending + 1)
            })
            .is_ok()
        {
            let _ = self
                .sound_count
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                    count.checked_sub(1)
                });
        }
    }

    /// Stops the sink by emptying the queue.
    #[inline]
    pub fn stop(&self) {
        self.controls.stopped.store(true, Ordering::SeqCst);
    }

    /// Destroys the sink without stopping the sounds that are still playing.
    #[inline]
    pub fn detach(mut self) {
        self.detached = true;
    }

    /// Sleeps the current thread until the sound ends.
    #[inline]
    pub fn sleep_until_end(&self) {
        if let Some(sleep_until_end) = self.sleep_until_end.take() {
            let _ = sleep_until_end.recv();
        }
    }

    /// Returns true if this sink has no more sounds to play.
    #[inline]
    pub fn empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of sounds currently in the queue.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.sound_count.load(Ordering::Relaxed)
    }

    /// Returns the position of the sound that's being played.
    ///
    /// This takes into account any speedup or delay applied.
    ///
    /// Example: if you apply a speedup of *2* to an mp3 decoder source and
    /// [`get_pos()`](Player::get_pos) returns *5s* then the position in the mp3
    /// recording is *10s* from its start.
    #[inline]
    pub fn get_pos(&self) -> Duration {
        self.controls.position.load()
    }
}

impl Drop for Player {
    #[inline]
    fn drop(&mut self) {
        self.queue_tx.set_keep_alive_if_empty(false);

        if !self.detached {
            self.controls.stopped.store(true, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::buffer::SamplesBuffer;
    use crate::math::nz;
    use crate::{Player, Source};

    #[test]
    fn test_immediate_length_changes() {
        let (player, mut source) = Player::new();

        player.skip_one();
        assert!(player.empty());

        player.append(SamplesBuffer::new(nz!(1), nz!(1), vec![2.0, 3.0]));
        player.append(SamplesBuffer::new(nz!(1), nz!(1), vec![1.0, 0.5]));
        assert_eq!(player.len(), 2);
        assert_eq!(source.next(), Some(2.0));

        player.skip_one();
        assert_eq!(player.len(), 1);
        assert_eq!(source.next(), Some(1.0));

        player.clear();
        assert_eq!(player.len(), 0);
        player.skip_one();
        assert!(player.empty());
    }

    #[test]
    fn test_pause_and_stop() {
        let (player, mut source) = Player::new();

        assert_eq!(source.next(), Some(0.0));
        // TODO (review) How did this test passed before? I might have broken something but
        //      silence source should come first as next source is only polled while previous ends.
        //      Respective test in Queue seem to be ignored (see queue::test::no_delay_when_added()
        //      at src/queue.rs:293).
        let mut source = source.skip_while(|x| *x == 0.0);

        let v = vec![10.0, -10.0, 20.0, -20.0, 30.0, -30.0];

        // Low rate to ensure immediate control.
        player.append(SamplesBuffer::new(nz!(1), nz!(1), v.clone()));
        let mut reference_src = SamplesBuffer::new(nz!(1), nz!(1), v);

        assert_eq!(source.next(), reference_src.next());
        assert_eq!(source.next(), reference_src.next());

        player.pause();

        assert_eq!(source.next(), Some(0.0));

        player.play();

        assert_eq!(source.next(), reference_src.next());
        assert_eq!(source.next(), reference_src.next());

        player.stop();

        assert_eq!(source.next(), Some(0.0));

        assert!(player.empty());
    }

    #[test]
    fn test_stop_and_start() {
        let (player, mut queue_rx) = Player::new();

        let v = vec![10.0, -10.0, 20.0, -20.0, 30.0, -30.0];

        player.append(SamplesBuffer::new(nz!(1), nz!(1), v.clone()));
        let mut src = SamplesBuffer::new(nz!(1), nz!(1), v.clone());

        assert_eq!(queue_rx.next(), src.next());
        assert_eq!(queue_rx.next(), src.next());

        player.stop();

        assert!(player.controls.stopped.load(Ordering::SeqCst));
        assert_eq!(queue_rx.next(), Some(0.0));

        src = SamplesBuffer::new(nz!(1), nz!(1), v.clone());
        player.append(SamplesBuffer::new(nz!(1), nz!(1), v));

        assert!(!player.controls.stopped.load(Ordering::SeqCst));
        // Flush silence
        let mut queue_rx = queue_rx.skip_while(|v| *v == 0.0);

        assert_eq!(queue_rx.next(), src.next());
        assert_eq!(queue_rx.next(), src.next());
    }

    #[test]
    fn test_volume() {
        let (player, mut queue_rx) = Player::new();

        let v = vec![10.0, -10.0, 20.0, -20.0, 30.0, -30.0];

        // High rate to avoid immediate control.
        player.append(SamplesBuffer::new(nz!(2), nz!(44100), v.clone()));
        let src = SamplesBuffer::new(nz!(2), nz!(44100), v.clone());

        let mut src = src.amplify(0.5);
        player.set_volume(0.5);

        for _ in 0..v.len() {
            assert_eq!(queue_rx.next(), src.next());
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn playback_advances_while_workers_query_and_update_controls() {
        use std::sync::Barrier;

        const WORKERS: usize = 16;
        const SAMPLES: usize = 192_000;
        let (player, mut output) = Player::new();
        player.append(SamplesBuffer::new(nz!(1), nz!(24_000), vec![1.0; SAMPLES]));
        let started = Barrier::new(WORKERS + 1);

        std::thread::scope(|scope| {
            for worker in 0..WORKERS {
                let player = &player;
                let started = &started;
                scope.spawn(move || {
                    started.wait();
                    let mut previous_position = std::time::Duration::ZERO;
                    for _ in 0..10_000 {
                        player.set_volume((worker % 4 + 1) as crate::Float / 4.0);
                        player.set_speed(1.0);
                        let position = player.get_pos();
                        assert!(position >= previous_position);
                        assert!(position <= std::time::Duration::from_secs(8));
                        assert!((0.25..=1.0).contains(&player.volume()));
                        assert_eq!(player.speed(), 1.0);
                        previous_position = position;
                    }
                });
            }
            started.wait();
            for _ in 0..SAMPLES {
                let sample = output.next().expect("the playing queue stays alive");
                assert!(sample.is_finite());
                assert!((0.0..=1.0).contains(&sample));
            }
        });

        assert!(player.get_pos() > std::time::Duration::from_secs(7));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn paused_seek_reports_the_position_when_acknowledged() {
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let (player, mut output) = Player::new();
        player.append(SamplesBuffer::new(nz!(1), nz!(48_000), vec![1.0; 96_000]));
        player.pause();
        let player = Arc::new(player);
        let target = Duration::from_millis(1_250);
        let seeking_player = Arc::clone(&player);
        let seeking = std::thread::spawn(move || seeking_player.try_seek(target));

        let deadline = Instant::now() + Duration::from_secs(5);
        while !seeking.is_finished() && Instant::now() < deadline {
            assert_eq!(output.next(), Some(0.0));
        }
        assert!(
            seeking.is_finished(),
            "seek should complete while rendering"
        );
        seeking.join().unwrap().unwrap();
        assert_eq!(player.get_pos(), target);
        assert!(player.is_paused());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn failed_seek_preserves_playback_position() {
        use std::time::{Duration, Instant};

        let (player, mut output) = Player::new();
        // Buffered sources expose playback but deliberately do not support seeking.
        player.append(SamplesBuffer::new(nz!(1), nz!(48_000), vec![1.0; 96_000]).buffered());
        for _ in 0..4_800 {
            output.next().unwrap();
        }
        player.pause();
        for _ in 0..480 {
            output.next().unwrap();
        }
        let before = player.get_pos();
        assert!(!before.is_zero());

        std::thread::scope(|scope| {
            let seeking = scope.spawn(|| player.try_seek(Duration::from_millis(1_250)));
            let deadline = Instant::now() + Duration::from_secs(5);
            while !seeking.is_finished() && Instant::now() < deadline {
                assert_eq!(output.next(), Some(0.0));
            }
            assert!(
                seeking.is_finished(),
                "seek should complete while rendering"
            );
            assert!(seeking.join().unwrap().is_err());
        });

        assert_eq!(player.get_pos(), before);
        assert!(player.is_paused());
    }
}
