<h1 align="center">
    GRS Frequency Synthesizer
    <br>
</h1>

<h4 align="center">Frequency Synthesizer of the SpaceLab's Ground Station.</h4>

<p align="center">
    <a href="https://github.com/spacelab-ufsc/grs-frequency-synthesizer">
        <img src="https://img.shields.io/badge/status-development-green?style=for-the-badge">
    </a>
    <a href="https://github.com/spacelab-ufsc/grs-frequency-synthesizer/releases">
        <img alt="GitHub commits since latest release (by date)" src="https://img.shields.io/github/commits-since/spacelab-ufsc/grs-frequency-synthesizer/latest?style=for-the-badge">
    </a>
    <a href="https://github.com/spacelab-ufsc/grs-frequency-synthesizer/blob/main/LICENSE">
        <img src="https://img.shields.io/badge/license-GPL3-yellow?style=for-the-badge">
    </a>
</p>

<p align="center">
    <a href="#overview">Overview</a> •
    <a href="#dependencies">Dependencies</a> •
    <a href="#usage">Usage</a> •
    <a href="#documentation">Documentation</a> •
    <a href="#license">License</a>
</p>

## Overview

The GRS Frequency Synthesizer is an internal component of SpaceLab's Ground Station software pipeline. It sits between the Station Manager and the IQ Receiver, responsible for computing the effective tuning frequency at any given moment by combining the base center frequency with a Doppler correction offset, and forwarding the result to the IQ Receiver as a tune command.

The IQ Receiver is autonomous and does not depend on the Frequency Synthesizer to operate — it simply reacts to tune commands whenever they arrive. The Frequency Synthesizer is an optional layer that handles the Doppler correction math on top, as well as any changes in the center frequency.

## Dependencies

* [pyzmq](https://pypi.org/project/pyzmq/)

### Installation

```pip install pyzmq```

## Usage

Run it as its own process, next to the IQ Receiver:

```
pip install -r requirements.txt
python frequency_synthesizer.py --bind tcp://*:5557 --station-manager tcp://station-manager:5581
```

With more than one radio in the station (one per band), run one synthesizer per
radio, each listening to its own channel and publishing on its own port:

```
python frequency_synthesizer.py --channel vhf --bind tcp://*:5557 --station-manager tcp://station-manager:5581
python frequency_synthesizer.py --channel uhf --bind tcp://*:5557 --station-manager tcp://station-manager:5581
```

| Argument | Default | Description |
|---|---|---|
| `--bind` | `tcp://*:5557` | Where to publish `tune` (PUB) |
| `--station-manager` | `tcp://localhost:5581` | Station Manager tuning announcements (SUB) |
| `--channel` | empty | Radio channel to follow (`vhf`, `uhf`...). Empty = the unsuffixed topics |

In the SpaceLab ground station (`nanosat-gs/grs-station`) it runs as the
`grs-frequency-synthesizer` (VHF) and `grs-frequency-synthesizer-uhf` services
of the compose, built from the `station` branch of this fork.

The class can still be embedded:

```python
from frequency_synthesizer import FrequencySynthesizer

synth = FrequencySynthesizer(
    iq_receiver_socket_address="tcp://*:5557",
    station_manager_pub_address="tcp://localhost:5581",
    channel="vhf",   # optional
)
synth.run()      # blocks; call synth.stop() from another thread or a signal handler
```

### ZMQ Interface

Subscribed topics (from Station Manager). With `--channel vhf` each topic gets
the suffix: `freq.vhf`, `doppler.vhf`, `offset.vhf`.

| Topic     | Payload        | Description                                                             |
|-----------|----------------|-------------------------------------------------------------------------|
| `freq`    | Frequency (Hz) | Nominal carrier of the downlink; a NEW value starts a new pass          |
| `doppler` | Offset (Hz)    | Predicted Doppler shift at this instant                                 |
| `offset`  | Offset (Hz)    | Fine tuning (AFC): the satellite oscillator error measured by the FFT block and integrated by the Station Manager. Optional; defaults to 0 |

Published topics (to IQ Receiver):

| Topic  | Payload        | Description                                                             |
|--------|----------------|-------------------------------------------------------------------------|
| `tune` | Frequency (Hz) | Effective frequency (`freq + doppler + offset`) to tune the SDR to. Never suffixed: each synthesizer has its own port |

### Behavior

- On startup, `center_freq` is `None`. The synthesizer will not send any tune command until the Station Manager sends a `freq` command.
- If a `doppler` or `offset` command arrives before any `freq` command has been received, it is ignored (a single warning is printed for `doppler`). The Station Manager re-sends `freq` every 30 announcements, so a synthesizer restarted mid-pass recovers within ~30 s.
- A `freq` with a NEW value resets both the Doppler and the fine offset to 0: they belonged to the previous satellite.
- A `freq` with the SAME value only re-sends the current tune; it does not reset anything. The Station Manager repeats `freq` periodically for late subscribers, and resetting there would throw the receiver back to the nominal frequency for ~1 s every time.
- Malformed messages (wrong number of parts, unparsable value, non-positive `freq`) are dropped with a warning instead of stopping the process.
- With no announcements, nothing is published: the IQ Receiver keeps its last tune.
- The effective frequency sent to the IQ Receiver is always `center_freq + doppler_offset + fine_offset`.
- It is the Station Manager's responsibility to ensure the initial `center_freq` matches the frequency the IQ Receiver was started with.
- SIGTERM (`docker stop`) closes the sockets and exits cleanly.

> **Note:** The Frequency Synthesizer does not compute Doppler shifts or the fine offset itself. It expects the Station Manager to supply both, already computed.

### Tests

```
pip install pytest
pytest tests
```

### Changes in the `station` branch

This is the `nanosat-gs` fork. The `station` branch adds, on top of the upstream
`master`: the command-line entry point and clean shutdown; `freq` repeats that
no longer reset the Doppler; tolerance to malformed messages; `--channel`
(one synthesizer per radio); and the `offset` topic (fine tuning). The
rationale for each change is in the module docstring of
`frequency_synthesizer.py`.

## Documentation

The documentation of the upstream project is generated using the Sphinx tool, and it is available [here](https://spacelab-ufsc.github.io/grs-frequency-synthesizer/). It does not cover the changes in the `station` branch, and the Sphinx sources are not in this repository.

### Dependencies

* Sphinx
* sphinx-rtd-theme

### Building the Documentation

```make html```

## License

This project is licensed under GPLv3 license.
