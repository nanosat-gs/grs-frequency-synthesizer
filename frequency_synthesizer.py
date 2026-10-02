"""GRS Frequency Synthesizer.

Fica entre o Station Manager e o IQ Receiver: assina a frequência central
(`freq`) e o desvio Doppler (`doppler`) que o Station Manager anuncia, e
publica a sintonia efetiva (`tune` = central + Doppler) para o receptor.

    SUB  <station manager>   [b"freq", b"<Hz>"]  [b"doppler", b"<Hz>"]
                             [b"offset", b"<Hz>"]   (opcional)
    PUB  <bind>              [b"tune", b"<Hz>"]

`tune` = central + Doppler + desvio. O `offset` é o ajuste fino medido pelo
bloco FFT do rádio (o erro do oscilador do satélite, que o Doppler previsto
não vê), já integrado pelo Station Manager. Sem ele, vale zero — e o
sintetizador se comporta exatamente como antes.

Com mais de um rádio na estação (um por faixa), o Station Manager anuncia
cada um no seu canal — `freq.vhf`, `doppler.vhf`, `freq.uhf`... — e roda um
sintetizador por rádio, com `--channel`. O `tune` que sai continua sem canal:
cada sintetizador tem a sua porta, e o receptor do rádio assina só ela.

Roda como processo próprio (`python frequency_synthesizer.py`): no diagrama da
estação ele mora no Station Server, junto do receptor, e o Station Manager no
Control Server — em máquinas diferentes, falando ZMQ pela rede.

O que mudou em relação ao upstream (spacelab-ufsc), e por quê:

- `freq` repetido NÃO zera o Doppler. O Station Manager reenvia a nominal de
  tempos em tempos, para quem conectou no meio da passagem. Zerar o Doppler a
  cada reenvio mandava o receptor de volta à nominal por ~1 s (até o próximo
  `doppler`) — com 3 kHz de desvio, um buraco periódico na recepção. Só uma
  frequência NOVA (outra passagem, outro satélite) zera o Doppler.
- Mensagem malformada é descartada com aviso, em vez de derrubar o laço: no
  upstream só `zmq.Again` era tratado, e um valor ilegível ou um número
  errado de partes encerrava o processo com exceção.
- Ponto de entrada com os endereços por argumento, e encerramento limpo no
  SIGTERM (`docker stop`).

Sem anúncios, nada é publicado: o receptor fica na última sintonia. É o
comportamento certo quando o Station Manager cai no meio de uma passagem — e
o motivo de o Station Manager reenviar a nominal: um sintetizador religado no
meio da passagem descarta os `doppler` até o próximo `freq`, e então retoma.
"""

import argparse
import signal
import sys
import time

import zmq

DEFAULT_BIND = "tcp://*:5557"
DEFAULT_STATION_MANAGER = "tcp://localhost:5581"

# O Doppler chega a cada segundo; logar cada um afogaria o log. Um resumo a
# cada tantos segundos basta para ver a correção andando.
LOG_EVERY_S = 10.0


class FrequencySynthesizer:
    def __init__(self, iq_receiver_socket_address, station_manager_pub_address, channel=None):
        self.channel = channel or None
        suffix = f".{self.channel}" if self.channel else ""
        self.freq_topic = "freq" + suffix
        self.doppler_topic = "doppler" + suffix
        self.offset_topic = "offset" + suffix

        self.center_freq = None
        self.doppler_offset = 0
        self.fine_offset = 0

        self.context = zmq.Context()

        self.cmd_socket = self.context.socket(zmq.PUB)
        self.cmd_socket.setsockopt(zmq.LINGER, 0)
        self.cmd_socket.bind(iq_receiver_socket_address)

        self.manager_socket = self.context.socket(zmq.SUB)
        self.manager_socket.setsockopt(zmq.LINGER, 0)
        # Timeout no recv em vez de NOBLOCK + sleep: o laço acorda para ver o
        # pedido de parada sem girar à toa.
        self.manager_socket.setsockopt(zmq.RCVTIMEO, 200)
        self.manager_socket.connect(station_manager_pub_address)
        # O SUB do ZMQ filtra por PREFIXO: sem canal, "freq" também recebe
        # "freq.vhf". Por isso handle() compara o tópico inteiro.
        self.manager_socket.setsockopt_string(zmq.SUBSCRIBE, self.freq_topic)
        self.manager_socket.setsockopt_string(zmq.SUBSCRIBE, self.doppler_topic)
        self.manager_socket.setsockopt_string(zmq.SUBSCRIBE, self.offset_topic)

        self.iq_receiver_address = iq_receiver_socket_address
        self.station_manager_address = station_manager_pub_address
        self.tunes_sent = 0
        self._running = False
        self._last_log = 0.0
        self._waiting_logged = False

    def _effective_frequency(self):
        return self.center_freq + self.doppler_offset + self.fine_offset

    def _send_tune(self):
        if self.center_freq is None:
            return None
        freq = self._effective_frequency()
        self.cmd_socket.send_multipart([b"tune", str(freq).encode()])
        self.tunes_sent += 1
        return freq

    def handle(self, frames):
        """Processa uma mensagem do Station Manager. Devolve a sintonia
        publicada, ou None se nada foi publicado."""
        if len(frames) != 2:
            print(f"[Synthesizer] mensagem com {len(frames)} partes descartada", flush=True)
            return None

        try:
            topic = frames[0].decode()
            value = int(round(float(frames[1].decode())))
        except (UnicodeDecodeError, ValueError):
            print(f"[Synthesizer] mensagem ilegível descartada: {frames!r}", flush=True)
            return None

        if topic == self.freq_topic:
            if value <= 0:
                print(f"[Synthesizer] freq inválida descartada: {value}", flush=True)
                return None
            if value != self.center_freq:
                print(f"[Synthesizer] frequência central {value} Hz", flush=True)
                self.center_freq = value
                self.doppler_offset = 0
                # O ajuste fino era do satélite anterior.
                self.fine_offset = 0
            return self._send_tune()

        if topic == self.doppler_topic:
            if self.center_freq is None:
                # Religado no meio de uma passagem: o Station Manager só
                # reenvia a nominal a cada 30 anúncios. Avisa uma vez, não 30.
                if not self._waiting_logged:
                    self._waiting_logged = True
                    print("[Synthesizer] doppler antes de freq: descartando até o Station "
                          "Manager reenviar a nominal (até ~30 s)", flush=True)
                return None
            self.doppler_offset = value
            tune = self._send_tune()
            now = time.monotonic()
            if now - self._last_log >= LOG_EVERY_S:
                self._last_log = now
                print(f"[Synthesizer] tune {tune} Hz (Doppler {value:+d} Hz)", flush=True)
            return tune

        if topic == self.offset_topic:
            if self.center_freq is None:
                return None
            if value != self.fine_offset:
                print(f"[Synthesizer] ajuste fino {value:+d} Hz", flush=True)
            self.fine_offset = value
            return self._send_tune()

        return None

    def run(self):
        print(f"[Synthesizer] Running: {self.station_manager_address} -> "
              f"{self.iq_receiver_address} (canal {self.channel or 'único'})", flush=True)
        self._running = True

        try:
            while self._running:
                try:
                    frames = self.manager_socket.recv_multipart()
                except zmq.Again:
                    continue
                except zmq.ZMQError:
                    if not self._running:
                        break
                    raise
                self.handle(frames)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def stop(self):
        """Pede a parada. O laço termina em até um timeout de recv e fecha
        os sockets na própria thread, que é a dona deles."""
        self._running = False

    def close(self):
        self.cmd_socket.close()
        self.manager_socket.close()
        self.context.term()


def main(argv=None):
    parser = argparse.ArgumentParser(description="GRS Frequency Synthesizer")
    parser.add_argument("--bind", default=DEFAULT_BIND,
                        help=f"Onde publicar o tune para o IQ Receiver (padrão {DEFAULT_BIND})")
    parser.add_argument("--station-manager", default=DEFAULT_STATION_MANAGER,
                        help="PUB de freq/doppler do Station Manager "
                             f"(padrão {DEFAULT_STATION_MANAGER})")
    parser.add_argument("--channel", default="",
                        help="Rádio deste sintetizador (vhf, uhf...): assina freq.<canal> e "
                             "doppler.<canal>. Vazio = os tópicos sem canal, de uma estação "
                             "com um rádio só.")
    args = parser.parse_args(argv)

    synth = FrequencySynthesizer(args.bind, args.station_manager, channel=args.channel)
    signal.signal(signal.SIGTERM, lambda *_: synth.stop())
    signal.signal(signal.SIGINT, lambda *_: synth.stop())
    synth.run()
    print(f"[Synthesizer] encerrado ({synth.tunes_sent} tunes publicados)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
