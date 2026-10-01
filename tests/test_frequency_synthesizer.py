"""Testes do sintetizador de frequência.

O que importa é a sintonia que sai para o receptor: central + Doppler, sem
voltar à nominal quando o Station Manager reenvia a frequência, e sem morrer
com uma mensagem estragada. O último teste fala ZMQ de verdade, nas duas
pontas, porque o contrato com a estação é o fio, não a classe.
"""

import pathlib
import sys
import threading
import time

import pytest
import zmq

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from frequency_synthesizer import FrequencySynthesizer, main  # noqa: E402


@pytest.fixture
def synth():
    s = FrequencySynthesizer("inproc://tune-test", "inproc://manager-test")
    yield s
    s.close()


def test_tune_e_central_mais_doppler(synth):
    assert synth.handle([b"freq", b"145900000"]) == 145_900_000
    assert synth.handle([b"doppler", b"3282"]) == 145_903_282
    assert synth.handle([b"doppler", b"-1500"]) == 145_898_500


def test_doppler_antes_da_frequencia_e_descartado(synth, capsys):
    """Religado no meio da passagem: até 30 `doppler` chegam antes do `freq`
    repetido. Descarta todos, avisando uma vez — e retoma no `freq`."""
    for _ in range(30):
        assert synth.handle([b"doppler", b"3282"]) is None
    assert synth.tunes_sent == 0
    assert capsys.readouterr().out.count("doppler antes de freq") == 1

    synth.handle([b"freq", b"145900000"])
    assert synth.handle([b"doppler", b"3282"]) == 145_903_282


def test_freq_repetido_nao_zera_o_doppler(synth):
    """O defeito do upstream: o Station Manager reenvia a nominal a cada 30
    anúncios, e zerar o Doppler ali devolvia o receptor à nominal por ~1 s —
    com 3 kHz de desvio, um buraco periódico na recepção."""
    synth.handle([b"freq", b"145900000"])
    synth.handle([b"doppler", b"3282"])

    assert synth.handle([b"freq", b"145900000"]) == 145_903_282


def test_frequencia_nova_zera_o_doppler(synth):
    """Outra passagem, outro satélite: o Doppler da anterior não vale mais."""
    synth.handle([b"freq", b"145900000"])
    synth.handle([b"doppler", b"3282"])

    assert synth.handle([b"freq", b"437200000"]) == 437_200_000


@pytest.mark.parametrize("frames", [
    [b"doppler"],
    [b"doppler", b"3282", b"extra"],
    [b"doppler", b"lixo"],
    [b"\xff\xfe", b"1"],
    [b"freq", b"-5"],
    [b"freq", b"0"],
])
def test_mensagem_estragada_e_descartada_sem_derrubar(synth, frames):
    synth.handle([b"freq", b"145900000"])
    synth.handle([b"doppler", b"100"])

    assert synth.handle(frames) is None
    assert (synth.center_freq, synth.doppler_offset) == (145_900_000, 100)


def test_topico_desconhecido_e_ignorado(synth):
    synth.handle([b"freq", b"145900000"])

    assert synth.handle([b"tune", b"1"]) is None


def test_valor_fracionario_e_arredondado(synth):
    synth.handle([b"freq", b"145900000.4"])

    assert synth.handle([b"doppler", b"-2345.6"]) == 145_897_654


def test_ida_e_volta_por_zmq():
    """Station Manager falso -> sintetizador -> receptor falso, por TCP, com
    o sintetizador rodando na própria thread e parando pelo stop()."""
    context = zmq.Context.instance()
    manager = context.socket(zmq.PUB)
    manager_port = manager.bind_to_random_port("tcp://127.0.0.1")

    probe = context.socket(zmq.REP)
    tune_port = probe.bind_to_random_port("tcp://127.0.0.1")
    probe.close()  # só para achar uma porta livre

    synth = FrequencySynthesizer(f"tcp://127.0.0.1:{tune_port}",
                                 f"tcp://127.0.0.1:{manager_port}")
    receiver = context.socket(zmq.SUB)
    receiver.setsockopt(zmq.SUBSCRIBE, b"tune")
    receiver.setsockopt(zmq.RCVTIMEO, 2000)
    receiver.connect(f"tcp://127.0.0.1:{tune_port}")

    thread = threading.Thread(target=synth.run, daemon=True)
    thread.start()
    time.sleep(0.5)  # PUB descarta o que sai antes de o SUB conectar

    try:
        manager.send_multipart([b"freq", b"145900000"])
        assert receiver.recv_multipart() == [b"tune", b"145900000"]
        manager.send_multipart([b"doppler", b"3282"])
        assert receiver.recv_multipart() == [b"tune", b"145903282"]
    finally:
        synth.stop()
        thread.join(timeout=2)
        receiver.close()
        manager.close()

    assert not thread.is_alive()


def test_main_aceita_os_enderecos(monkeypatch):
    seen = {}

    class Fake:
        tunes_sent = 0

        def __init__(self, bind, manager, channel=None):
            seen.update(bind=bind, manager=manager, channel=channel)

        def run(self):
            pass

        def stop(self):
            pass

    monkeypatch.setattr(sys.modules["frequency_synthesizer"], "FrequencySynthesizer", Fake)

    assert main(["--bind", "tcp://*:6000", "--station-manager", "tcp://sm:5581",
                 "--channel", "uhf"]) == 0
    assert seen == {"bind": "tcp://*:6000", "manager": "tcp://sm:5581", "channel": "uhf"}


# --- canais: um sintetizador por rádio ---------------------------------------------


def test_com_canal_so_os_topicos_do_seu_radio_valem():
    s = FrequencySynthesizer("inproc://tune-uhf", "inproc://manager-uhf", channel="uhf")
    try:
        assert s.handle([b"freq.vhf", b"145900000"]) is None
        assert s.handle([b"freq.uhf", b"468400000"]) == 468_400_000
        assert s.handle([b"doppler.vhf", b"1000"]) is None
        assert s.handle([b"doppler.uhf", b"3210"]) == 468_403_210
    finally:
        s.close()


def test_sem_canal_ignora_os_topicos_com_canal(synth):
    """O SUB do ZMQ filtra por prefixo: "freq" também recebe "freq.vhf". Sem
    comparar o tópico inteiro, o sintetizador de canal único sintonizaria o
    receptor na frequência do outro rádio."""
    assert synth.handle([b"freq.uhf", b"468400000"]) is None
    assert synth.handle([b"freq", b"145900000"]) == 145_900_000
    assert synth.handle([b"doppler.uhf", b"3210"]) is None
    assert synth.center_freq == 145_900_000
