import time
import adafruit_rfm9x
import busio
import board
import json
import zlib
import struct
import shutil
import os
from logger import configurar_logger
from digitalio import DigitalInOut, Direction, Pull
from datetime import datetime

# === Parâmetros para dividir o JSON em partes de 252 bytes ===
MAX_BYTES = 252
FLOAT_SIZE = 4  # Supondo uso de float32
seq = 0
idx = 0
partes = []
handshake = False
zero = False
final = False

# Inicialização
CS = DigitalInOut(board.CE1)
RESET = DigitalInOut(board.D25)
spi = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)
rfm9x = adafruit_rfm9x.RFM9x(spi, CS, RESET, 915.0)
rfm9x.tx_power = 23

seq_perdidas = []
seq_int = 0

COUNT_LAST_PKG = 0

logger = configurar_logger("Emissor")


# ==== Procesa o pacote perdido====
def processa_pacote_perdidas(parte):
    global seq_perdidas, length_perdido, seq_, seq_int
    try:
        # pacote_str = pacote_bytes.decode('utf-8')
        # parte = json.loads(pacote_bytes)

        id_ = parte["id"]
        seq_ = parte["seq"]

        if seq_ == 0:
            length_perdido = parte["len"]
            print(f"Tamanho dados perdidos: {length_perdido}")
            logger.info(f"{length_perdido} pacotes perdidos na tramissão do DIARIO")
        else:
            pacotes_perdidos_bytes = bytes(parte["bloco"])
            for i in range(0, len(pacotes_perdidos_bytes), 2):
                perda = int.from_bytes(
                    pacotes_perdidos_bytes[i : i + 2], byteorder="big"
                )
                print(f"Pacote perdido: {perda}")
                seq_perdidas.append(perda)
            if seq_ != seq_int:
                print(f"ERRO: sequência perdida entre {seq_int} e {seq_-1}")
                seq_int = seq_

        seq_int += 1

    except Exception as e:
        print("Erro ao processar:", e)


# Abrir JSON
with open("/home/altona/altona-system/logs/JSON.txt", "r") as f:
    data = json.load(f)

now = datetime.now()
date = now.strftime("%d_%m_%Y")
shutil.move("/home/altona/altona-system/logs/JSON.txt", f"/home/altona/altona-system/data/JSON/JSON_{date}.txt")

# === Extrai os campos necessários ===
id_ = data["id"]
values = data["values[s]"]

# Estimar o número máximo de floats que cabem (aproximadamente)
def estimate_max_floats():
    amostra = [0.0] * 10
    teste = {"id": id_, "seq": 0, "checksum": "00000000", "dados": amostra}
    json_bytes = json.dumps(teste).encode("utf-8")
    used = len(json_bytes)
    sobra = MAX_BYTES - used
    if sobra <= 0:
        return 1
    return max(1, sobra // (FLOAT_SIZE + 2))


N = estimate_max_floats()

# === Divide em parcelas de no máximo 252 bytes ===
while idx < len(values):
    dados = values[idx : idx + N]
    seq += 1

    # Gera checksum usando os dados em binário
    dados_bin = struct.pack(f"{len(dados)}f", *dados)
    checksum = format(zlib.crc32(dados_bin), "08x")

    nova_parte = {"id": id_, "seq": seq, "checksum": checksum, "dados": dados}

    # Verifica se ficou abaixo do tamanho limite
    json_bytes = json.dumps(nova_parte).encode("utf-8")
    while len(json_bytes) > MAX_BYTES:
        # Reduz até caber
        dados = dados[:-1]
        dados_bin = struct.pack(f"{len(dados)}f", *dados)
        checksum = format(zlib.crc32(dados_bin), "08x")
        nova_parte["dados"] = dados
        nova_parte["checksum"] = checksum
        json_bytes = json.dumps(nova_parte).encode("utf-8")

    partes.append(json_bytes)
    idx += len(dados)

length_dados = seq

infos_enviados = {
    "id": data["id"],
    "seq": 0,
    "len": length_dados,
    "t": data["t"],  # Corrigido: agora é a string da data
    "dB": data["dB"],  # Arredonda a média para 2 casas decimais
    "std": data["std"], # Desvio padrao
    #"f": data["f"],  # Nome do arquivo de áudio (ajuste se necessário)
    "l": data["l"],  # True se algum valor passou de 85 dB
}
json_bytes = json.dumps(infos_enviados).encode("utf-8")
partes.insert(0, json_bytes)

while not handshake:
    cabecalho = {"id": id_, "m": "d"}
    json_cabecalho = json.dumps(cabecalho).encode("utf-8")
    rfm9x.send(json_cabecalho)
    print("Tentando Iniciar Comunicação")
    packet = rfm9x.receive()
    if packet:
        logger.info("Handshake DIÁRIO feito!")
        packet_text = packet.decode("utf-8")
        print(packet_text)
        resposta_handshake = json.loads(packet_text)
        if resposta_handshake["id"] == id_ and resposta_handshake["r"] == "d" or "a":
            handshake = True
    else:
        logger.critical("Sem resposta no handshake no envio de DIÁRIO")
        print("Esperando retorno do ACK")

time.sleep(1)

while not zero:
    rfm9x.send(partes[0])
    print("Mandando parte 0")
    packet = rfm9x.receive()
    if packet:
        packet_text = packet.decode("utf-8")
        print(packet_text)
        resposta_zero = json.loads(packet_text)
        if resposta_zero["id"] == id_ and resposta_zero["c"] == "s":
            zero = True
    else:
        print("Esperando retorno do zero para enviar o resto dos pacotes")

time.sleep(1)

# === Envio das mensagens ===
for i in range(1, len(partes)):
    parte = partes[i]
    print(parte)
    rfm9x.send(parte)
    parte_seq = json.loads(parte.decode("utf-8"))
    # print("seq", parte_seq["seq"])
    time.sleep(0.1)

inicio = time.time()
while final == False:
    if time.time() - inicio > 5 * 60:
        if COUNT_LAST_PKG > 5:
            logger.info("Tentou reenviar pacote final mais de 5 vezes. Encerrando comunicação...")
            final = True
            handshake = False
            COUNT_LAST_PKG = 0
            break
        else:
            COUNT_LAST_PKG +=1
            rfm9x.send(parte)
            time.sleep(0.1)

    packet = None
    packet = rfm9x.receive()
    if packet is None:
        print("Esperando pacote de resposta final")
    else:
        if len(packet) == 1 and packet[0] == 0:
            logger.info("Transmissão de DIARIO finalizado")
            print("Recebido pacote final sem perdas.")
            final = True
            seq_perdidas.clear()
            handshake = False
        else:
            parte = json.loads(packet.decode("utf-8"))
            print(parte)
            processa_pacote_perdidas(parte)
            print(f"Sequencia perdida: {seq_perdidas}")
            print(f"Seq_ antes do if:{seq_}")
            if seq_ == length_perdido:
                for perdida in seq_perdidas:
                    if perdida <= len(partes) - 1:
                        parte = partes[perdida]
                        time.sleep(0.1)
                        rfm9x.send(parte)
                        print(f"Reenviando sequencia perdida {perdida}: {parte}")
        seq_perdidas.clear()
        print(f"Seq perdidas depois do clear {seq_perdidas}")
