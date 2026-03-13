import time
from datetime import datetime
import adafruit_rfm9x
import busio
from digitalio import DigitalInOut
import board
import zlib
import struct
import json
import os
from logger import configurar_logger

# === Parâmetros globais ===
MAX_BYTES = 252
FLOAT_SIZE = 4

logger = configurar_logger("Alerta")


# === Estimar quantos floats cabem no JSON sem ultrapassar 252 bytes ===
def estimate_max_floats(id):
    amostra = [0.0] * 10
    teste = {"id": id, "seq": 0, "checksum": "00000000", "dados": amostra}
    json_bytes = json.dumps(teste).encode("utf-8")
    sobra = MAX_BYTES - len(json_bytes)
    return max(1, sobra // (FLOAT_SIZE + 2))


# === Função genérica para fragmentar arquivo ===
def fragmenta_arquivo(caminho, tipo="float", id=1):
    """
    Fragmenta arquivo em pacotes de até 252 bytes.
    tipo pode ser:
      - "float"   -> arquivo de texto com valores numéricos
      - "binario" -> arquivo binário (ex: mp3, jpg)
    """
    partes = []
    seq = 0

    if tipo == "float":
        valores = []
        with open(caminho, "r") as f:
            for linha in f:
                partes_linha = linha.strip().split(";")
                if len(partes_linha) == 2:
                    try:
                        valor = float(partes_linha[1])
                        valores.append(valor)
                    except ValueError:
                        continue

        N = estimate_max_floats(id)
        idx = 0
        while idx < len(valores):
            dados = valores[idx : idx + N]
            seq += 1
            dados_bin = struct.pack(f"{len(dados)}f", *dados)
            checksum = format(zlib.crc32(dados_bin), "08x")

            nova_parte = {"id": id, "seq": seq, "checksum": checksum, "dados": dados}

            json_bytes = json.dumps(nova_parte).encode("utf-8")
            # === Ajusta até caber ===
            while len(json_bytes) > MAX_BYTES:
                dados = dados[:-1]
                dados_bin = struct.pack(f"{len(dados)}f", *dados)
                checksum = format(zlib.crc32(dados_bin), "08x")
                nova_parte["dados"] = dados
                nova_parte["checksum"] = checksum
                json_bytes = json.dumps(nova_parte).encode("utf-8")

            partes.append(json_bytes)
            idx += len(dados)

    elif tipo == "binario":
        with open(caminho, "rb") as f:
            data = f.read()
            print(f"Tamanho da data do arquivo binário: {len(data)}")
        idx = 0
        # print(f"Seq: {seq}") # adicionado para teste
        while idx < len(data):
            # começa com o máximo possível
            print("Montando mensagem inicial")
            chunk = data[idx : idx + MAX_BYTES]
            seq += 1
            checksum = format(zlib.crc32(chunk), "08x")

            pacote = {"id": id, "seq": seq, "checksum": checksum, "dados": list(chunk)}
            json_bytes = json.dumps(pacote).encode("utf-8")

            # === Ajusta até caber (igual ao caso float) ===  ### ALTERADO
            while len(json_bytes) > MAX_BYTES:
                chunk = chunk[:-1]
                checksum = format(zlib.crc32(chunk), "08x")
                pacote["dados"] = list(chunk)
                pacote["checksum"] = checksum
                json_bytes = json.dumps(pacote).encode("utf-8")

            partes.append(json_bytes)
            idx += len(chunk)

            # print(f"Pacote: {json_bytes}") #teste para ver os pacotes
            # print(f"Partes: {partes}\n") # teste para ver as partes juntas

    return partes, seq


def enviar_pacotes(rfm9x, partes, id, tipo="mensagem"):
    """
    Envia pacotes via rádio, trata perdas e aguarda confirmação final.
    rfm9x  -> objeto rádio
    partes -> lista de pacotes (json_bytes)
    id     -> identificação do módulo
    tipo   -> "mensagem" ou "audio" (só para logs)
    """
    # === Envio dos pacotes ===
    for i in range(1, len(partes)):
        parte = partes[i]
        print(parte)
        rfm9x.send(parte)

    # === Recebe pacotes perdidos e reenvia ===
    final = False
    while not final:
        packet = rfm9x.receive()
        print(packet)
        if packet is None:
            print(f"Esperando Resposta Final ({tipo})")
        else:
            if len(packet) == 1 and packet[0] == 0:
                logger.info(f"Transmissão de ALERTA finalizado no envio de {tipo}")
                print(f"Recebido pacote final sem perdas ({tipo}).")
                final = True

            else:
                seq_perdidas = []

                try:
                    # 1️⃣ Decodifica o pacote de perdas (JSON enviado pelo receptor)
                    parte = json.loads(packet.decode("utf-8"))

                    # 2️⃣ Extrai e reconstrói o campo "bloco" (lista de bytes)
                    bloco_bytes = bytes(parte["bloco"])

                    # 3️⃣ Converte cada par de bytes em um número de sequência perdido
                    for i in range(0, len(bloco_bytes), 2):
                        perda = int.from_bytes(bloco_bytes[i : i + 2], byteorder="big")
                        seq_perdidas.append(perda)

                    logger.info(
                        f"{len(seq_perdidas)} pacotes perdidos na transmissão do ALERTA no envio de {tipo}"
                    )

                    print(f"Sequências perdidas ({tipo}):", seq_perdidas)

                    # 4️⃣ Reenvia cada pacote perdido
                    for perdida in seq_perdidas:
                        if 0 <= perdida < len(partes):
                            print(f"Reenviando sequência {perdida} ({tipo})")
                            rfm9x.send(partes[perdida])
                            time.sleep(0.1)

                except Exception as e:
                    logger.error(f"Erro ao processar pacote de perdas ({tipo}): {e}")
                    print(f"Erro ao processar pacote de perdas ({tipo}): {e}")


def emitir_alerta():

    # Configuração do Modulo
    id = 1

    # === Parâmetros ===
    seq = 0
    partes = []
    handshake = False
    zero = False
    final = False

    # Inicialização do rádio
    CS = DigitalInOut(board.CE1)
    RESET = DigitalInOut(board.D25)
    spi = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)
    rfm9x = adafruit_rfm9x.RFM9x(spi, CS, RESET, 915.0)
    rfm9x.tx_power = 23
    
    # === Handshake inicial ===
    logger.info("Iniciando HANDSHAKE de ALERTA!")
    while not handshake:
        cabecalho = {"id": id, "m": "a"}
        json_cabecalho = json.dumps(cabecalho).encode("utf-8")
        rfm9x.send(json_cabecalho)
        print("Tentando Iniciar Comunicação")

        packet = rfm9x.receive()
        if packet:
            logger.info("Handshake ALERTA feito!")
            packet_text = packet.decode("utf-8")
            print(packet_text)
            resposta_handshake = json.loads(packet_text)
            if resposta_handshake["id"] == id and resposta_handshake["r"] == "a":
                handshake = True
        else:
            logger.critical("Erro no handshake no envio de ALERTA - Esperando retorno")
            print("Esperando retorno do ACK")

    time.sleep(1)
    

    # === Fragmentacao da mensagem ===
    
    print("Fragmentando alerta.txt")
    # === Fragmenta alerta.txt em pacotes (usando a função genérica) ===
    partes_mensagem, length_dados = fragmenta_arquivo("/home/altona/altona-system/logs/alerta.txt", tipo="float", id=id)
    print(f"{length_dados} pacotes gerados a partir do arquivo alerta.txt")

    # === Cabeçalho inicial com metadados ===
    infos_enviados = {
        "id": id,
        "len": length_dados,
        "seq": 0,
        "t": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    json_bytes = json.dumps(infos_enviados).encode("utf-8")
    partes_mensagem.insert(0, json_bytes)

    print("Fragmentando 30s.mp3")
    # === Fragmenta 30s.mp3 em pacotes (usando a função genérica) ===
    partes_audio, length_dados = fragmenta_arquivo(
        "/home/altona/altona-system/logs/audio_alerta.mp3", tipo="binario", id=id
    )
    print(f"{length_dados} pacotes gerados a partir do arquivo 30s.mp3")

    # === Cabeçalho inicial com metadados ===
    infos_enviados = {
        "id": id,
        "len": length_dados,
        "seq": 0,
        "t": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    json_bytes = json.dumps(infos_enviados).encode("utf-8")
    partes_audio.insert(0, json_bytes)

    # Renomeia arquivo após leitura
    now = datetime.now()
    date = now.strftime("%d_%m_%Y")
    os.rename("/home/altona/altona-system/logs/alerta.txt", f"/home/altona/altona-system/data/Alerta/alerta_{date}.txt")
    os.rename("/home/altona/altona-system/logs/audio_alerta.mp3", f"/home/altona/altona-system/data/Audios Alerta/audio_alerta_{date}.mp3")

    # ====================== Envio da mensagem ======================

    while not zero:
        rfm9x.send(partes_mensagem[0])
        print(f"Mandando parte 0: {partes_mensagem[0]}")
        packet = rfm9x.receive()
        if packet:
            packet_text = packet.decode("utf-8")
            print(packet_text)
            resposta_zero = json.loads(packet_text)
            if resposta_zero["id"] == id and resposta_zero["c"] == "s":
                zero = True
        else:
            print("Esperando retorno do zero para enviar o resto dos pacotes")

    time.sleep(1)

    enviar_pacotes(rfm9x, partes_mensagem, id, tipo="mensagem")

    # ====================== Envio do áudio ======================

    zero = False
    while not zero:
        rfm9x.send(partes_audio[0])
        print(f"Mandando parte 0: {partes_mensagem[0]}")
        packet = rfm9x.receive()
        if packet:
            packet_text = packet.decode("utf-8")
            print(packet_text)
            resposta_zero = json.loads(packet_text)
            if resposta_zero["id"] == id and resposta_zero["c"] == "s":
                zero = True
        else:
            print("Esperando retorno do zero para enviar o resto dos pacotes")

    time.sleep(1)

    enviar_pacotes(rfm9x, partes_audio, id, tipo="audio")


if __name__ == "__main__":
    logger.info("Alerta sendo transmitido")
    emitir_alerta()
