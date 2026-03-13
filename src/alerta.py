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
import shutil
from logger import configurar_logger
 
logger = configurar_logger("Alerta")

class EmissorAlerta:
    def __init__(self, id_mod=1):
        self.id = id_mod
        self.path_texto = "/home/altona/altona-system/logs/alerta.txt"
        self.path_audio = "/home/altona/altona-system/logs/audio_alerta.mp3"
        self.path_config = "/home/altona/altona-system/config/config.json"
        
        self.partes_texto = []
        self.partes_audio = []
        self.fase_atual = "texto" # texto ou audio
        
        with open(self.path_config, "r") as f:
            config = json.load(f)

        self.id = config["node"]

        self.MAX_BYTES = 252
        self.FLOAT_SIZE = 4 
    
    def iniciar_radio(self):
        try:
            cs = DigitalInOut(board.CE1)
            reset = DigitalInOut(board.D25)
            spi = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)
            self.rfm9x = adafruit_rfm9x.RFM9x(spi, cs, reset, 915.0)
            self.rfm9x.tx_power = 23
            return True
        except Exception as e:
            logger.error(f"Erro ao inicializar rádio: {e}")
            return False 
        
    def realizar_handshake(self):
        """Handshake infinito para Alerta (r: a) com cooldown inteligente para Busy"""
        msg = json.dumps({"id": self.id, "m": "a"}).encode("utf-8")

        cooldown_busy = 300  # 5 minutos
        
        logger.info("Iniciando busca por handshake de ALERTA...")

        while True:  
            self.rfm9x.send(msg)
            
            packet = self.rfm9x.receive()

            if packet:
                try:
                    res = json.loads(packet.decode("utf-8"))
                    if res.get("id") == self.id:
                        if res.get("id") == self.id and res.get("r") == "a":
                            logger.info("Handshake ALERTA aceito!")
                            return True
                        
                        elif res.get("r") == "b":
                            logger.warning(f"Receptor ocupado. Aguardando {cooldown_busy/60} min para tentar novamente...")
                            # sleep por 5 minutos para depois voltar a tentar 
                            time.sleep(cooldown_busy)
                      
                except Exception as e:
                    logger.error(f"Erro ao decodificar resposta do handshake: {e}")
            else:
                # Pequena pausa entre tentativas normais para não saturar o rádio
                time.sleep(2)
    
    def estimate_max_floats(self):
        amostra = [0.0] * 10
        teste = {"id": self.id, "seq": 0, "checksum": "00000000", "dados": amostra}
        json_bytes = json.dumps(teste).encode("utf-8")
        sobra = self.MAX_BYTES - len(json_bytes)
        return max(1, sobra // (self.FLOAT_SIZE + 2))

    def fragmenta_pacotes(self):
        try:
            # --- 1. Fragmentar Texto (alerta.txt) ---
            valores = []
            with open(self.path_texto, "r") as f:
                for linha in f:
                    p = linha.strip().split(";")
                    if len(p) == 2: valores.append(float(p[1]))
            
            N = self.estimate_max_floats()
            pacotes_txt = []
            idx, seq_txt = 0, 1 
            
            while idx < len(valores):
                chunk = valores[idx : idx + N]
                dbin = struct.pack(f"{len(chunk)}f", *chunk)
                cksum = format(zlib.crc32(dbin), "08x")
                
                nova_parte = {"id": self.id, "seq": seq_txt, "checksum": cksum, "dados": chunk}
                jb = json.dumps(nova_parte).encode("utf-8")
                
                while len(jb) > self.MAX_BYTES:
                    chunk = chunk[:-1]
                    dbin = struct.pack(f"{len(chunk)}f", *chunk)
                    cksum = format(zlib.crc32(dbin), "08x")
                    nova_parte.update({"dados": chunk, "checksum": cksum})
                    jb = json.dumps(nova_parte).encode("utf-8")

                pacotes_txt.append(jb)
                idx += len(chunk)
                seq_txt += 1

            # ----- Pacote zero Texto (metadados) ---------
            t_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            infos_zero_txt = {"id": self.id, "len": len(pacotes_txt), "seq": 0, "t": t_str}

            self.partes_texto = [json.dumps(infos_zero_txt).encode("utf-8")] + pacotes_txt

            # --- 2. Fragmentar Áudio (audio_alerta.mp3) ---
            with open(self.path_audio, "rb") as f:
                data_bin = f.read()
            
            idx, seq_aud = 0, 1 # CORREÇÃO: Reset da sequência para o áudio começar em 1
            pacotes_aud = []
            
            # DICA: Para áudio em lista JSON, comece com chunks menores (ex: 140 bytes)
            # para evitar que o ajuste de tamanho demore muito.
            CHUNK_AUD = 140 

            while idx < len(data_bin):
                chunk = data_bin[idx : idx + CHUNK_AUD]
                cksum = format(zlib.crc32(chunk), "08x")
                
                pacote = {"id": self.id, "seq": seq_aud, "checksum": cksum, "dados": list(chunk)}
                jb = json.dumps(pacote).encode("utf-8")

                while len(jb) > self.MAX_BYTES:
                    chunk = chunk[:-1]
                    cksum = format(zlib.crc32(chunk), "08x")
                    pacote.update({"dados": list(chunk), "checksum": cksum})
                    jb = json.dumps(pacote).encode("utf-8")

                pacotes_aud.append(jb)
                idx += len(chunk)
                seq_aud += 1

            infos_zero_aud = {"id": self.id, "seq": 0, "len": len(pacotes_aud), "t": t_str}
            self.partes_audio = [json.dumps(infos_zero_aud).encode("utf-8")] + pacotes_aud
            
            logger.info(f"Fragmentação do pacote ALERTA concluida")
            return True
        
        except Exception as e:
            logger.error(f"Erro na fragmentação do Alerta: {e}")
            return False
    
    def confirmar_pacote_zero(self, lista_partes):
        if not lista_partes:
            logger.error("Falha em pacote zero: Lista de pacotes está vazia.")
            return False

        pacote_zero = lista_partes[0]

        while True:
            logger.info(f"Enviando Pacote 0...")
            self.rfm9x.send(pacote_zero)
            
            packet = self.rfm9x.receive()
            
            if packet:
                try:
                    res = json.loads(packet.decode("utf-8"))
                    # Verifica se o ID bate e se a confirmação (c) é de sucesso (s)
                    if res.get("id") == self.id and res.get("c") == "s":
                        logger.info("Pacote 0 confirmado com sucesso!")
                        return True
                except Exception as e:
                    logger.error(f"Erro ao processar ACK do Pacote 0: {e}")
            
            time.sleep(1)
             
    def transmitir(self):

        # 1. Preparar pacotes e Rádio
        if not self.iniciar_radio():
            return

        # 2. Handshake Geral (m: a)
        if not self.realizar_handshake():
            return
        
        # 3. Fragmenta pacotes de audio e texxto
        if not self.fragmenta_pacotes():
            return
        
        # === FASE 1: TEXTO ===
        logger.info("Iniciando FASE 1: Texto do Alerta")
        if self.confirmar_pacote_zero(self.partes_texto):
            # Envia os dados do texto (índices 1 até o fim)
            time.sleep(1)
            for i in range(1, len(self.partes_texto)):
                self.rfm9x.send(self.partes_texto[i])
                time.sleep(0.1)
            
            # Recuperação da fase de texto
            self.loop_recuperacao(self.partes_texto, "texto")

        # === FASE 2: ÁUDIO ===
        logger.info("Iniciando FASE 2: Áudio MP3 do Alerta")
        if self.confirmar_pacote_zero(self.partes_audio):
            # Envia os dados do áudio
            time.sleep(1)
            for i in range(1, len(self.partes_audio)):
                self.rfm9x.send(self.partes_audio[i])
                time.sleep(0.1)
            
            # Recuperação da fase de áudio
            self.loop_recuperacao(self.partes_audio, "audio")
        
        self.finalizar_alerta_local()
    
    def loop_recuperacao(self, lista_partes, rotulo="fase"):
        """
        Escuta o Receptor para reenvios ou confirmação de sucesso.
        lista_partes: self.partes_texto ou self.partes_audio
        rotulo: apenas para logs (ex: 'texto' ou 'audio')
        """
        logger.info(f"Entrando em recuperação da fase: {rotulo}")
        inicio_rec = time.time()
        
        # Estado para reconstruir a lista de perdas do Receptor
        estado_rec = {
            "length_perdido": 0,
            "seq_recebida_cont": 0,
            "lista_acumulada": []
        }

        COUNT_LAST_PKG = 0

        while True:
            # Espera feedback do receptor
            packet = self.rfm9x.receive()
            
            if packet is None:
                if time.time() - inicio_rec > 300: #Se ja passou 5 minutos esperando o pacote
                    if COUNT_LAST_PKG > 5: #Tentou 5*300s = 15 minutos
                        logger.info("Tentou reenviar pacote final mais de 5 vezes. Encerrando comunicação...")
                        return # Sai da funcao de recuperacao e vai para o proximo estagio
                    else:
                        if not estado_rec.get("lista_acumulada"): #Se nao tiver chego nenhum pacote ainda, reenvia o ultimo
                            COUNT_LAST_PKG +=1
                            self.rfm9x.send(lista_partes[-1])
                            time.sleep(0.1)
                        else: #Se ja tiver chego algum pacote
                            logger.warning(f"Reenviando {len(estado_rec['lista_acumulada'])} pacotes de {rotulo}")
                            for p_idx in estado_rec["lista_acumulada"]:
                                if 0 <= p_idx < len(lista_partes):
                                    self.rfm9x.send(lista_partes[p_idx])
                                    time.sleep(0.1)
                        
                continue #Reinicia o loop while

            # Se chegou um pacote, reinicia o contador para ver o tempo de inatividade
            inicio_rec = time.time() 
            COUNT_LAST_PKG = 0

            # CASO SUCESSO: Receptor manda Byte 0
            if len(packet) == 1 and packet[0] == 0:
                logger.info(f"Sucesso confirmado na fase: {rotulo}!")
                return # Sai da funcao de recuperacao e vai para o proximo estagio

            # CASO REENVIOS: Processa a lista binária do Receptor
            try:
                parte = json.loads(packet.decode("utf-8"))
                if parte.get("id") == self.id:
                    # Usamos a mesma lógica do Emissor Diário
                    seq = parte.get("seq") if parte.get("seq") is not None else parte.get("seq_rec")
                    
                    if seq == 0:
                        estado_rec.update({"length_perdido": parte["len"], "seq_recebida_cont": 0, "lista_acumulada": []})
                        logger.info(f"Receptor solicitou {estado_rec['length_perdido']} blocos de reenvio.")
                    else:
                        pacotes_perdidos_bytes = bytes(parte["bloco"])
                        for i in range(0, len(pacotes_perdidos_bytes), 2):
                            perda = int.from_bytes(pacotes_perdidos_bytes[i:i+2], byteorder="big")
                            if perda not in estado_rec["lista_acumulada"]:
                                estado_rec["lista_acumulada"].append(perda)
                        estado_rec["seq_recebida_cont"] = seq

                    # Se recebeu todos os blocos de solicitação, reenvia agora
                    if estado_rec["length_perdido"] > 0 and estado_rec["seq_recebida_cont"] == estado_rec["length_perdido"]:
                        logger.warning(f"Reenviando {len(estado_rec['lista_acumulada'])} pacotes de {rotulo}")
                        for p_idx in estado_rec["lista_acumulada"]:
                            if 0 <= p_idx < len(lista_partes):
                                self.rfm9x.send(lista_partes[p_idx])
                                time.sleep(0.1)

                        # Reseta para a próxima rodada de perdas
                        estado_rec.update({"length_perdido": 0, "lista_acumulada": []})
                   
            except Exception as e:
                logger.error(f"Erro no loop de recuperação ({rotulo}): {e}")
    
    def finalizar_alerta_local(self):
        """Arquiva os arquivos enviados e limpa o sistema."""
        try:
            now = datetime.now()
            date_str = now.strftime("%d_%m_%Y")
            
            # Caminhos de destino
            dest_txt = f"/home/altona/altona-system/data/Alerta/alerta_{date_str}.txt"
            dest_aud = f"/home/altona/altona-system/data/Audios Alerta/audio_alerta_{date_str}.mp3"
            
            # Garante pastas
            os.makedirs(os.path.dirname(dest_txt), exist_ok=True)
            os.makedirs(os.path.dirname(dest_aud), exist_ok=True)

            if os.path.exists(self.path_texto): shutil.move(self.path_texto, dest_txt)
            if os.path.exists(self.path_audio): shutil.move(self.path_audio, dest_aud)
            
            logger.info("Alerta (Texto + Áudio) enviado e arquivado com sucesso!")
        except Exception as e:
            logger.error(f"Erro ao finalizar alerta: {e}")

def main():
    try:
        # 1. Instancia a classe
        # O __init__ já vai ler o ID do config.json automaticamente
        alerta = EmissorAlerta()
        
        logger.info("=== INICIANDO TRANSMISSÃO DE ALERTA CRÍTICO ===")

        # 2. Verifica se os arquivos necessários existem
        # Se não houver alerta.txt ou o mp3, não faz sentido ligar o rádio
        if not os.path.exists(alerta.path_texto) or not os.path.exists(alerta.path_audio):
            logger.warning("Arquivos de alerta não encontrados. Abortando.")
            return

        # 3. Executa o motor de transmissão
        # Handshake -> Texto -> Loop Recuperação -> Áudio -> Loop Recuperação -> Finalizacao
        alerta.transmitir()

    except KeyboardInterrupt:
        print("\nInterrompido manualmente.")
        logger.info("Alerta interrompido pelo usuário.")
        
    except Exception as e:
        # Erro crítico (ex: falha no barramento SPI ou rádio desconectado)
        logger.critical(f"ERRO FATAL NO ALERTA: {e}", exc_info=True)

if __name__ == "__main__":
    main()
