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

# Configurações de Caminho
BASE_DIR = "/home/altona/altona-system"
PATH_JSON = f"{BASE_DIR}/logs/JSON.txt"
PATH_STATE = f"{BASE_DIR}/state/emissor_state.json"

logger = configurar_logger("Emissor")

class EmissorDiario:
    def __init__(self):
        self.path_json = "/home/altona/altona-system/logs/JSON.txt"
        self.path_checkpoint = "/home/altona/altona-system/state/checkpoint_diario.json"
        
        # Variáveis de controle
        self.id_arquivo = 0
        self.partes = []
        self.proxima_seq_enviar = 0

        self.MAX_BYTES = 252
        self.FLOAT_SIZE = 4

        self.cooldown_handshake = 300
        self.last_try_handshake = 0
    
    def verificar_pendencias(self):
        """
        Verifica se existe o arquivo JSON para enviar e se há 
        um checkpoint de uma tentativa interrompida.
        """
        if not os.path.exists(self.path_json):
            logger.info("Nenhum arquivo JSON.txt pendente para envio.")
            return False

        if os.path.exists(self.path_checkpoint):
            try:
                with open(self.path_checkpoint, "r") as f:
                    ckpt = json.load(f)
                    # Verifica se o checkpoint é do arquivo atual (pela data)
                    data_hoje = datetime.now().strftime("%d_%m_%Y")
                    if ckpt.get("date") == data_hoje:
                        self.id_arquivo = ckpt.get("id_arquivo")
                        self.proxima_seq_enviar = ckpt.get("last_seq", 0)
                        self.partes = [p.encode('utf-8') for p in ckpt.get("partes", [])]
                        logger.info(f"Retomando envio DIÁRIO interrompido na sequência: {self.proxima_seq_enviar}")
                    else:
                        logger.info("Checkpoint antigo detectado. Iniciando novo ciclo.")
                        self.proxima_seq_enviar = 0
            except Exception as e:
                logger.error(f"Erro ao ler checkpoint: {e}")
                self.proxima_seq_enviar = 0
            
        return True

    def estimate_max_floats(self):
        amostra = [0.0] * 10
        teste = {"id": self.id_arquivo, "seq": 0, "checksum": "00000000", "dados": amostra}
        json_bytes = json.dumps(teste).encode("utf-8")
        used = len(json_bytes)
        sobra = self.MAX_BYTES - used
        if sobra <= 0:
            return 1
        return max(1, sobra // (self.FLOAT_SIZE + 2))
    
    def preparar_pacotes(self):
        """
        Lê o arquivo JSON e transforma em uma lista de bytes (self.partes).
        O índice da lista corresponderá à sequência (seq).
        """
        try:
            with open(self.path_json, "r") as f:
                data = json.load(f)
            
            values = data.get("values[s]", [])
            self.id_arquivo = data.get("id")
            
            # --- 1. Criar o Pacote 0 (Metadados) ---
            # Este pacote o Receptor usa para abrir o contexto do Diário
            infos_zero = {
                "id": self.id_arquivo,
                "seq": 0,
                "len": 0,          # Será preenchido após a contagem
                "t": data.get("t"),
                "dB": data.get("dB"),
                "std": data.get("std"),
                "l": data.get("l")
            }

            # --- 2. Fragmentar os Dados (Floats) ---
            N = self.estimate_max_floats()

            idx = 0
            pacotes_dados = []
            seq_atual = 1

            while idx < len(values):
                chunk = values[idx : idx + N]
                
                # Gerar Checksum binário (exatamente como o receptor espera)
                dados_bin = struct.pack(f"{len(chunk)}f", *chunk)
                checksum = format(zlib.crc32(dados_bin), "08x")
                
                corpo_pacote = {
                    "id": self.id_arquivo,
                    "seq": seq_atual,
                    "checksum": checksum,
                    "dados": chunk
                }

                json_bytes = json.dumps(corpo_pacote).encode("utf-8")

                while len(json_bytes) > self.MAX_BYTES:
                    # Reduz até caber
                    chunk = chunk[:-1]
                    dados_bin = struct.pack(f"{len(chunk)}f", *chunk)
                    checksum = format(zlib.crc32(dados_bin), "08x")
                    corpo_pacote["dados"] = chunk
                    corpo_pacote["checksum"] = checksum
                    json_bytes = json.dumps(corpo_pacote).encode("utf-8")

                # Transformar em bytes para envio
                pacotes_dados.append(json_bytes)
                
                idx += len(chunk)
                seq_atual += 1

            # --- 3. Finalizar e Unir ---
            infos_zero["len"] = seq_atual
            pacote_zero_bytes = json.dumps(infos_zero).encode("utf-8")
            
            # A lista self.partes terá o pacote 0 no índice 0, pacote 1 no índice 1, etc.
            self.partes = [pacote_zero_bytes] + pacotes_dados
            return True

        except Exception as e:
            logger.error(f"Erro ao processar fragmentação: {e}")
            return False
        
    def iniciar_radio(self):
        """Configura o hardware do rádio LoRa."""
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
        """
        Tenta iniciar a comunicação com o receptor.
        Retorna True se puder prosseguir, False se estiver ocupado ou sem resposta.
        """
        cabecalho = {"id": self.id_arquivo, "m": "d"}
        msg = json.dumps(cabecalho).encode("utf-8")
        
        handshake = False

        while not handshake:
            logger.info(f"Tentando handshake...)")
            self.rfm9x.send(msg)
            
            packet = self.rfm9x.receive()
            if packet:
                try:
                    res = json.loads(packet.decode("utf-8"))
                    if res.get("id") == self.id_arquivo:
                        if res.get("r") == "d":
                            logger.info("Handshake DIÁRIO aceito pelo receptor.")
                            return True
                        elif res.get("r") == "b":
                            logger.warning("Receptor ocupado (Busy) com Alerta. Encerrando para tentar mais tarde.")
                            self.last_try_handshake = time.time()
                            return False
                except Exception as e:
                    logger.error(f"Erro ao processar resposta do handshake: {e}")
            time.sleep(1)
            
        logger.error("Falha no handshake: Receptor não respondeu.")
        return False
    
    def confirmar_pacote_zero(self):
        """
        Envia o Pacote 0 (metadados) e aguarda confirmação (c: s).
        Fundamental para o Receptor saber o tamanho total dos dados.
        """
        if not self.partes:
            return False

        pacote_zero = self.partes[0]
        confirmado = False

        while not confirmado:
            logger.info(f"Enviando Pacote 0 (Metadados)...")
            self.rfm9x.send(pacote_zero)
            
            # Aguarda o ACK específico do pacote zero: {"id": X, "c": "s"}
            packet = self.rfm9x.receive()
            if packet:
                try:
                    res = json.loads(packet.decode("utf-8"))
                    if res.get("id") == self.id_arquivo and res.get("c") == "s":
                        logger.info("Pacote 0 confirmado pelo receptor.")
                        confirmado = True
                        return True
                except Exception as e:
                    logger.error(f"Erro ao processar ACK do pacote zero: {e}")
            
            time.sleep(1)
        return False
    
    def salvar_progresso(self, seq):
        """Salva o progresso para que o Observer possa retomar o serviço se necessário."""
        try:
            # Convertemos os pacotes de bytes para string para o JSON aceitar
            partes_str = [p.decode('utf-8') for p in self.partes]
            
            with open(self.path_checkpoint, "w") as f:
                json.dump({
                    "date": datetime.now().strftime("%d_%m_%Y"),
                    "last_seq": seq,
                    "id_arquivo": self.id_arquivo,
                    "partes": partes_str
                }, f)
            logger.info(f"Checkpoint salvo na seq {seq}.")
        except Exception as e:
            logger.error(f"Erro ao salvar checkpoint: {e}")
    
    def transmitir(self):
        """Motor principal: Handshake -> Pacote 0 -> Dados -> Recuperação."""
        if not self.verificar_pendencias():
            return

        if not self.iniciar_radio():
            return

        if self.proxima_seq_enviar == 0:
            
            # 1. Montar partes
            if not self.preparar_pacotes():
                return
            
            # 2. Handshake Inicial
            sucesso_handshake = self.realizar_handshake()
            while not sucesso_handshake:
                if (time.time() - self.last_try_handshake) > self.cooldown_handshake: #Tentar o handshake a cada 5 minutos até conseguir
                    sucesso_handshake = self.realizar_handshake()
                time.sleep(1)
            
            # 3. Envio pacote zero
            if not self.confirmar_pacote_zero():
                return
        else:

            # 2. Handshake de retomada
            sucesso_handshake = self.realizar_handshake()
            while not sucesso_handshake:
                if (time.time() - self.last_try_handshake) > self.cooldown_handshake: #Tentar o handshake a cada 5 minutos até conseguir
                    sucesso_handshake = self.realizar_handshake()
                time.sleep(1)

        time.sleep(1)

        # 3. Transmissão dos Dados
        inicio = max(1, self.proxima_seq_enviar)
        
        logger.info(f"Iniciando envio dos dados a partir da sequência {inicio}")
        for i in range(inicio, len(self.partes)):

            if os.path.exists("/home/altona/altona-system/Flags/INTERRUPT_FLAG"):
                self.salvar_progresso(i)
                break

            self.rfm9x.send(self.partes[i])
                
            time.sleep(0.1) # Flow control para não sobrecarregar o receptor

        # 4. Fase de Recuperação de Pacotes Perdidos
        self.loop_recuperacao()

        self.finalizar_sessao_local()
    
    def processar_pacote_perdidas(self, parte, estado_rec):
        """
        Lógica para processar os pacotes de solicitação do Receptor.
        estado_rec: dicionário para manter controle entre as chamadas do loop.
        """
        try:
            # O Receptor usa 'seq' no cabeçalho e 'seq_rec' nos blocos (conforme constroi_blocos_solicitacao)
            # Mas vamos checar ambos para garantir compatibilidade
            seq = parte.get("seq") if parte.get("seq") is not None else parte.get("seq_rec")
            
            if seq == 0:
                estado_rec["length_perdido"] = parte["len"]
                estado_rec["seq_recebida_cont"] = 0
                estado_rec["lista_acumulada"] = []
                logger.info(f"Receptor solicitou {estado_rec['length_perdido']} blocos de reenvio.")
            else:
                pacotes_perdidos_bytes = bytes(parte["bloco"])
                for i in range(0, len(pacotes_perdidos_bytes), 2):
                    perda = int.from_bytes(pacotes_perdidos_bytes[i : i + 2], byteorder="big")
                    if perda not in estado_rec["lista_acumulada"]:
                        estado_rec["lista_acumulada"].append(perda)
                estado_rec["seq_recebida_cont"] = seq

            # Se recebemos o último bloco esperado, disparamos o reenvio
            if estado_rec["length_perdido"] > 0 and estado_rec["seq_recebida_cont"] == estado_rec["length_perdido"]:
                logger.info(f"Reenviando {len(estado_rec['lista_acumulada'])} pacotes agora...")
                for p_idx in estado_rec["lista_acumulada"]:
                    if 0 <= p_idx < len(self.partes):
                        self.rfm9x.send(self.partes[p_idx])
                        time.sleep(0.1)
                
                # Reseta estado para a próxima possível rodada de perdas
                estado_rec["length_perdido"] = 0
                estado_rec["lista_acumulada"] = []

        except Exception as e:
            logger.error(f"Erro ao processar pacote de perdas: {e}")

    def loop_recuperacao(self):
        """Loop que escuta o Receptor após o envio principal."""
        logger.info("Aguardando confirmação final ou lista de reenvios...")
        inicio_rec = time.time()

        # Dicionário de estado local para esta sessão de recuperação
        estado_rec = {
            "length_perdido": 0,
            "seq_recebida_cont": 0,
            "lista_acumulada": []
        }
        
        COUNT_LAST_PKG = 0

        while True:

            packet = self.rfm9x.receive()
            
            if packet is None:
                if time.time() - inicio_rec > 300:
                    if COUNT_LAST_PKG > 5: #Tentou 5*300s = 15 minutos
                        logger.info("Tentou reenviar pacote final mais de 5 vezes. Encerrando comunicação...")
                        return
                    else:
                        if not estado_rec.get("lista_acumulada"): #Se nao tiver chego nenhum pacote ainda, reenvia o ultimo
                            COUNT_LAST_PKG +=1
                            self.rfm9x.send(self.partes[-1])
                            time.sleep(0.1)
                        else: #Se ja tiver chego algum pacote
                            logger.warning(f"Reenviando {len(estado_rec['lista_acumulada'])} apos 5 minutos sem resposta")
                            for p_idx in estado_rec["lista_acumulada"]:
                                if 0 <= p_idx < len(self.partes):
                                    self.rfm9x.send(self.partes[p_idx])
                                    time.sleep(0.1)
                continue
            
            # Se chegou um pacote, reinicia o contador para ver o tempo de inatividade
            inicio_rec = time.time()
            COUNT_LAST_PKG = 0

            # CASO SUCESSO ABSOLUTO (O Receptor mandou o Byte 0)
            if len(packet) == 1 and packet[0] == 0:
                logger.info("Transmissão de DIARIO finalizado")
                return

            # CASO REENVIOS (JSON)
            try:
                parte = json.loads(packet.decode("utf-8"))
                if parte.get("id") == self.id_arquivo:
                    self.processar_pacote_perdidas(parte, estado_rec)
            except Exception:
                pass
            
    
    def finalizar_sessao_local(self):
        """Arquiva o JSON e deleta o checkpoint. O Receptor já encerrou."""
        try:
            date_str = datetime.now().strftime("%d_%m_%Y")
            destino = f"/home/altona/altona-system/data/JSON/JSON_{date_str}.txt"
            
            os.makedirs(os.path.dirname(destino), exist_ok=True)
            
            if os.path.exists(self.path_json):
                shutil.move(self.path_json, destino)
                logger.info(f"Arquivo movido para {destino}")
            
            if os.path.exists(self.path_checkpoint):
                os.remove(self.path_checkpoint)
                logger.info("Checkpoint removido. Emissor finalizado.")

        except Exception as e:
            logger.error(f"Erro ao finalizar: {e}")

def main():
    try:
        # Instancia a classe do Emissor
        emissor = EmissorDiario()
        
        logger.info("Iniciando processo de envio do DIÁRIO...")

        emissor.transmitir()

    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário (CTRL+C)")
        logger.info("Envio interrompido manualmente.")
        
    except Exception as e:
        logger.critical(f"Erro inesperado no Emissor: {e}", exc_info=True)
        # Não removemos o checkpoint aqui para que o Observer possa 
        # reiniciar o script e tentar recuperar de onde parou.

if __name__ == "__main__":
    main()