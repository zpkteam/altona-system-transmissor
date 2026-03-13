#!/usr/bin/env python3
import pyaudio
import struct
import math
import time
import wave
import os
import sys
import shutil
from pydub import AudioSegment
from threading import Thread
from datetime import datetime, time as dtime
from construcaoJson import construir_json
from alerta import emitir_alerta
from collections import deque
from logger import configurar_logger

# ----------------------------
# Configurações de captura
# ----------------------------
CHUNK = 48000  # 1 segundo de áudio (samples por leitura)
FORMAT = pyaudio.paInt16
CHANNELS = 1
RATE = 48000

# Segmentação por hora (cada arquivo = 1 hora)
SEGMENT_SECONDS = 3600  # 1 hora
SAMPLES_PER_SEGMENT = SEGMENT_SECONDS  # porque CHUNK = 1s

# Escrita / flush
FLUSH_INTERVAL = 1000  # flush do log de SPL a cada 1000 amostras

# Janelas para alerta
tam_janela = 60  # 60 amostras = 60 segundos = 1 minuto
amostras_anteriores = 5  # contexto antes do início da sequência

# Arquivos / pastas
BASE_DIR = "/home/altona/altona-system/logs"  # onde fica o log.txt e alertas
BASE_DIR_WAV = "/home/altona/altona-system/data/gravacaoDiaria"  # nova pasta raiz para gravações por dia
DEST_LOG_DIR = "/home/altona/altona-system/data/medicoesDiarias"
LOG_FILE_PATH = os.path.join(BASE_DIR, "log.txt")
OUTPUT_FILE_ALERTA_WAV = "audio_alerta.wav"
OUTPUT_FILE_ALERTA_MP3 = "audio_alerta.mp3"

# Outros
logger = configurar_logger("Microfone")
alerta_disparado = False

# ----------------------------
# Funções utilitárias
# ----------------------------


def obter_limite_atual():
    agora = datetime.now().time()
    if dtime(7, 0) <= agora <= dtime(22, 0):
        return 60  # das 07:00 às 22:00
    else:
        return 55  # das 22:01 às 06:59


def garantir_pasta_do_dia(data):
    """Cria pasta no formato /home/altona/gravacaoDiaria/DD_MM_YYYY"""
    nome_pasta = data.strftime("%d_%m_%Y")
    caminho = os.path.join(BASE_DIR_WAV, nome_pasta)
    os.makedirs(caminho, exist_ok=True)
    return caminho


def abrir_wav_da_hora(pyaudio_instancia, dt: datetime):
    caminho = garantir_pasta_do_dia(dt.date())
    nome_arquivo = f"{dt.strftime('%d_%m_%Y')}_{dt.strftime('%H')}h.wav"
    arquivo = os.path.join(caminho, nome_arquivo)
    # abrir em modo wb (write) e configurar canais/sampwidth/framerate
    wf = wave.open(arquivo, "wb")
    wf.setnchannels(CHANNELS)
    wf.setsampwidth(pyaudio_instancia.get_sample_size(FORMAT))
    wf.setframerate(RATE)
    return wf


def safe_fsync(file_obj):
    try:
        file_obj.flush()  # Força a escrever no buffer do SO
        os.fsync(file_obj.fileno())  # Força a escrever fisicamente no disco/SD
    except Exception:
        # em alguns sistemas de arquivo (SD), fsync pode falhar; ainda assim o flush ajuda
        pass


# ----------------------------
# Função principal de captura
# ----------------------------
def capturar_audio():
    global alerta_disparado

    # Buffers de contexto curtos (mantêm apenas o necessário para gerar alerta)
    # Agora spl_buffer guarda tuplas (indice_amostra, spl_float, audio_bytes)
    max_context_entries = tam_janela + amostras_anteriores + 10
    spl_buffer = deque(maxlen=max_context_entries)  # (idx, spl, data)

    indice_amostra = 0
    ultima_execucao = datetime.now().date()  # controle para o JSON diário
    
    p = pyaudio.PyAudio()

    # função para abrir o stream
    def abrir_stream():
        return p.open(
            format=FORMAT, channels=CHANNELS, rate=RATE, input=True, frames_per_buffer=CHUNK
        )

    stream = None
    try:
        stream = abrir_stream()
    except Exception as e:
        logger.error(f"Erro abrindo stream inicialmente: {e}")
        # tenta limpar e relançar o erro para reiniciar o processo
        try:
            p.terminate()
        except:
            pass
        raise

    # Preparar arquivo de log (texto) - append contínuo
    os.makedirs(BASE_DIR, exist_ok=True)
    log_diario = open(LOG_FILE_PATH, "a", buffering=1)  # line buffered
    samples_since_flush = 0

    # Preparar arquivo WAV da hora atual (na pasta do dia)
    agora = datetime.now()
    hora_atual_dt = agora.replace(minute=0, second=0, microsecond=0)

    try:
        os.makedirs(BASE_DIR_WAV, exist_ok=True)
    except Exception:
        # se não conseguir criar a pasta raiz, ainda continua e tenta criar no abrir_wav_da_hora
        pass
    try:
        wf_diario = abrir_wav_da_hora(p, hora_atual_dt)
    except Exception as e:
        logger.error(f"Erro abrindo WAV inicial: {e}")
        # tenta abrir sem interromper (será tentado novamente dentro do loop na rotação)
        wf_diario = None

    print("Capturando áudio... (CTRL+C para parar)")

    try:
        while True:
            loop_inicio = time.time()
            try:
                data = stream.read(CHUNK, exception_on_overflow=False)
            except Exception as e:
                # tenta reiniciar o stream (como no código original)
                logger.error(f"Falha na leitura do stream de áudio: {e}. Reiniciando stream...")
                try:
                    stream.stop_stream()
                    stream.close()
                except Exception:
                    pass
                time.sleep(0.2)
                try:
                    stream = abrir_stream()
                except Exception as e2:
                    logger.error(f"Falha ao reabrir stream: {e2}. Pausando 1s antes de tentar novamente.")
                    time.sleep(1)
                continue  # pula para a próxima iteração do while

            # Calcula RMS e SPL (mesma fórmula que você já tinha)
            try:
                samples = struct.unpack("<" + "h" * CHUNK, data)
            except Exception as e:
                logger.error(f"Erro unpacking dados de áudio: {e}")
                continue  # pula para a próxima iteração

            sum_squares = sum((s / 32768.0) ** 2 for s in samples)
            rms = math.sqrt(sum_squares / CHUNK)

            if rms <= 0.00109:
                spl = rms * (-5218.88631) + 54.06670447
            elif rms > 0.00109 and rms <= 0.002053:
                spl = rms * (4386.881945) + 46.43418128
            elif rms > 0.002053 and rms <= 0.0038:
                spl = rms * (4081.365583) + 57.86457827
            elif rms > 0.0038 and rms <= 0.005100:
                spl = rms * (4086.037127) + 58.93575636
            else:
                spl = rms * (87.96853481) + 85.4564661

            horario = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Escreve SPL diretamente no log (arquivo)
            linha_spl = f"{horario};{spl:.2f}\n"
            try:
                log_diario.write(linha_spl)
            except Exception as e:
                logger.error(f"Erro escrevendo log SPL: {e}")

            samples_since_flush += 1
            if samples_since_flush >= FLUSH_INTERVAL:
                safe_fsync(log_diario)
                samples_since_flush = 0

            # Guarda em buffer curto para contexto/alerta (sincroniza SPL e áudio)
            spl_buffer.append((indice_amostra, spl, data, horario))

            # Escreve o chunk no arquivo WAV atual (modo streaming)
            if wf_diario is not None:
                try:
                    wf_diario.writeframesraw(data)
                except Exception as e:
                    logger.error(f"Erro escrevendo no arquivo WAV diario: {e}")

            # ----------------------------
            # Rotação de arquivo WAV por HORA 
            # ----------------------------
            agora_dt = datetime.now()
            if (wf_diario is None) or (agora_dt.hour != hora_atual_dt.hour) or (agora_dt.date() != hora_atual_dt.date()):
                # fecha arquivo antigo corretamente (se existir)
                try:
                    if wf_diario is not None:
                        wf_diario.close()
                except Exception as e:
                    logger.error(f"Erro fechando arquivo WAV antigo: {e}")

                # abre novo arquivo para nova  hora dentro da pasta do dia
                hora_atual_dt = agora_dt.replace(minute=0, second=0, microsecond=0)
                try:
                    wf_diario = abrir_wav_da_hora(p, hora_atual_dt)
                    logger.info(f"Abrindo novo arquivo de audio")
                except Exception as e:
                    logger.error(f"Erro abrindo novo arquivo WAV: {e}")
                    # tenta reabrir depois de uma pausa
                    time.sleep(0.5)
                    try:
                        wf_diario = abrir_wav_da_hora(p, hora_atual_dt)
                    except Exception as e2:
                        logger.error(f"Segunda tentativa falhou ao abrir WAV: {e2}")
                        wf_diario = None


            # ----------------------------
            # Lógica de alerta (versão final corrigida)
            # ----------------------------
            limite_atual = obter_limite_atual()

            if spl >= limite_atual:
                logger.warning(f"Ruído acima do limite de {limite_atual}: {spl:.2f} dB")

            # Verifica se a janela está completa e TODOS os valores da janela >= limite
            if len(spl_buffer) >= tam_janela:
                # últimos tam_janela elementos (em ordem do mais antigo para o mais novo)
                janela = list(spl_buffer)[-tam_janela:]

                # extrai só os valores SPL
                valores_janela = [val for (_, val, _, _) in janela]

                if all(v >= limite_atual for v in valores_janela):
                    if not alerta_disparado:
                        logger.warning("Ruído a mais de um minuto acima do limite. Disparando alerta")

                        # índice do primeiro item da janela (mais antigo)
                        indice_inicio_seq = janela[0][0]
                        indice_inicio_contexto = max(0, indice_inicio_seq - amostras_anteriores)

                        dados_ultrapassados = []
                        dados_audio = []

                        #print(f"Alerta! Primeira amostra acima do limite no índice {indice_inicio_seq}")
                        #print(f"Valores {amostras_anteriores} anteriores a ela:")

                        # --- Coleta contexto anterior (se disponível dentro do buffer) ---
                        # percorre o buffer inteiro (ordenado) e pega índices no intervalo de contexto
                        for idx, val, audio_chunk, horario_dado  in list(spl_buffer):
                            if indice_inicio_contexto <= idx < indice_inicio_seq:
                                #print(f"{idx}: {val:.2f}")
                                dados_ultrapassados.append(f"{horario_dado};{val:.2f}")
                                dados_audio.append(audio_chunk)

                        # --- Coleta os frames da janela (em ordem) ---
                        for idx, val, audio_chunk, horario_dado in janela:
                            print(f"{idx}")
                            dados_ultrapassados.append(f"{horario_dado};{val:.2f}")
                            dados_audio.append(audio_chunk)

                        # Grava WAV concatenando os frames
                        try:
                            wf = wave.open(os.path.join(BASE_DIR, OUTPUT_FILE_ALERTA_WAV), "wb")
                            wf.setnchannels(CHANNELS)
                            wf.setsampwidth(p.get_sample_size(FORMAT))
                            wf.setframerate(RATE)
                            wf.writeframes(b"".join(dados_audio))
                            wf.close()
                        except Exception as e:
                            logger.error(f"Erro criando WAV de alerta: {e}")

                        # Converte para MP3 e amplifica
                        try:
                            audio = AudioSegment.from_wav(os.path.join(BASE_DIR, OUTPUT_FILE_ALERTA_WAV))
                            audio_amplificado = audio + 10 #equivale a multiplicar o volume por 3.16
                            audio_amplificado.export(os.path.join(BASE_DIR, OUTPUT_FILE_ALERTA_MP3), format="mp3")
                            os.remove(os.path.join(BASE_DIR, OUTPUT_FILE_ALERTA_WAV))
                        except Exception as e:
                            logger.error(f"Erro convertendo alerta para mp3: {e}")

                        # Salva os SPLs
                        try:
                            with open(os.path.join(BASE_DIR, "alerta.txt"), "a") as f:
                                f.write("\n".join(dados_ultrapassados) + "\n")
                        except Exception as e:
                            logger.error(f"Erro escrevendo alerta.txt: {e}")

                        # Dispara alerta
                        if not os.path.exists("/home/altona/altona-system/Flags/ALERT_FLAG"):
                            open("/home/altona/altona-system/Flags/ALERT_FLAG", "w").close()

                        alerta_disparado = True
                else:
                    alerta_disparado = False
            else:
                alerta_disparado = False

            indice_amostra += 1
            
            if os.path.exists("/home/altona/altona-system/Flags/ROTATE_FLAG"):
                try:                   
                    # Fecha o log atual
                    log_diario.close() #encerra o handle com o log atual
                    
                    # Move log para a pasta destino ja
                    shutil.move(LOG_FILE_PATH, os.path.join(DEST_LOG_DIR,"log.txt"))
                    
                    logger.info("Flag de rotacao localizada: Log movido")
                    
                    os.remove("/home/altona/altona-system/Flags/ROTATE_FLAG")
                except Exception as e:
                    logger.error(f"Erro ao mover log: {e}")

            # Mensagem de console
            print(f"Nível de pressão sonora: {spl:.2f} dB SPL e indice da amostra {indice_amostra}")

            # Proteção: se o loop estiver demorando muito (ex: > 2s), reinicia o stream
            loop_tempo = time.time() - loop_inicio
            if loop_tempo > 2:
                logger.error(f"Loop demorou {loop_tempo:.2f}s -> reiniciando stream")
                try:
                    stream.stop_stream()
                    stream.close()
                except Exception:
                    pass
                time.sleep(0.2)
                try:
                    stream = abrir_stream()
                except Exception as e:
                    logger.error(f"Falha ao reabrir stream após timeout: {e}")

    except KeyboardInterrupt:
        print("\nEncerrando...")
        logger.info("Encerrando por KeyboardInterrupt")

    except Exception as e:
        logger.error(f"Erro inesperado na captura: {e}")
        raise

    finally:
        # Fecha/flush arquivos e stream
        try:
            safe_fsync(log_diario)
            log_diario.close()
        except Exception:
            pass

        try:
            if wf_diario is not None:
                wf_diario.close()
        except Exception:
            pass

        try:
            stream.stop_stream()
            stream.close()
        except Exception:
            pass

        try:
            p.terminate()
        except Exception:
            pass


# ----------------------------
# main
# ----------------------------
def main():
    try:
        logger.info("Código microfone iniciado")
        capturar_audio()
    except Exception as e:
        logger.error(f"Erro crítico: {e}. Reiniciando processo...")
        # reinicia o processo Python 
        os.execv(sys.executable, ["python3"] + sys.argv)


if __name__ == "__main__":
    main()
