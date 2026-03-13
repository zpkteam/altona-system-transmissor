import json
import os
import numpy as np
import time
from datetime import datetime, time as dtime
from logger import configurar_logger

logger = configurar_logger("construcaoJson")

CONFIG_PATH = "/home/altona/altona-system/config/config.json"

def obter_limite_atual():
    agora = datetime.now().time()

    if dtime(7, 0) <= agora <= dtime(22, 0):
        return 60  # das 07:00 às 22:00
    else:
        return 55  # das 22:01 às 06:59

def calc_media_dp(valores_db):
    
    valores_med =[np.mean(valores_db[i:i+300]) for i in range(0, len(valores_db), 300)]
    dp = np.std(valores_db)
    valores_med = np.round(valores_med, 2)
    valores_med = [float(m) for m in valores_med]
    
    return valores_med, dp

def ler_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)
    
def construir_json():
    data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    valores_db = []

    try:
        for tent in range(3):
            print("Tentando abrir arquivo")
            try:
                with open("/home/altona/altona-system/data/medicoesDiarias/log.txt", "r") as f:
                    for linha in f:
                        partes = linha.strip().split(";")
                        if len(partes) == 2:
                            try:
                                data, inicio_leitura = partes[0].split(" ")
                                valor_db = float(partes[1])
                                valores_db.append(valor_db)
                            except ValueError:
                                pass
                break
            except FileNotFoundError:
                print("Tentativa falha de abrir arquivo")
                time.sleep(0.1)
            
        now = datetime.now()
        date = now.strftime("%d_%m_%Y")
        os.rename(
            "/home/altona/altona-system/data/medicoesDiarias/log.txt", f"/home/altona/altona-system/data/medicoesDiarias/medicao_{date}.txt"
        )
        
        limite_atual = obter_limite_atual()
        
        if valores_db:
            media = sum(valores_db) / len(valores_db)
            limite = any(db > limite_atual for db in valores_db)
            
            valores_med, desvio_padrao = calc_media_dp(valores_db)
            
            config = ler_config()
            id_ = config["node"]

            evento = {
                "id": id_,
                "t": inicio_leitura,
                "dB": round(media, 2),
                "values[s]": valores_med,
                "std": round(desvio_padrao, 2), 
                #"f": f"audio_diario_{date}.wav", -> Verificar a necessidade disso tendo em vista que são gerados 24 arquivos por dia, talvez mandar um cabeçalho de data ao inves de arquivo wav
                "l": limite,
            }

            mensagem = json.dumps(evento)

            with open("/home/altona/altona-system/logs/JSON.txt", "w") as f:
                f.write(mensagem + "\n")

            logger.info("Construção JSON finalizada")

    except Exception as e:
        print(f"Erro ao construir JSON: {e}")
        logger.critical(f"Erro ao construir JSON: {e}")


if __name__ == "__main__":
    construir_json()
