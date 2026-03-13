import os
import time
import subprocess
import json
from datetime import datetime
from enum import Enum
from logger import configurar_logger

logger = configurar_logger("Observer")

CONFIG_PATH = "/home/altona/altona-system/config/config.json"
STATE_FILE = "/home/altona/altona-system/state/state.txt"
CONSTRUCAOJSON_FILE = "/home/altona/altona-system/src/construcaoJson.py"
GERENCIADORMEMORIA_FILE = "/home/altona/altona-system/src/gerenciadorDeMemoria.py"

def verifica_hora(h, m):
	now = datetime.now()
	return (now.hour, now.minute) == (h, m)
	
def ler_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)
	
#================== Funções para gestão de serviços (bloqueantes) ===============
	
def _systemctl(args):
	return subprocess.run(["sudo", "systemctl"] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def start_service(service: str):
	_systemctl(["start", service])

def stop_service(service: str):
	_systemctl(["stop", service])

def restart_service(service: str):
	_systemctl(["restart", service])

def service_state(service: str) -> str:
	result = _systemctl(["show", service, "-p", "ActiveState", "--value"])
	return result.stdout.strip()
	
#===============================================================================


class State(Enum):
	COLLECTING = 1 #-> Microfone Escrevendo
	ROTATING = 2 #-> Rotaciondo log 
	JSON_PENDING = 3 #-> Log encerrado, JSON nao existe
	READY_TO_SEND = 4 #-> JSON	pronto, aguardando horario de envio
	INICIALIZATION = 5 #-> Inicialização Raspberry
	ALERT = 6 #-> Alerta de Ruído

def detectar_estado():
	#Estado inferido por artefatos
	if os.path.exists("/home/altona/altona-system/Flags/ALERT_FLAG"):
		return State.ALERT
		
	if os.path.exists("/home/altona/altona-system/logs/JSON.txt"):
		return State.READY_TO_SEND
	
	if os.path.exists("/home/altona/altona-system/data/medicoesDiarias/log.txt"):
		return State.JSON_PENDING
	
	if os.path.exists("/home/altona/altona-system/logs/log.txt"):
		return State.COLLECTING
	
	return State.INICIALIZATION

def verifica_sistema():
	status_microfone = service_state("microfone")
	
	status_observer = service_state("observer")
	
	status_emissor = service_state("emissor")

def main():
	estado_ant = None
	
	config = ler_config()
	
	h = config["h"]
	m = config["m"]
	
	while True:
		# Verifica o sistema para saber o estado
		estado = detectar_estado()
		
		if estado != estado_ant:
			with open (STATE_FILE, 'w') as f:
				f.write(f"State: {estado.name}\n")
				update = datetime.now().strftime("%Y/%m/%d %H:%M")
				f.write(f"Last updated: {update}")
			estado_ant = estado 
		
		if estado == State.INICIALIZATION:
			#verifica_sistema()
			start_service("microfone")
			#logger.info("Microfone service iniciado")
		
		if estado == State.COLLECTING:
			if verifica_hora(23, 59):
				#1 - Criar flag rotate
				if not os.path.exists("/home/altona/altona-system/Flags/ROTATE_FLAG"):
					open("/home/altona/altona-system/Flags/ROTATE_FLAG", "w").close()
				
				#2 - Chamar o gerenciador de memoria -> Processo não bloqueante
				subprocess.run(["python3", GERENCIADORMEMORIA_FILE])
				#logger.info("Gerenciador de memória executado")
			
			status_microfone = service_state("microfone")
			
			if status_microfone != "active":
				start_service("microfone")
				#logger.warning("Iniciado Microfone Service após interrupção")
		
		
		if estado == State.JSON_PENDING:
			#1 - Reiniciar código do microfone
			restart_service("microfone")
			#logger.info("Reiniciando Microfone Service")
			
			#2 - Chama o construtorJSon -> Processo não bloqueante
			subprocess.run(["python3", CONSTRUCAOJSON_FILE])
			#logger.info("ConstrucaoJSon executado")
			
		if estado == State.READY_TO_SEND:
			if verifica_hora(h,m):
				#1- Chama o emissor
				start_service("emissor")
				logger.info("Emissor Service iniciado")
				
		if estado == State.ALERT:
			status_emissor = service_state("emissor")
			if status_emissor == "active":
				logger.warning("Prioridade: Parando envio do Diário para iniciar Alerta.")
				if not os.path.exists("/home/altona/altona-system/Flags/INTERRUPT_FLAG"):
					open("/home/altona/altona-system/Flags/INTERRUPT_FLAG", "w").close()
				
				if os.path.exists("/home/altona/altona-system/state/emissor_state.json"):
					stop_service("emissor")
		
			
			status_alert = service_state("alert")
			if status_alert =! "active":
   				start_service("alerta")
			if status_alert == "inactive":
				os.remove("/home/altona/altona-system/Flags/ALERT_FLAG")
				os.remove("/home/altona/altona-system/Flags/INTERRUPT_FLAG")
				if estado_ant.name == "READY_TO_SEND" and service_state("emissor") != "active":
					start_service("emissor")
				#logger.info("Alerta service finalizado")
				
	time.sleep(2)
		
if __name__ == "__main__":
    main()
