import os
import shutil
from logger import configurar_logger

logger = configurar_logger("GerenciadorMemoria")

def limpar_pastas_antigas(caminho_base, manter=3):

	pastas = [
				os.path.join(caminho_base, p)
				for p in os.listdir(caminho_base)
				if os.path.isdir(os.path.join(caminho_base, p))
			 ]
	
	# Orde da mais antiga para a mais nova (mtime)
	pastas.sort(key=os.path.getmtime)
	
	if len(pastas) > manter:
		pastas_remover = pastas[:-manter]
		for pasta in pastas_remover:
			shutil.rmtree(pasta, ignore_errors=True)
	
	logger.info("Sistema Limpo")
	

		

limpar_pastas_antigas("/home/altona/altona-system/data/gravacaoDiaria/", manter=3)
