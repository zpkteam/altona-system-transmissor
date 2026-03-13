import logging
import os
from datetime import datetime


# --- CLASSE DO FILTRO (NOVA) ---
class NameFilter(logging.Filter):
    """
    Filtro para limpar o nome do logger.
    Transforma 'LOGGER_SISTEMA_ALTONA.Microfone' em apenas 'Microfone'.
    """

    def filter(self, record):
        if "." in record.name:
            # Pega apenas o texto depois do último ponto
            record.display_name = record.name.split(".")[-1]
        else:
            record.display_name = record.name
        return True


# --- SEU HANDLER DE PASTAS (O MESMO DE ANTES) ---
class DailyOrganizedHandler(logging.FileHandler):
    def __init__(self, base_dir, encoding="utf-8"):
        self.base_dir = base_dir
        self.encoding = encoding
        self.current_date = datetime.now().strftime("%d_%m_%Y")

        filename = self._get_filename_for_date(datetime.now())
        super().__init__(filename, mode="a", encoding=encoding, delay=True)

    def _get_filename_for_date(self, date_obj):
        month_folder = date_obj.strftime("%Y_%m")
        day_filename = f"log_{date_obj.strftime('%d_%m_%Y')}.log"
        full_dir_path = os.path.join(self.base_dir, month_folder)
        os.makedirs(full_dir_path, exist_ok=True)
        return os.path.join(full_dir_path, day_filename)

    def emit(self, record):
        try:
            new_date_obj = datetime.now()
            new_date_str = new_date_obj.strftime("%d_%m_%Y")

            if new_date_str != self.current_date:
                self.acquire()
                try:
                    if new_date_str != self.current_date:
                        self._rollover(new_date_obj, new_date_str)
                finally:
                    self.release()
            super().emit(record)
        except Exception:
            self.handleError(record)

    def _rollover(self, date_obj, date_str):
        self.close()
        self.current_date = date_str
        new_filename = self._get_filename_for_date(date_obj)
        self.baseFilename = new_filename
        self.stream = None


# --- FUNÇÃO DE CONFIGURAÇÃO (ATUALIZADA) ---
def configurar_logger(nome_modulo="main"):
    base_logs_dir = "/home/altona/altona-system/data/logs"

    root_logger = logging.getLogger("LOGGER_SISTEMA_ALTONA")
    root_logger.setLevel(logging.INFO)

    if not root_logger.handlers:
        handler = DailyOrganizedHandler(base_logs_dir)

        # ADICIONAMOS O FILTRO AQUI
        # Isso força o handler a limpar o nome antes de escrever
        handler.addFilter(NameFilter())

        # O formato continua usando %(name)s, mas agora ele estará limpo
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(display_name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        root_logger.addHandler(handler)

        # Configura o console também com o filtro, se quiser
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        console.addFilter(NameFilter())
        root_logger.addHandler(console)

    return root_logger.getChild(nome_modulo)
