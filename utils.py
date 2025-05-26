import socket
def get_static_ip():
  # Genera un socket momentáneo para obtener la IP con la que el ping saldría.
  try:
    # Crea un socket "falso" para obtener la IP de salida
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))  # No envía datos, solo abre conexión
    ip = s.getsockname()[0]
    s.close()
    return ip
  except Exception as e:
    return f"Error al obtener IP: {e}"
