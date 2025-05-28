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
    # Por defecto, se coloca ID 1 al nodo.
    return 1

import subprocess

def ping_ip(ip):
  """
  Realiza un ping a una IP y devuelve True si responde, False si no.
  El ping solo envia 2 paquetes como maximo y con una espera de 1 segundos.
  """
  try:
    result = subprocess.run(["ping", "-c", "1", "-W", "1", ip], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0
  except:
    return result.returncode == 0

def update_neighbours(requester_ip, ips: dict):
  """
  Funcion encargada de verificar a que vecinos tiene acceso un nodo al
  comienzo de su ejecucion.
  """
  neighbours = {}
  for node_id, ip in ips.items():
    if ip != requester_ip and ping_ip(ip):
      neighbours[node_id] = ip
  return neighbours

import json
def is_valid_json(text: str):
  try:
    json.loads(text)
    return True
  except json.JSONDecodeError:
    return False

def divide_list(lista, num_parts):
  k, m = divmod(len(lista), num_parts)
  return [lista[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(num_parts)]
