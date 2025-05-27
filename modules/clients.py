import datetime, json, sqlite3

def view_clients(node):
  """Muestra la lista de clientes"""
  try:
    conn = sqlite3.connect(node.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, email, phone, last_updated_at FROM clients")
    rows = cursor.fetchall()
    conn.close()

    print("\nLista de clientes:")
    print("=" * 40)
    for row in rows:
      print(f"ID: {row[0]}, Name: {row[1]}, Email: {row[2]}, Phone: {row[3]}, Last Updated: {row[4]}")
  except Exception as e:
    print(f"[Nodo {node.id_node}] Error leyendo clientes disponibles: {e}")

def propagate_client_update(node, client_id, name, phone, email):
  """Propaga la actualización de un cliente a los demás nodos"""
  update_message = {
    'client_id': client_id,
    'name': name,
    'phone': phone,
    'email': email,
    'last_updated_at': datetime.now().isoformat(),
  }

  for node_id, ip in node.neighbours.items():
    try:
      msg = {
        'type': 'CLIENT_UPDATE',
        'origin': node.id_node,
        'destination': node_id,
        'content': json.dumps(update_message),
        'timestamp': datetime.now().isoformat(),
      }
      if node.send_message(msg):
        print(f"[Nodo {node.id_node}] Actualizacion de cliente enviada al Nodo {node_id - node.base_port}")
    except Exception as e:
      print(f"[Nodo {node.id_node}] Error enviando actualizacion de cliente al Nodo {node_id - node.base_port}: {e}")

def add_client(node, name, phone, email):
  """Agrega un cliente a la base de datos y propaga la actualización a otros nodos"""
  try:
    conn = sqlite3.connect(node.db_name)
    cursor = conn.cursor()
    # Insertar cliente localmente
    cursor.execute("""
      INSERT INTO clients (name, phone, email, last_updated_at)
      VALUES (?, ?, ?, ?)
    """, (name, phone, email, datetime.now().isoformat()))
    conn.commit()
    client_id = cursor.lastrowid  # Obtener el ID del cliente recién agregado
    conn.close()
    print(f"[Nodo {node.id_node}] Cliente agregado: {name}")

    # Propagar la actualización a otros nodos
    propagate_client_update(client_id, name, phone, email)
  except Exception as e:
    print(f"[Nodo {node.id_node}] Error agregando cliente: {e}")

def add_client_ui(node):
  """Interfaz para agregar un cliente"""
  try:
    name = input("Enter client name: ").strip()
    email = input("Enter client email: ").strip()
    phone = input("Enter client phone: ").strip()
    add_client(node, name, email, phone)
  except Exception as e:
    print(f"Error: {e}")

def handle_client_update(node, message):
  """Maneja una actualización de cliente recibida de otro nodo"""
  try:
    client_id = message['client_id']
    name = message['name']
    email = message['email']
    phone = message['phone']
    last_update = message['last_updated_at']

    conn = sqlite3.connect(node.db_name)
    cursor = conn.cursor()

    # Verificar si el cliente ya existe
    cursor.execute("SELECT id FROM clients WHERE id = ?", (client_id,))
    result = cursor.fetchone()

    if result:
      # Actualizar cliente existente
      cursor.execute("""
        UPDATE clients
        SET name = ?, phone = ?, email = ?, last_updated_at = ?
        WHERE id = ?
      """, (name, phone, email, last_update, client_id))
      print(f"[Nodo {node.id_node}] Cliente {client_id} actualizado.")
    else:
      # Insertar nuevo cliente
      cursor.execute("""
        INSERT INTO clients (id, name, phone, email, last_updated_at)
        VALUES (?, ?, ?, ?, ?)
      """, (client_id, name, phone, email, last_update))
      print(f"[Nodo {node.id_node}] Cliente {client_id} agregado.")
    conn.commit()
    conn.close()
  except Exception as e:
    print(f"[Nodo {node.id_node}] Error durante actualizacion de cliente: {e}")
