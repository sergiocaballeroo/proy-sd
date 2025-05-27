import sqlite3, datetime, json

def show_inventory(node):
  """Muestra el inventario local"""
  try:
    conn = sqlite3.connect(node.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT id, item_id, quantity, last_updated_at FROM branch_stock")
    rows = cursor.fetchall()
    conn.close()

    print("\nInventario local:")
    print("=" * 40)
    for row in rows:
      print(f"ID: {row[0]}, Name: {row[1]}, Quantity: {row[2]}, Price: {row[3]}, Last Updated: {row[4]}")
  except Exception as e:
    print(f"[Nodo {node.id_node}] Error obteniendo inventario: {e}")

def get_item_quantity(node, item_id):
  """Obtiene la cantidad actual de un artículo en el inventario local"""
  try:
    conn = sqlite3.connect(node.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT quantity FROM branch_stock WHERE id = ?", (item_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
      return result[0]
    return 0
  except Exception as e:
    print(f"[Nodo {node.id_node}] Error obteniendo numero de unidades del articulo {item_id}: {e}")
    return 0

def propagate_inventory_update(node, item_id, new_quantity):
  """Propaga la actualización de inventario a los demás nodos y espera confirmaciones"""
  confirmations = 1  # Ya está confirmado localmente
  total_nodes = len(node.neighbours) + 1  # Incluye este nodo

  update_message = {
    'type': 'INVENTORY_UPDATE',
    'item_id': item_id,
    'new_quantity': new_quantity,
    'origin': node.id_node,
    'timestamp': datetime.now().isoformat()
  }

  for port, ip in node.neighbours.items():
    try:
      msg = {
        'destination': port,
        'content': json.dumps(update_message)
      }
      if node.send_message(msg):
        confirmations += 1
    except Exception as e:
      print(f"[Nodo {node.id_node}] Error al enviar actualizacion de inventario al Nodo {port - node.base_port}: {e}")

  # Consenso simple: mayoría
  if confirmations >= (total_nodes // 2) + 1:
    print(f"[Nodo {node.id_node}] Actualizacion de inventario confirmada por mayoria ({confirmations}/{total_nodes})")
    return True
  else:
    print(f"[Nodo {node.id_node}] Actualizacion de inventario NO confirmada por mayoria ({confirmations}/{total_nodes})")
    return False

def update_inventory(node, item_id, quantity_change, propagate=True):
  """Actualiza la cantidad de un artículo en el inventario y propaga el cambio si es necesario"""
  try:
    conn = sqlite3.connect(node.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT quantity FROM branch_stock WHERE item_id = ?", (item_id,))
    result = cursor.fetchone()
    if result:
      new_quantity = result[0] + quantity_change
      if new_quantity < 0:
        print(f"[Nodo {node.id_node}] Error: No hay unidades suficientes del articulo {item_id}")
        return False
      cursor.execute("""
        UPDATE branch_stock
        SET quantity = ?, last_updated_at = ?
        WHERE item_id = ?
      """, (new_quantity, datetime.now().isoformat(), item_id))
      conn.commit()
      print(f"[Nodo {node.id_node}] Inventario actualizado para el articulo {item_id}")

      # Propaga la actualización si es necesario
      if propagate:
        success = node._propagate_inventory_update(item_id, new_quantity)
        if not success:
          print(f"[Nodo {node.id_node}] Descartando cambios en inventario para el articulo {item_id}")
          # Rollback: restaurar cantidad anterior
          cursor.execute("""
            UPDATE branch_stock
            SET quantity = ?, last_updated_at = ?
            WHERE item_id = ?
          """, (result[0], datetime.now().isoformat(), item_id))
          conn.commit()
          conn.close()
          return False
    else:
      print(f"[Nodo {node.id_node}] Error: No se encontro inventario del articulo {item_id}.")
    conn.close()
    return True
  except Exception as e:
    print(f"[Nodo {node.id_node}] Error actualizando inventario: {e}")
    return False

def add_item_inventory(node, item_id, quantity):
  """Agrega un producto al inventario y propaga la actualización a otros nodos"""
  try:
    conn = sqlite3.connect(node.db_name)
    cursor = conn.cursor()
    # Insertar cliente localmente
    cursor.execute("""
      INSERT INTO branch_stock (branch_id, item_id, quantity, last_updated_at)
      VALUES (?, ?, ?, ?)
    """, (node.id_node, item_id, quantity, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    print(f"[Nodo {node.id_node}] Producto agregado: {item_id}")

    # TODO: Propagar la actualización a otros nodos
  except Exception as e:
      print(f"[Nodo {node.id_node}] Error agregando producto al inventario: {e}")

def update_inventory_ui(node):
  """Maneja la inserción de items al inventario"""
  try:
    item_id = input("Enter product id: ").strip()
    quantity = input("Enter quantity: ").strip()
    add_item_inventory(node, item_id, quantity)
  except Exception as e:
    print(f"Error: {e}")

def sync_inventory(node):
  """Sincroniza el inventario con otros nodos"""
  for port, ip in node.neighbours.items():
    try:
      message = {
        'origin': node.id_node,
        'destination': port,
        'content': 'SYNC_INVENTORY',
        'timestamp': datetime.now().isoformat()
      }
      if node.send_message(message):
        print(f"[Nodo {node.id_node}] Solicitud de sincronizacion enviada al Nodo {port - node.base_port}")
    except Exception as e:
      print(f"[Nodo {node.id_node}] Error sincronizando inventario con el Nodo {port - node.base_port}: {e}")
