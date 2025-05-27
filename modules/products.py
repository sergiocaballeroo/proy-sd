import sqlite3, datetime, json

def show_products(node):
    """Muestra el los productos registrados en nodo actual"""
    try:
        conn = sqlite3.connect(node.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, category, price FROM items")
        rows = cursor.fetchall()
        conn.close()

        print("\nProductos:")
        print("=" * 40)
        for row in rows:
            print(f"ID: {row[0]}, Name: {row[1]}, Category: {row[2]}, Price: {row[3]}")
    except Exception as e:
        print(f"[Node {node.id_node}] Error durante el listado de productos: {e}")

def create_product(node, product_data):
    """Funcion encargada de crear un producto y enviar su cambio al resto de nodos."""
    try:
        conn = sqlite3.connect(node.db_name)
        cursor = conn.cursor()
        cursor.execute("" \
            "INSERT INTO items (name, category, price, stock, tax_rate) " \
            "VALUES (?, ?, ?, ?, ?)"
        , (product_data['name'], product_data['category'], product_data['price'], product_data['stock'], 0.16))
        conn.commit()

        print(f"\nProducto registrado con ID: {cursor.lastrowid}")
        print("=" * 40)
        cursor.execute("SELECT id, name, category, price FROM items WHERE id = ?", (cursor.lastrowid,))
        product = cursor.fetchone()
        if product:
            print(f"ID: {product[0]}, Name: {product[1]}, Category: {product[2]}, Price: {product[3]}")
        conn.close()
    except Exception as e:
        print(f"[Node {node.id_node}] Error durante la creacion del producto: {e}")

def create_product_ui(node):
    try:
        name = input("Nombre del producto: ")
        category = input("Categoria: ")
        price = float(input("Precio unitario: "))
        stock = int(input("Unidades disponibles: "))

        product_data = { 'name': name, 'category': category, 'price': price, 'stock': stock}
        create_product(node, product_data)
    except ValueError:
        print("Entrada incorrecta. Por favor intenta de nuevo.")

def update_product(node, product_data):
    """Muestra el los productos registrados en nodo actual"""
    try:
        conn = sqlite3.connect(node.db_name)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE items SET name = ?, category = ?,  price = ? WHERE id = ?"
        , (product_data['name'], product_data['category'], product_data['price'], product_data['id']))
        conn.commit()

        print(f"\nProducto actualizado con ID: {product_data['id']}")
        print("=" * 40)
        cursor.execute("SELECT id, name, category, price FROM items WHERE id = ?", (product_data['id'],))
        product = cursor.fetchone()
        if product:
            print(f"ID: {product[0]}, Name: {product[1]}, Category: {product[2]}, Price: {product[3]}")
        conn.close()
    except Exception as e:
        print(f"[Node {node.id_node}] Error durante la actualizacion del producto: {e}")

def update_product_ui(node):
    try:
        product_id = input("ID del producto a actualizar: ")
        name = input("Nombre del producto: ")
        category = input("Categoria: ")
        price = float(input("Precio unitario: "))

        product_data = { 'name': name, 'category': category, 'price': price, 'id': product_id}
        update_product(node, product_data)
    except ValueError:
        print("Entrada incorrecta. Por favor intenta de nuevo.")

def distribute_items(node, item_id, total_quantity):
    """Distribuye automáticamente los artículos entre las sucursales"""
    if not node.is_master:
        print(f"[Nodo {node.id_node}] Error: Solo el nodo maestro puede distribuir articulos.")
        return

    print(f"[Nodo {node.id_node}] Comenzando distribuicion del articulo {item_id} con un total de {total_quantity}.")

    # Obtener la capacidad actual de cada nodo
    capacities = {}
    for port, ip in node.neighbours.items():
        try:
            message = {
                'type': 'GET_CAPACITY',
                'item_id': item_id,
                'origin': node.id_node,
                'timestamp': datetime.now().isoformat()
            }
            if node.send_message({'destination': port, 'content': json.dumps(message)}):
                print(f"[Nodo {node.id_node}] Capacidad solicitada desde el Nodo {port - node.base_port}")
        except Exception as e:
            print(f"[Nodo {node.id_node}] Error solicitando capacidad desde el Nodo {port - node.base_port}: {e}")

    # Simular capacidades (en un entorno real, esto se recibiría como respuesta)
    capacities = {port: 100 for port in node.neighbours.keys()}  # Ejemplo: cada nodo tiene capacidad de 100

    # Calcular la distribución equitativa
    total_nodes = len(capacities)
    base_quantity = total_quantity // total_nodes
    remainder = total_quantity % total_nodes

    # Distribuir los artículos
    for port, capacity in capacities.items():
        quantity_to_send = base_quantity + (1 if remainder > 0 else 0)
        if remainder > 0:
            remainder -= 1

        # Enviar actualización de inventario al nodo
        update_message = {
            'type': 'INVENTORY_UPDATE',
            'item_id': item_id,
            'new_quantity': quantity_to_send,
            'origin': node.id_node,
            'timestamp': datetime.now().isoformat()
        }
        try:
            if node.send_message({'destination': port, 'content': json.dumps(update_message)}):
                print(f"[Nodo {node.id_node}] Se enviaron {quantity_to_send} unidades del articulo {item_id} al Nodo {port - node.base_port}")
        except Exception as e:
            print(f"[Nodo {node.id_node}] Error enviando actualizacion del inventario al nodo {port - node.base_port}: {e}")

    print(f"[Nodo {node.id_node}] Resumen de la distribuicion:")
    for port, quantity in capacities.items():
        print(f"  - Nodo {port - node.base_port}: {quantity} unidades")

    print(f"[Nodo {node.id_node}] Distribuicion del articulo {item_id} completada.")

def distribute_items_ui(node):
    """Interfaz para distribuir artículos"""
    try:
        item_id = int(input("Ingresa el ID del articulo a distribuir: "))
        total_quantity = int(input("Cantidad de unidades a distribuir: "))
        distribute_items(item_id, total_quantity)
    except ValueError:
        print("Entrada invalida. Por favor ingresa solo valores numericos.")