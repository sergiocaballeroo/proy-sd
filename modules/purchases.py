import sqlite3

def show_purchases(node):
  """Muestra lista de compras aplicadas en la sucursal."""
  try:
    conn = sqlite3.connect(node.db_name)
    cursor = conn.cursor()
    cursor.execute("" \
      "SELECT purchase_id, item_id, branch_id, client_id, quantity, delivery_guide, created_at FROM purchases" \
    "")
    rows = cursor.fetchall()
    conn.close()

    print("\nCompras en esta sucursal:")
    print("=" * 40)
    for row in rows:
      print(f"ID Compra: {row[0]}, Producto: {row[1]}, Sucursal: {row[2]}, Cliente: {row[3]}, Cantidad: {row[4]}, Guia de envio: {row[5]}, Fecha de creacion: {row[6]}")
  except Exception as e:
    print(f"[Nodo {node.id_node}] Error obteniendo lista de compras: {e}")

def create_purchase_ui(node):
  """Interfaz para comprar un artículo con exclusión mutua"""
  try:
    item_id = int(input("Enter the item ID to purchase: "))
    quantity = int(input("Enter the quantity to purchase: "))

    # Define la lógica de compra como un método interno
    def purchase_item():
      if node._update_inventory(item_id, -quantity):
        print(f"Se compraron {quantity} unidades del articulo {item_id}.")
      else:
        print(f"Error comprando unidades del articulo {item_id}.")

    # Asigna la lógica de compra al atributo `node.purchase_item`
    node.purchase_item = purchase_item

    # Solicitar acceso a la sección crítica
    node.request_critical_section()
  except ValueError:
    print("Entrada invalida. Por favor ingresa valores numericos.")