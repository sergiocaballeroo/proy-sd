import sqlite3

def show_products(self):
    """Muestra el los productos registrados en nodo actual"""
    try:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, category, price FROM items")
        rows = cursor.fetchall()
        conn.close()

        print("\nProductos:")
        print("=" * 40)
        for row in rows:
            print(f"ID: {row[0]}, Name: {row[1]}, Category: {row[2]}, Price: {row[3]}")
    except Exception as e:
        print(f"[Node {self.id_node}] Error durante el listado de productos: {e}")

def create_product(self, product_data):
    """Funcion encargada de crear un producto y enviar su cambio al resto de nodos."""
    try:
        conn = sqlite3.connect(self.db_name)
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
        print(f"[Node {self.id_node}] Error durante la creacion del producto: {e}")

def update_product(self, product_data):
    """Muestra el los productos registrados en nodo actual"""
    try:
        conn = sqlite3.connect(self.db_name)
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
        print(f"[Node {self.id_node}] Error durante la actualizacion del producto: {e}")