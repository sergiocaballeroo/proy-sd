"""
Communication System between Nodes - Versión Corregida
Implementación de un sistema distribuido con confirmación de mensajes
"""
import os
import socket
import threading
import json
from datetime import datetime
import sqlite3
import time
from pathlib import Path
from db_utils import ensure_schema

class Node:
    def __init__(self, id_node, port, nodes_info, node_ip='0.0.0.0', server_ready_event=None, base_port=5000,db_path = "/home/axelf/node_1.db", base_dir="/home/axelf/tables"):
        """
        Args:
            id_node: Identificador único del nodo (1, 2, 3...)
            port: Puerto de escucha
            nodes_info: Diccionario {puerto: ip} de nodos disponibles
            node_ip: IP del nodo
            server_ready_event: Evento para sincronización
            base_port: Puerto base para cálculo de IDs
        """
        self.id_node = id_node
        self.port = port
        self.ip = node_ip
        self.nodes_info = nodes_info
        self.messages = []
        self.server = None
        self.server_ready_event = server_ready_event
        self.base_port = base_port
        self.db_name = f"node_{self.id_node}.db"
        self._init_db()
        self.clock = 0  # Reloj lógico Lamport
        self.request_queue = []  # Cola de solicitudes pendientes
        self.in_critical_section = False  # Indica si el nodo está en la sección crítica
        self.replies_received = 0  # Contador de respuestas REPLY
        db_path = Path(db_path).expanduser().resolve()
        ensure_schema(db_path=db_path, data_dir=base_dir)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)

    def increment_clock(self):
        """Incrementa el reloj lógico"""
        self.clock += 1

    def synchronize_clock(self, received_clock):
        """Sincroniza el reloj lógico con un valor recibido"""
        self.clock = max(self.clock, received_clock) + 1

    def request_critical_section(self, timeout=5):
        """Solicita acceso a la sección crítica con un timeout"""
        self.increment_clock()
        self.in_critical_section = True
        self.replies_received = 0
        self.pending_replies = len(self.nodes_info)  # Número de nodos de los que se espera respuesta

        # Enviar mensaje REQUEST a todos los nodos
        for port, ip in self.nodes_info.items():
            message = {
                'type': 'REQUEST',
                'clock': self.clock,
                'origin': self.id_node,
                'timestamp': datetime.now().isoformat()
            }
            if self.send_message({
                'destination': port,
                'content': json.dumps(message)
            }):
                print(f"[Node {self.id_node}] Sent REQUEST to Node {port - self.base_port}")
            else:
                print(f"[Node {self.id_node}] Failed to send REQUEST to Node {port - self.base_port}")
                self.pending_replies -= 1  # Reducir el número de respuestas esperadas si el nodo no está disponible

        # Esperar respuestas con un timeout
        start_time = time.time()
        while self.replies_received < self.pending_replies:
            if time.time() - start_time > timeout:
                print(f"[Node {self.id_node}] Timeout waiting for replies. Aborting critical section request.")
                print(f"[Node {self.id_node}] Received {self.replies_received} replies out of {self.pending_replies} expected.")
                self.in_critical_section = False
                return  # Abortamos si no recibimos suficientes respuestas
            time.sleep(0.1)  # Esperar un breve momento antes de verificar nuevamente

        # Si se recibieron suficientes respuestas, entrar en la sección crítica
        if self.replies_received >= self.pending_replies:
            print(f"[Node {self.id_node}] Received all necessary replies. Entering critical section.")
            self.enter_critical_section()

    def handle_request(self, message):
        """Maneja un mensaje REQUEST recibido"""
        self.synchronize_clock(message['clock'])
        origin = message['origin']

        # Responder con REPLY si no estoy en la sección crítica o si mi solicitud tiene menor prioridad
        if not self.in_critical_section or (self.clock, self.id_node) > (message['clock'], origin):
            reply_message = {
                'type': 'REPLY',
                'clock': self.clock,
                'origin': self.id_node,
                'timestamp': datetime.now().isoformat()
            }
            self.send_message({
                'destination': self.base_port + origin,
                'content': json.dumps(reply_message)
            })
            print(f"[Node {self.id_node}] Sent REPLY to Node {origin}.")
        else:
            # Agregar la solicitud a la cola
            self.request_queue.append(message)

    def handle_reply(self, message):
        """Maneja un mensaje REPLY recibido"""
        self.synchronize_clock(message['clock'])
        self.replies_received += 1
        print(f"[Node {self.id_node}] Received REPLY from Node {message['origin']}. Total replies: {self.replies_received}")
    
    def enter_critical_section(self):
        """Entra en la sección crítica"""
        print(f"[Node {self.id_node}] In critical section.")
        # Aquí puedes realizar la operación crítica (por ejemplo, comprar un artículo)
        self.purchase_item()

        # Salir de la sección crítica
        self.exit_critical_section()

    def exit_critical_section(self):
        """Sale de la sección crítica"""
        print(f"[Node {self.id_node}] Exiting critical section.")
        self.in_critical_section = False

        # Responder a las solicitudes pendientes en la col
        while self.request_queue:
            pending_request = self.request_queue.pop(0)
            self.handle_request(pending_request)

    def _purchase_item_ui(self):
        """Interfaz para comprar un artículo con exclusión mutua"""
        try:
            item_id = int(input("Enter the item ID to purchase: "))
            quantity = int(input("Enter the quantity to purchase: "))

            # Define la lógica de compra como un método interno
            def purchase_item():
                if self.update_inventory(item_id, -quantity):
                    print(f"Purchased {quantity} of item {item_id}.")
                else:
                    print(f"Failed to purchase item {item_id}.")

            # Asigna la lógica de compra al atributo `self.purchase_item`
            self.purchase_item = purchase_item

            # Solicitar acceso a la sección crítica
            self.request_critical_section()

        except ValueError:
            print("Invalid input. Please enter numeric values.")

    def propagate_inventory_update(self, item_id, new_quantity):
        """Propaga la actualización de inventario a los demás nodos y espera confirmaciones"""
        confirmations = 1  # Ya está confirmado localmente
        total_nodes = len(self.nodes_info) + 1  # Incluye este nodo

        update_message = {
            'type': 'INVENTORY_UPDATE',
            'item_id': item_id,
            'new_quantity': new_quantity,
            'origin': self.id_node,
            'timestamp': datetime.now().isoformat()
        }

        for port, ip in self.nodes_info.items():
            try:
                msg = {
                    'destination': port,
                    'content': json.dumps(update_message)
                }
                if self.send_message(msg):
                    confirmations += 1
            except Exception as e:
                print(f"[Node {self.id_node}] Error sending inventory update to Node {port - self.base_port}: {e}")

        # Consenso simple: mayoría
        if confirmations >= (total_nodes // 2) + 1:
            print(f"[Node {self.id_node}] Inventory update confirmed by majority ({confirmations}/{total_nodes})")
            return True
        else:
            print(f"[Node {self.id_node}] Inventory update NOT confirmed by majority ({confirmations}/{total_nodes})")
            return False

            
    def start_server(self):
        """Inicia el servidor TCP para recibir mensajes"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self.server = s
            s.bind((self.ip, self.port))
            s.listen()
            print(f"Node {self.id_node} Server is ready and listening on {self.ip}:{self.port}")
            if self.server_ready_event:
                self.server_ready_event.set()

            while True:
                try:
                    conn, addr = s.accept()
                    threading.Thread(
                        target=self.handle_connection,
                        args=(conn, addr),
                        daemon=True
                    ).start()
                except Exception as e:
                    print(f"[Node {self.id_node}] Server error: {e}")

    def _init_db(self):
        """Inicializa la base de datos y crea las tablas si no existen"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            # Crear tabla de mensajes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    origin INTEGER,
                    destination INTEGER,
                    content TEXT,
                    timestamp TEXT
                )
            """)
            # Crear tabla de inventario
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inventory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    quantity INTEGER,
                    price REAL,
                    last_updated TEXT
                )
            """)
            conn.commit()
            conn.close()
            print(f"[Node {self.id_node}] Database initialized.")
        except Exception as e:
            print(f"[Node {self.id_node}] DB init error: {e}")

    def handle_connection(self, conn, addr):
        """Handles incoming connections"""
        with conn:
            try:
                # Receive and decode data
                data = conn.recv(1024).decode()
                if not data:
                    return

                # Parse main message
                try:
                    message = json.loads(data)
                except json.JSONDecodeError:
                    print(f"[Node {self.id_node}] Invalid JSON message: {data}")
                    return

                # Process content field
                content = message.get('content', '')
                content_data = None

                # Parse content if it's a JSON string
                if isinstance(content, str):
                    try:
                        content_data = json.loads(content)
                        message['content'] = content_data  # Replace with parsed dict
                    except json.JSONDecodeError:
                        content_data = content  # Keep as string
                elif isinstance(content, dict):
                    content_data = content

                # Extract message details
                msg_type = content_data.get('type') if content_data else None
                origin = content_data.get('origin') if content_data else None

                # Store parsed message (with content as dict)
                self.messages.append(message)
                self._save_message_to_db(message)

                # Print formatted message
                hour = datetime.fromisoformat(message['timestamp']).strftime("%H:%M:%S")
                print(f"[Node {self.id_node}] Received from {message['origin']} at {hour}: {content_data}")

                # Handle message types
                if msg_type == 'COORDINATOR':
                    self.handle_coordinator_message(content_data)

                elif msg_type == 'ELECTION':
                    self.handle_election_message(content_data)

                elif msg_type == 'REPLY':
                    self.handle_reply(content_data)

                elif msg_type == 'INVENTORY_UPDATE':
                    item_id = content_data.get('item_id')
                    new_quantity = content_data.get('new_quantity')
                    if item_id and new_quantity:
                        self.update_inventory(
                            item_id,
                            new_quantity - self.get_item_quantity(item_id),
                            propagate=False
                        )
                        print(f"[Node {self.id_node}] Inventory updated for item {item_id}")

                # Send ACK
                if not str(content).startswith("ACK:"):
                    ack_msg = {
                        'origin': self.id_node,
                        'destination': self.base_port + message['origin'],
                        'content': f"ACK: {json.dumps(content_data)}",
                        'timestamp': datetime.now().isoformat()
                    }
                    if self.send_message(ack_msg):
                        print(f"[Node {self.id_node}] ACK sent to {message['origin']}")
                    else:
                        print(f"[Node {self.id_node}] Failed to send ACK")

            except KeyError as ke:
                print(f"[Node {self.id_node}] Message format error: Missing key {ke}")
            except Exception as e:
                print(f"[Node {self.id_node}] Critical error: {str(e)}")

    def handle_coordinator_message(self, message):
        """Maneja mensajes COORDINATOR (nodo maestro)"""
        new_master_id = message['origin']

        # Verificar si ya se procesó este mensaje
        if hasattr(self, 'last_coordinator_message') and self.last_coordinator_message == message:
            return  # Ignorar mensajes repetidos

        # Actualizar el último mensaje procesado
        self.last_coordinator_message = message

        print(
            f"[Node {self.id_node}] 🟢 COORDINATOR ELECTED\n"
            f"   │ New Master: Node {new_master_id}\n"
            f"   │ Logical Clock: {message.get('clock', 'N/A')}\n"
            f"   │ Timestamp: {message['timestamp']}\n"
        )
        self.is_master = (self.id_node == new_master_id)
        self.current_master = new_master_id 

    # Debes agregar este método auxiliar en tu clase Node:
    def get_item_quantity(self, item_id):
        """Obtiene la cantidad actual de un artículo en el inventario local"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT quantity FROM inventory WHERE id = ?", (item_id,))
            result = cursor.fetchone()
            conn.close()
            if result:
                return result[0]
            return 0
        except Exception as e:
            print(f"[Node {self.id_node}] Error getting item quantity: {e}")
            return 0

    def show_inventory(self):
        """Muestra el inventario local"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT id, name, quantity, price, last_updated FROM inventory")
            rows = cursor.fetchall()
            conn.close()

            print("\nLocal Inventory:")
            print("=" * 40)
            for row in rows:
                print(f"ID: {row[0]}, Name: {row[1]}, Quantity: {row[2]}, Price: {row[3]}, Last Updated: {row[4]}")
        except Exception as e:
            print(f"[Node {self.id_node}] Error reading inventory: {e}")

    def update_inventory(self, item_id, quantity_change, propagate=True):
        """Actualiza la cantidad de un artículo en el inventario y propaga el cambio si es necesario"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT quantity FROM inventory WHERE id = ?", (item_id,))
            result = cursor.fetchone()
            if result:
                new_quantity = result[0] + quantity_change
                if new_quantity < 0:
                    print(f"[Node {self.id_node}] Error: Not enough stock for item {item_id}")
                    return False
                cursor.execute("""
                    UPDATE inventory
                    SET quantity = ?, last_updated = ?
                    WHERE id = ?
                """, (new_quantity, datetime.now().isoformat(), item_id))
                conn.commit()
                print(f"[Node {self.id_node}] Inventory updated for item {item_id}")

                # Propaga la actualización si es necesario
                if propagate:
                    success = self.propagate_inventory_update(item_id, new_quantity)
                    if not success:
                        print(f"[Node {self.id_node}] Rolling back inventory update for item {item_id}")
                        # Rollback: restaurar cantidad anterior
                        cursor.execute("""
                            UPDATE inventory
                            SET quantity = ?, last_updated = ?
                            WHERE id = ?
                        """, (result[0], datetime.now().isoformat(), item_id))
                        conn.commit()
                        conn.close()
                        return False
            else:
                print(f"[Node {self.id_node}] Error: Item {item_id} not found in inventory")
            conn.close()
            return True
        except Exception as e:
            print(f"[Node {self.id_node}] Error updating inventory: {e}")
            return False

    def sync_inventory(self):
        """Sincroniza el inventario con otros nodos"""
        for port, ip in self.nodes_info.items():
            try:
                message = {
                    'origin': self.id_node,
                    'destination': port,
                    'content': 'SYNC_INVENTORY',
                    'timestamp': datetime.now().isoformat()
                }
                if self.send_message(message):
                    print(f"[Node {self.id_node}] Sync request sent to Node {port - self.base_port}")
            except Exception as e:
                print(f"[Node {self.id_node}] Error syncing inventory with Node {port - self.base_port}: {e}")

    def _save_message_to_db(self, msg):
        """Guarda un mensaje en la base de datos"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            # Convertir el contenido del mensaje a JSON si es un diccionario
            content = msg.get('content')
            if isinstance(content, dict):
                content = json.dumps(content)

            cursor.execute("""
                INSERT INTO messages (origin, destination, content, timestamp)
                VALUES (?, ?, ?, ?)
            """, (
                msg.get('origin', self.id_node),
                msg.get('destination'),
                content,
                msg.get('timestamp', datetime.now().isoformat())
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[Node {self.id_node}] DB insert error: {e}")

    
    def _show_history(self):
        """Muestra el historial de mensajes desde la base de datos"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT origin, destination, content, timestamp FROM messages ORDER BY id ASC")
            rows = cursor.fetchall()
            conn.close()

            print("\nMessage History (from DB):")
            print("=" * 40)
            for i, (origin, dest, content, ts) in enumerate(rows, 1):
                print(f"{i}. [{ts}] {origin} -> {dest - self.base_port}: {content}")

        except Exception as e:
            print(f"[Node {self.id_node}] Error reading history: {e}")


    def send_message(self, message_dict):
        """Envía un mensaje a otro nodo"""
        try:
            dest_port = message_dict['destination']
            if dest_port == self.port:
                print(f"[Node {self.id_node}] Warning: Cannot send message to self.")
                return False

            dest_ip = self.nodes_info.get(dest_port)
            if not dest_ip:
                print(f"[Node {self.id_node}] Error: Unknown destination port {dest_port}")
                return False

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5.0)

                s.connect((dest_ip, dest_port))

                message_dict['origin'] = self.id_node
                message_dict['timestamp'] = datetime.now().isoformat()

                s.sendall(json.dumps(message_dict).encode('utf-8'))
                self.messages.append(message_dict)
                print(f"[Node {self.id_node}] Sent to {dest_port}: {message_dict}")
                return True

        except ConnectionRefusedError:
            print(f"[Node {self.id_node}] Error: Node {dest_port - self.base_port} not available")
        except socket.timeout:
            print(f"[Node {self.id_node}] Error: Connection timeout with node {dest_port - self.base_port}")
        except Exception as e:
            print(f"[Node {self.id_node}] Send error: {e}")
        
        return False


    def user_interface(self):
        """Interfaz de línea de comandos"""
        print(f"\nNode {self.id_node} - Command Interface")
        print("=" * 40)

        while True:
            try:
                print("\nOptions:")
                print("1. Send message")
                print("2. View message history")
                print("3. Export message history")
                print("4. View DB messages")
                print("5. Show inventory")
                print("6. Update inventory")
                print("7. Sync inventory with other nodes")
                print("8. Add new client")
                print("9. View client list")
                print("10. Purchase an item (with mutual exclusion)")
                print("11. Start master election")  # Nueva opción
                print("12. Exit")

                choice = input("Select option: ").strip()

                if choice == "1":
                    self._send_message_ui()
                elif choice == "2":
                    self._show_history()
                elif choice == "3":
                    self.export_history()
                elif choice == "4":
                    self._show_db_messages()
                elif choice == "5":
                    self.show_inventory()
                elif choice == "6":
                    self._update_inventory_ui()
                elif choice == "7":
                    self.sync_inventory()
                elif choice == "8":
                    self._add_client_ui()
                elif choice == "9":
                    self._view_clients()
                elif choice == "10":
                    self._purchase_item_ui()
                elif choice == "11":
                    self.start_election()  # Llama al método para iniciar la elección
                elif choice == "12":
                    print("Exiting...")
                    break
                else:
                    print("Invalid option")

            except Exception as e:
                print(f"Error: {e}")


    def _send_message_ui(self):
        """Maneja el envío de mensajes desde la UI"""
        available_ids = [p - self.base_port for p in self.nodes_info.keys()]
        print("\nAvailable node IDs:", available_ids)
        try:
            dest_node_id = int(input("Destination node ID: "))
            dest_port = self.base_port + dest_node_id
            
            if dest_port not in self.nodes_info:
                print("Error: Invalid destination node ID")
                return

            content = input("Message: ").strip()
            if not content:
                print("Error: Message cannot be empty")
                return

            message = {
                'destination': dest_port,
                'content': content
            }
            self.send_message(message)

        except ValueError:
            print("Error: Please enter a valid node ID")

    def _show_history(self):
        """Muestra el historial de mensajes"""
        print("\nMessage History:")
        print("=" * 40)
        for i, msg in enumerate(self.messages, 1):
            direction = f"{msg.get('origin', '?')} -> {msg['destination'] - self.base_port}"
            print(f"{i}. [{msg['timestamp']}] {direction}: {msg['content']}")

    def export_history(self):
        """Exporta el historial a JSON"""
        filename = f"node_{self.id_node}_history.json"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'node_id': self.id_node,
                    'timestamp': datetime.now().isoformat(),
                    'messages': self.messages
                }, f, indent=2, ensure_ascii=False)
            print(f"History exported to {filename}")
        except Exception as e:
            print(f"Export failed: {e}")

    def _show_db_messages(self):
        """Muestra los mensajes guardados en la base de datos"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT origin, destination, content, timestamp FROM messages")
            rows = cursor.fetchall()
            conn.close()

            print("\nDatabase Messages:")
            for i, row in enumerate(rows, 1):
                origin, destination, content, timestamp = row
                print(f"{i}. [{timestamp}] {origin} -> {destination - self.base_port}: {content}")
        except Exception as e:
            print(f"[Node {self.id_node}] Error reading messages from DB: {e}")

    ### ELECCIÓN DE NODO MAESTRO ###

    def start_election(self):
        """Inicia el proceso de elección (algoritmo de Bully)"""
        print(f"[Node {self.id_node}] Starting election...")
        self.increment_clock()

        # Identificar nodos con IDs mayores
        higher_nodes = [port for port in self.nodes_info.keys() if port > self.port]
        higher_nodes_ids = [port - self.base_port for port in higher_nodes]

        print(
            f"\n[Node {self.id_node}] ⚡ ELECTION INITIATED\n"
            f"   │ Current Clock: {self.clock}\n"
            f"   │ Contacting Nodes: {higher_nodes_ids}\n"
            f"   └── Waiting for replies (5s timeout)..."
        )

        # Enviar mensajes de ELECTION a nodos con IDs mayores
        election_message = {
            'type': 'ELECTION',
            'origin': self.id_node,
            'clock': self.clock,
            'timestamp': datetime.now().isoformat()
        }
        for port in higher_nodes:
            self.send_message({
                'destination': port,
                'content': json.dumps(election_message)
            })

        # Esperar respuestas de nodos con IDs mayores
        start_time = time.time()
        while time.time() - start_time < 5:
            for msg in self.messages:
                content = msg.get('content', {})
                if isinstance(content, str):
                    try:
                        content = json.loads(content)
                    except json.JSONDecodeError:
                        continue

                # Si un nodo con ID mayor responde, abortar la elección
                if content.get('type') == 'REPLY' and content.get('origin', 0) > self.id_node:
                    print(f"[Node {self.id_node}] Aborting election: Higher node {content['origin']} active")
                    return

            time.sleep(0.1)

        # Si no hay respuestas, declararse como maestro
        self.become_master()
            
        # Esperar respuestas con timeout
        start_time = time.time()
        while time.time() - start_time < 5:
            for msg in self.messages:
                # Extract content (could be str or dict)
                content = msg.get('content', {})
                if isinstance(content, str):
                    try:
                        content = json.loads(content)
                    except json.JSONDecodeError:
                        continue
                
                # Check if it's a valid REPLY from higher node
                if content.get('type') == 'REPLY' and content.get('origin', 0) > self.id_node:
                    print(f"[Node {self.id_node}] Aborting election: Higher node {content['origin']} active")
                    return
            
            time.sleep(0.1)
        
        # Only become master if no replies
        self.become_master()
        


    def become_master(self):
        """Se declara como maestro y notifica a los demás nodos"""
        print(
            f"\n[Node {self.id_node}] 🎉 MASTER NODE ELECTED\n"
            f"   │ Current Clock: {self.clock}\n"
            f"   │ Election Time: {datetime.now().isoformat()}\n"
            f"   │ Notifying Nodes: {list(self.nodes_info.keys())}\n"
            f"   └── Sending COORDINATOR messages..."
        )

        coordinator_message = {
            'type': 'COORDINATOR',
            'origin': self.id_node,
            'clock': self.clock,
            'timestamp': datetime.now().isoformat()
        }
        for port in self.nodes_info.keys():
            self.send_message({
                'destination': port,
                'content': json.dumps(coordinator_message)
            })

    def announce_master(self):
        """Anuncia a todos los nodos que este nodo es el nuevo maestro"""
        for port in self.nodes_info.keys():
            try:
                coordinator_message = {
                    'type': 'COORDINATOR',
                    'origin': self.id_node,
                    'clock': self.clock,  # Incluye el reloj lógico actual
                    'timestamp': datetime.now().isoformat()
                }
                print(f"[Node {self.id_node}] Sending COORDINATOR message to Node {port - self.base_port}: {coordinator_message}")
                self.send_message({
                    'destination': port,
                    'content': json.dumps(coordinator_message)
                })
            except Exception as e:
                print(f"[Node {self.id_node}] Error sending COORDINATOR to Node {port - self.base_port}: {e}")

    def handle_election_message(self, message):
        """Handles ELECTION messages"""
        self.synchronize_clock(message['clock'])
        if self.id_node > message['origin']:
            print(
                f"\n[Node {self.id_node}] 🔄 ELECTION RESPONSE\n"
                f"   │ Received From: Node {message['origin']}\n"
                f"   │ Their Clock: {message['clock']}\n"
                f"   └── Sending REPLY (ID {self.id_node} > {message['origin']})"
            )
            reply = {
                'type': 'REPLY',
                'origin': self.id_node,
                'clock': self.clock,
                'timestamp': datetime.now().isoformat()
            }
            self.send_message({
                'destination': self.base_port + message['origin'],
                'content': json.dumps(reply)
            })
            self.start_election()

if __name__ == "__main__":
    # Configuración - CAMBIAR POR CADA NODO
    NODE_ID = int(os.getenv("NODE_ID", 1))  # Toma el valor de la variable de entorno NODE_ID, por defecto 1  # Cambiar este valor (1, 2, 3...)
    BASE_PORT = 5000
    NODE_IPS = {
        5001: '192.168.100.61',
        5002: '192.168.100.62',
        5003: '192.168.100.63',
        5004: '192.168.100.64'
    }

    server_ready = threading.Event()
    node = Node(
        id_node=NODE_ID,
        port=BASE_PORT + NODE_ID,
        nodes_info={p: ip for p, ip in NODE_IPS.items() if p != BASE_PORT + NODE_ID},
        server_ready_event=server_ready,
        base_port=BASE_PORT
    )

    threading.Thread(target=node.start_server, daemon=True).start()
    server_ready.wait()
    node.user_interface()
