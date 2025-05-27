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
import utils
from modules import products, clients, inventories, purchases

class Node:
    def __init__(
            self, id_node: int, port: int, all_nodes: dict, static_ip: str,
            node_ip='0.0.0.0', base_port=5000,
            server_ready_event=None, neighbours_ready_event=None
        ):
        """
        Args:
            id_node: Identificador único del nodo (1, 2, 3...)
            node_ip: IP del nodo
            port: Puerto de escucha
            base_port: Puerto base para cálculo de IDs
            server_ready_event: Evento para sincronización
            all_nodes: Arreglo de todos los nodos disponibles ['x.x.x.x', 'y.y.y.y', ...]
        """
        self.id_node = id_node
        self.ip = node_ip
        self.static_ip = static_ip
        self.port = port
        self.base_port = base_port
        self.all_nodes = all_nodes # Lista con las IPs de todos los nodos esperados.
        self.neighbours = {} # Diccionario con las {node_id: static_id} de los nodos a los que si tiene acceso.
        self.messages = []
        self.server = None
        self.db_name = f"node_{self.id_node}.db"
        self.clock = 0  # Reloj lógico Lamport
        self.request_queue = []  # Cola de solicitudes pendientes
        self.in_critical_section = False  # Indica si el nodo está en la sección crítica
        self.replies_received = 0  # Contador de respuestas REPLY

        # Inicialización de la base de datos (utilizando rutas relativas).
        # Se ingresará la db al mismo nivel que todo el proyecto.
        db_dir = Path(__file__).resolve().parent
        db_path = db_dir / self.db_name
        db_path.touch(exist_ok=True)

        db_seeds_dir = Path(__file__).resolve().parent / 'seeds'
        ensure_schema(db_path=db_path, seeds_dir=db_seeds_dir)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.is_master = False  # Debe existir al menos una eleccion para encontrar el maestro.
        self.master = None  # Al inicar el nodo, no tiene referencia de quien es el maestro.

        self.neighbours_ready_event = neighbours_ready_event
        self.server_ready_event = server_ready_event

    ########################
    # ACCIONES NODO LOCAL
    ########################

    # Servicios en segundo plano (deamons)
    def start_server(self):
        """Inicia el servidor TCP para recibir mensajes"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self.server = s
            s.bind((self.ip, self.port))
            s.listen()
            print(f"[Nodo {self.id_node}] Servidor escuchando en {self.ip}:{self.port}")
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
                    print(f"[Nodo {self.id_node}] Error en servidor: {e}")
    
    def daemon_check_neighbours(self, interval=15):
        prev_neighbours = set()
        while True:
            # Se identificarán los "nodos" disponibles al momento de la creación del nodo.
            neighbours_available: dict = utils.update_neighbours(self.static_ip, self.all_nodes)
            curr_neighbours = set(neighbours_available)

            if curr_neighbours != prev_neighbours:
                news = curr_neighbours - prev_neighbours
                lost = prev_neighbours - curr_neighbours

                for n in news:
                    print(f"🟢 Nuevo nodo conectado: {n}")
                for l in lost:
                    print(f"🔴 Nodo desconectado: {l}")
                
                prev_neighbours = curr_neighbours

                NODE_IPS = {}
                for node_id in curr_neighbours:
                    NODE_IPS[self.base_port + node_id] = neighbours_available.get(node_id)
                self.neighbours = NODE_IPS
            else:
                print("✅ Lista de nodos sin cambios.", self.neighbours)
            # Se debe ejecutar el proceso de busqueda de vecinos almenos una vez para pasar al siguiente paso.
            self.neighbours_ready_event.set()
            # Independientemente de los cambios en los vecinos, se revisa si sigue activo el nodo maestro.
            self.check_master()
            time.sleep(interval)

    def check_master(self):
        # Si existe el maestro, verificar si sigue activo.
        # Si no hay maestro o el maestro se detuvo, identificar nodos con IDs mayores
        higher_nodes = [node_id for node_id in self.neighbours.keys() if node_id > self.port]
        if not self.master:
            print(f'🚨 No hay referencia de nodo maestro. ({self.master})')
            self.start_election(higher_nodes)
        elif not higher_nodes and self.master == self.id_node:
            print(f'🗿 Te mantienes como coordinador. ({self.master})')
        elif higher_nodes and self.master == self.id_node:
            print(f'💎 Nuevo(s) candidato(s) a coordinador. ({higher_nodes})')
            self.start_election(higher_nodes)
        elif self.master and self.master != self.id_node and utils.ping_ip(self.neighbours.get(self.master)):
            print(f'⭐️ El nodo maestro sigue sin cambios. ({self.master})')
        elif self.master != self.id_node and not self.neighbours.get(self.master):
            print(f'🚨 Nodo maestro desconectado. ({self.master})')
            self.start_election(higher_nodes)

    ########################
    # ACCIONES ENTRE NODOS
    ########################
    # Mensajeria
    def handle_connection(self, conn, addr):
        """Maneja cualquier mensaje de entrada."""
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
                    print(f"[Nodo {self.id_node}] Invalid JSON message: {data}")
                    return

                # Parse content if it's a JSON string
                if utils.is_valid_json(message.get('content')):
                    message['content'] = json.loads(message.get('content'))

                # Ensure content_data is a dictionary
                if not isinstance(message, dict):
                    print(f"[Nodo {self.id_node}] Invalid content format: {message}")
                    return

                # Extract message details
                msg_type = message.get('type')
                origin = message.get('origin')

                # Store parsed message (with content as dict)
                self.messages.append(message)
                self._save_message_to_db(message)

                # Print formatted message
                hour = datetime.fromisoformat(message['timestamp']).strftime("%H:%M:%S")
                print(f"[Nodo {self.id_node}] Recibido de {origin} a las {hour}: {message}")

                # Handle message types
                if msg_type == 'COORDINATOR':
                    self.handle_coordinator_message(message)

                elif msg_type == 'ELECTION':
                    self.handle_election_message(message)

                elif msg_type == 'REPLY':
                    self.handle_reply(message)

                elif msg_type == 'INVENTORY_UPDATE':
                    item_id = message.get('content').get('item_id')
                    new_quantity = message.get('content').get('new_quantity')
                    if item_id and new_quantity:
                        self._update_inventory(
                            item_id,
                            new_quantity - self._get_item_quantity(item_id),
                            propagate=False
                        )
                        print(f"[Nodo {self.id_node}] Inventario actualizado para el item {item_id}")

                elif msg_type == 'CLIENT_UPDATE':
                    # Handle client updates
                    clients.handle_client_update(self, message)
                
                elif msg_type == 'GET_CAPACITY':
                    item_id = message.get('content').get('item_id')
                    current_quantity = self._get_item_quantity(item_id)
                    capacity_message = {
                        'type': 'CAPACITY_RESPONSE',
                        'item_id': item_id,
                        'capacity': current_quantity,
                        'origin': self.id_node,
                        'timestamp': datetime.now().isoformat()
                    }
                    self.send_message(self, {
                        'destination': self.base_port + origin,
                        'content': json.dumps(capacity_message)
                    })
                    print(f"[Nodo {self.id_node}] Se envio stock actual del articulo {item_id} al Nodo {origin}.")
                elif msg_type == 'PLAIN_TEXT':
                    ack_msg = {
                        'type': 'ACK',
                        'origin': self.id_node,
                        'destination': origin,
                        'content': f"ACK: {json.dumps(message.get('content'))}",
                        'timestamp': datetime.now().isoformat()
                    }
                    if self.send_message(ack_msg):
                        print(f"[Nodo {self.id_node}] ACK enviado a {origin}")
                    else:
                        print(f"[Nodo {self.id_node}] Error enviando ACK")
                elif msg_type == 'ACK':
                    print(f"[Nodo {self.id_node}] ACK recibido desde {message.get('origin')}")

            except KeyError as ke:
                print(f"[Nodo {self.id_node}] Message format error: Missing key {ke}")
            except Exception as e:
                print(f"[Nodo {self.id_node}] Critical error: {str(e)}")

    def send_message(self, message_dict: dict):
        """
        Envía un mensaje a otro nodo
        message_dict: Diccionario con la estructura {destination: int, content: JSON}
        """
        node_dest_id = message_dict['destination']
        port_dest = self.base_port + node_dest_id
        if node_dest_id == self.id_node:
            print(f"[Nodo {self.id_node}] Advertencia: No se puede enviar un mensaje a si mismo.")
            return False

        dest_ip = self.neighbours.get(node_dest_id)
        if not dest_ip:
            print(f"[Nodo {self.id_node}] Error: Destino desconocido en puerto {node_dest_id}")
            return False
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5.0)

                s.connect((dest_ip, port_dest))

                message_dict['origin'] = self.id_node
                message_dict['timestamp'] = datetime.now().isoformat()

                s.sendall(json.dumps(message_dict).encode('utf-8'))
                self.messages.append(message_dict)
                print(f"[Nodo {self.id_node}] Enviado a {port_dest}: {message_dict}")
                return True

        except ConnectionRefusedError as e:
            print(f"[Nodo {self.id_node}] Error: Nodo {node_dest_id} no disponible")
            print(e)
        except socket.timeout:
            print(f"[Nodo {self.id_node}] Error: Connection timeout with node {port_dest}")
        except Exception as e:
            print(f"[Nodo {self.id_node}] Error en envio: {e}")
        
        return False
    
    def _send_message_ui(self):
        """Maneja el envío de mensajes desde la UI"""
        available_ids = [p - self.base_port for p in self.neighbours.keys()]
        print("\nNodos disponibles (IDs):", available_ids)
        try:
            dest_node_id = int(input("Nodo destino (ID): "))
            
            if dest_node_id not in available_ids:
                print("Error: ID de nodo invalido.")
                return

            content = input("Mensaje: ").strip()
            if not content:
                print("Error: El mensaje no puede estar vacio.")
                return

            message: dict = {
                'type': 'PLAIN_TEXT',
                'destination': dest_node_id,
                'content': content
            }
            self.send_message(message)

        except ValueError:
            print("Error: Por favor ingresa un ID de nodo valido.")

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
            self.send_message(self, {
                'destination': self.base_port + origin,
                'content': json.dumps(reply_message)
            })
            print(f"[Nodo {self.id_node}] RESPUESTA enviada al nodo {origin}.")
        else:
            # Agregar la solicitud a la cola
            self.request_queue.append(message)

    def handle_reply(self, message):
        """Maneja un mensaje REPLY recibido"""
        self.synchronize_clock(message['clock'])
        self.replies_received += 1
        print(f"[Nodo {self.id_node}] RESPUESTA recibida desde nodo {message['origin']}. Todas las respuestas: {self.replies_received}")

    def _show_bd_history(self):
        """Muestra el historial de mensajes desde la base de datos"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT origin, destination, content, timestamp FROM messages ORDER BY id ASC")
            rows = cursor.fetchall()
            conn.close()

            print("\nHistorial de mensajes (desde BD):")
            print("=" * 40)
            for i, (origin, dest, content, ts) in enumerate(rows, 1):
                print(f"{i}. [{ts}] {origin} -> {dest - self.base_port}: {content}")

        except Exception as e:
            print(f"[Nodo {self.id_node}] Error leyendo historial: {e}")

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
            print(f"[Nodo {self.id_node}] Error durante insercion en la BD: {e}")

    def _show_text_history(self):
        """Muestra el historial de mensajes"""
        print("\nHistorial de mensajes:")
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
            print(f"Historial exportado en {filename}")
        except Exception as e:
            print(f"Error durante exportacion: {e}")

    # Sincronizacion
    def synchronize_clock(self, received_clock):
        """Sincroniza el reloj lógico con un valor recibido"""
        self.clock = max(self.clock, received_clock) + 1

    def increment_clock(self):
        """Incrementa el reloj lógico"""
        self.clock += 1

    # Metodos de exclusion mutua.
    def request_critical_section(self, timeout=5):
        """Solicita acceso a la sección crítica con un timeout"""
        self.increment_clock()
        self.in_critical_section = True
        self.replies_received = 0
        self.pending_replies = len(self.neighbours)  # Número de nodos de los que se espera respuesta

        # Enviar mensaje REQUEST a todos los nodos
        for port, ip in self.neighbours.items():
            message = {
                'type': 'REQUEST',
                'clock': self.clock,
                'origin': self.id_node,
                'timestamp': datetime.now().isoformat()
            }
            if self.send_message(self, {
                'destination': port,
                'content': json.dumps(message)
            }):
                print(f"[Nodo {self.id_node}] Enviando SOLICITUD al nodo {port - self.base_port}")
            else:
                print(f"[Nodo {self.id_node}] Error enviando SOLICITUD al nodo {port - self.base_port}")
                self.pending_replies -= 1  # Reducir el número de respuestas esperadas si el nodo no está disponible

        # Esperar respuestas con un timeout
        start_time = time.time()
        while self.replies_received < self.pending_replies:
            if time.time() - start_time > timeout:
                print(f"[Nodo {self.id_node}] Timeout waiting for replies. Aborting critical section request.")
                print(f"[Nodo {self.id_node}] Se recibieron {self.replies_received} respuestas de {self.pending_replies} esperadas.")
                self.in_critical_section = False
                return  # Abortamos si no recibimos suficientes respuestas
            time.sleep(0.1)  # Esperar un breve momento antes de verificar nuevamente

        # Si se recibieron suficientes respuestas, entrar en la sección crítica
        if self.replies_received >= self.pending_replies:
            print(f"[Nodo {self.id_node}] Se recibieron todas las respuestas necesarias. Accediendo a seccion critica.")
            self.enter_critical_section()
 
    def enter_critical_section(self):
        """Entra en la sección crítica"""
        print(f"[Nodo {self.id_node}] En seccion critica.")
        # Aquí puedes realizar la operación crítica (por ejemplo, comprar un artículo)
        self.purchase_item()

        # Salir de la sección crítica
        self.exit_critical_section()

    def exit_critical_section(self):
        """Sale de la sección crítica"""
        print(f"[Nodo {self.id_node}] Saliendo de la seccion critica.")
        self.in_critical_section = False

        # Responder a las solicitudes pendientes en la col
        while self.request_queue:
            pending_request = self.request_queue.pop(0)
            self.handle_request(pending_request)

    # Consenso
    def two_phase_commit(self, prepare_message, commit_message, abort_message):
        """Implementa el protocolo de 2PC"""
        try:
            # Fase 1: PREPARE
            prepare_responses = 0
            for port, ip in self.neighbours.items():
                if self.send_message({
                    'destination': port,
                    'content': json.dumps(prepare_message)
                }):
                    prepare_responses += 1

            # Verificar si todos los nodos están listos
            if prepare_responses < len(self.neighbours):
                print(f"[Nodo {self.id_node}] No todos los nodos estan listos. Abortando...")
                for port, ip in self.neighbours.items():
                    self.send_message({
                        'destination': port,
                        'content': json.dumps(abort_message)
                    })
                return False

            # Fase 2: COMMIT
            for port, ip in self.neighbours.items():
                self.send_message({
                    'destination': port,
                    'content': json.dumps(commit_message)
                })
            print(f"[Nodo {self.id_node}] Commit aplicado.")
            return True
        except Exception as e:
            print(f"[Nodo {self.id_node}] Error en 2PC: {e}")
            return False

    # ELECCIÓN DE NODO MAESTRO
    def start_election(self, higher_nodes):
        """Inicia el proceso de elección (algoritmo de Bully)"""
        print(f"[Nodo {self.id_node}] Comenzando eleccion...")
        self.increment_clock()

        print(
            f"\n[Nodo {self.id_node}] ⚡ ELECCION INICIADA\n"
            f"   │ Reloj actual: {self.clock}\n"
            f"   │ Contactando nodos: {higher_nodes}\n"
            f"   └── Esperando respuestas (5s timeout)..."
        )

        # Enviar mensajes de ELECTION a nodos con IDs mayores
        election_message = {
            'type': 'ELECTION',
            'origin': self.id_node,
            'clock': self.clock,
            'timestamp': datetime.now().isoformat()
        }
        for port in [self.base_port + node_id for node_id in higher_nodes]:
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
                    print(f"[Nodo {self.id_node}] Abortando eleccion: Nodo mayor {content['origin']} esta activo.")
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
                    print(f"[Nodo {self.id_node}] Abortando eleccion: Nodo mayor {content['origin']} esta activo.")
                    return
            
            time.sleep(0.1)
        
        # Only become master if no replies
        self.become_master()
        
    def become_master(self):
        """Se declara como maestro y notifica a los demás nodos"""
        self.is_master = True  # Este nodo ahora es el maestro
        self.master = self.id_node
        print(
            f"\n[Nodo {self.id_node}] 🎉 AHORA ERES EL NODO MAESTRO\n"
            f"   │ Reloj actual: {self.clock}\n"
            f"   │ Momento de eleccion: {datetime.now().isoformat()}\n"
            f"   │ Notificando a nodos: {list(self.neighbours.keys())}\n"
            f"   └── Enviando notificacion como COORDINADOR..."
        )

        coordinator_message = {
            'type': 'COORDINATOR',
            'origin': self.id_node,
            'clock': self.clock,
            'timestamp': datetime.now().isoformat()
        }
        for port in self.neighbours.keys():
            self.send_message({
                'destination': port,
                'content': json.dumps(coordinator_message)
            })

    def announce_master(self):
        """Anuncia a todos los nodos que este nodo es el nuevo maestro"""
        for port in self.neighbours.keys():
            try:
                coordinator_message = {
                    'type': 'COORDINATOR',
                    'origin': self.id_node,
                    'clock': self.clock,  # Incluye el reloj lógico actual
                    'timestamp': datetime.now().isoformat()
                }
                print(f"[Nodo {self.id_node}] Enviando mensaje como COORDINATOR al Nodo {port - self.base_port}: {coordinator_message}")
                self.send_message({
                    'destination': port,
                    'content': json.dumps(coordinator_message)
                })
            except Exception as e:
                print(f"[Nodo {self.id_node}] Error enviando mensaje como COORDINATOR al Nodo {port - self.base_port}: {e}")

    def handle_election_message(self, message):
        """Handles ELECTION messages"""
        self.synchronize_clock(message['clock'])
        if self.id_node > message['origin']:
            print(
                f"\n[Nodo {self.id_node}] 🔄 RESPUESTA A LA ELECCION\n"
                f"   │ Recibido desde: Nodo {message['origin']}\n"
                f"   │ Su reloj: {message['clock']}\n"
                f"   └── Enviando RESPUESTA (ID {self.id_node} > {message['origin']})"
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

    def handle_coordinator_message(self, message):
        """Maneja mensajes COORDINATOR (nodo maestro)"""
        new_master_id = message['origin']

        # Verificar si ya se procesó este mensaje
        if hasattr(self, 'last_coordinator_message') and self.last_coordinator_message == message:
            return  # Ignorar mensajes repetidos

        # Actualizar el último mensaje procesado
        self.last_coordinator_message = message

        print(
            f"[Nodo {self.id_node}] 🟢 CORDINADOR SELECCIONADO\n"
            f"   │ Nuevo coordinador: Node {new_master_id}\n"
            f"   │ Reloj logico: {message.get('clock', 'N/A')}\n"
            f"   │ Timestamp: {message['timestamp']}\n"
        )
        self.is_master = (self.id_node == new_master_id)  # Solo el nuevo maestro tiene is_master = True
        self.master = new_master_id

    ############
    # CLIENTES
    ############
    def _view_clients(self): clients.view_clients(self)
    def _add_client_ui(self): clients.add_client_ui(self)

    #####################
    # PRODUCTOS / ITEMS
    #####################
    def _show_products(self): products.show_products(self)
    def _create_product_ui(self): products.create_product_ui(self)
    def _update_product_ui(self): products.update_product_ui(self)
    def _distribute_items_ui(self): products.distribute_items_ui(self)

    ###############
    # INVENTARIOS
    ###############
    def _get_item_quantity(self, item_id): inventories.get_item_quantity(self, item_id)
    def _show_inventory(self): inventories.show_inventory(self)
    def _propagate_inventory_update(self, item_id, new_quantity):
        inventories.propagate_inventory_update(self, item_id, new_quantity)
    def _update_inventory(self, item_id, quantity_change, propagate=True):
        inventories.update_inventory(self, item_id, quantity_change, propagate)
    def _update_inventory_ui(self): inventories.update_inventory_ui(self)

    ##########
    # COMPRAS
    ##########
    def _create_purchase_ui(self): purchases.create_purchase_ui(self)
    def _show_purchases(self): purchases.show_purchases(self)

    #######################
    # INTERFAZ DE USUARIO
    #######################
    def user_interface(self):
        """Interfaz de línea de comandos"""

        while True:
            try:
                print("\n////////////////////////////////////////////")
                print(f"Nodo {self.id_node} - Opciones disponibles")
                print("=" * 40)
                print("\t--> Clientes <--")
                print("1. Ver lista de clientes")
                print("2. Agregar nuevo cliente")
                print("\t--> Productos <--")
                print("3. Mostrar productos")
                print("4. Crear productos")
                print("5. Actualizar productos")
                print("\t--> Inventario <--")
                print("6. Mostrar inventario")
                print("\t--> Compras <--")
                print("7. Ver compras")
                print("8. Realizar una compra") # Método que integra exclusión mutua.
                print('\nEXTRAS')
                print("=" * 40)
                print("\t--> Nodo actual <--")
                print("400. Enviar mensaje")
                print("401. Ver historial de mensajes")
                print("402. Exportar historial de mensajes")
                print("403. Ver mensajes de BD")
                print("\t--> Opciones para realizar pruebas <--")
                print("404. Actualizar inventario")
                print("405. FIX: Show Product guide")
                print("406. FIX: Sync inventory with other nodes")
                print("407. FIX: Start master election")
                print("408. FIX: Distribute items")
                print("\nSALIR")
                print("=" * 40)
                print("99. Salir \n")

                choice = input("Selecciona una opcion: ").strip()

                # Opciones del proyecto
                if choice == "1":
                    self._view_clients()
                elif choice == "2":
                    self._add_client_ui()
                elif choice == "3":
                    self._show_products()
                elif choice == "4":
                    self._create_product_ui()
                elif choice == "5":
                    self._update_product_ui()
                elif choice == "6":
                    self._show_inventory()
                elif choice == "7":
                    self._show_purchases()
                elif choice == "8":
                    self._create_purchase_ui()
                # Opciones extras
                elif choice == "400":
                    self._send_message_ui()
                elif choice == "401":
                    self._show_text_history()
                elif choice == "402":
                    self._export_history()
                elif choice == "403":
                    self._show_bd_history()
                elif choice == "404":
                    self._update_inventory_ui()
                elif choice == "405":
                    self._user_guide()
                elif choice == "406":
                    self._sync_inventory()
                elif choice == "407":
                    self._start_election()  # Llama al método para iniciar la elección
                elif choice == "408":
                    self._distribute_items_ui()  # Llama al método para distribuir artículos
                elif choice == "99":
                    print("Saliendo...")
                    break
                else:
                    print("Opcion invalida.")

            except Exception as e:
                print(f"Error: {e}")


# Diccionario inicial de los nodos esperados en el sistema.
# Clave: ID de nodo, Valor: IP estatica del nodo.
DEFAULT_IPS = [
    '192.168.100.100',
    '192.168.100.61',
    '192.168.100.62',
    '192.168.100.63',
    '192.168.100.64'
]

if __name__ == "__main__":
    # Toma el valor de la variable de entorno NODE_ID, si no se cuenta con valor, se determina con el ultimo
    # numero de su IP estatica.
    current_ip = utils.get_static_ip()
    NODE_ID = int(os.getenv("NODE_ID", current_ip.split('.')[-1]))
    print(f'Generando nodo con ID {NODE_ID} ({current_ip})...')

    neighbours_ready = threading.Event()
    server_ready = threading.Event()
    node = Node(
        id_node = NODE_ID,
        static_ip=current_ip,
        port = 5000 + NODE_ID,
        all_nodes = {int(ip.split('.')[-1]): ip for ip in DEFAULT_IPS},
        neighbours_ready_event = neighbours_ready,
        server_ready_event = server_ready,
    )

    # Se realiza la busqueda de los nodos vecinos al menos 1 vez antes de que inicie el servidor.
    threading.Thread(target=node.daemon_check_neighbours, daemon=True).start()
    neighbours_ready.wait()
    # Ya que existen sus nodos vecinos, se comienzan a pedir instrucciones.
    threading.Thread(target=node.start_server, daemon=True).start()
    server_ready.wait()
    node.user_interface()
