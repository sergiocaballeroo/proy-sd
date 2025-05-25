#!/bin/bash

# Nombre del script: configurar_nodo.sh

echo "Configuración de la variable de entorno NODE_ID para el nodo."

# Solicitar al usuario el ID del nodo
read -p "Introduce el NODE_ID para este nodo: " NODE_ID

# Configurar la variable de entorno temporalmente
export NODE_ID=$NODE_ID
echo "Variable de entorno NODE_ID configurada temporalmente con valor: $NODE_ID"

# Preguntar si desea configurar la variable permanentemente
read -p "¿Deseas configurar NODE_ID permanentemente en ~/.bashrc? (s/n): " RESPUESTA
if [[ "$RESPUESTA" == "s" || "$RESPUESTA" == "S" ]]; then
    # Agregar la configuración al archivo ~/.bashrc
    echo "export NODE_ID=$NODE_ID" >> ~/.bashrc
    echo "NODE_ID=$NODE_ID agregado a ~/.bashrc"

    # Recargar ~/.bashrc
    source ~/.bashrc
    echo "Archivo ~/.bashrc recargado."
fi

# Verificar la configuraciónp
echo "Verificando la configuración de NODE_ID..."
echo "NODE_ID actual: $NODE_ID"

# Preguntar si desea ejecutar el script Python
read -p "¿Deseas ejecutar el script nodes.py ahora? (s/n): " RESPUESTA
if [[ "$RESPUESTA" == "s" || "$RESPUESTA" == "S" ]]; then
    python3 nodes.py
else
    echo "Configuración completada. Puedes ejecutar nodes.py manualmente con 'python3 nodes.py'."
fi