# Guía de Instalación del Proyecto

Este proyecto requiere **Python 3.10** y un archivo `.env` para almacenar variables de entorno sensibles o de configuración. A continuación, se detallan los pasos para su correcta instalación y uso.

---

## 1. Instalar Python 3.10

### 1.1 Windows
1. Visita la página oficial de [Python](https://www.python.org/downloads/windows/).  
2. Descarga el instalador de **Python 3.10** (“Windows installer”).  
3. Ejecuta el instalador y **asegúrate de marcar la casilla “Add Python to PATH”**.  
4. Finalizada la instalación, abre **CMD** o **PowerShell** y verifica:  
   ```bash
   python --version
   ```
   Debería mostrar `Python 3.10.x`.

### 1.2 macOS
1. Visita la página oficial de [Python](https://www.python.org/downloads/macos/).  
2. Descarga la última versión de **Python 3.10** para macOS.  
3. Abre el archivo `.pkg` y sigue las instrucciones de instalación.  
4. Verifica la instalación en la **Terminal**:
   ```bash
   python3 --version
   ```
   Debería mostrar `Python 3.10.x`.

### 1.3 Linux (Debian/Ubuntu)
1. Abre la **Terminal**.
2. Actualiza los repositorios:
   ```bash
   sudo apt update
   ```
3. Instala Python 3.10:
   ```bash
   sudo apt install python3.10
   ```
4. Verifica la instalación:
   ```bash
   python3.10 --version
   ```
   Debería mostrar `Python 3.10.x`.

---

## 2. Clonar o descargar el proyecto

Si tienes acceso a un repositorio remoto, clona el proyecto con:
```bash
git clone https://github.com/gguanilo-neural/firebird-sftp.git
cd firebird-sftp
```
O bien, descarga el proyecto como archivo ZIP y descomprímelo.

---

## 3. Crear un entorno virtual (virtual environment)

Para aislar las dependencias del proyecto y evitar conflictos con otras instalaciones de Python, se recomienda crear un **entorno virtual** con Python 3.10.  

### En Windows
```bash
python -m venv venv
```

### En macOS/Linux
```bash
python3 -m venv venv
```

Esto creará una carpeta llamada `venv` que contendrá los archivos necesarios para el entorno virtual.

---

## 4. Activar el entorno virtual

#### Windows (CMD o PowerShell)
```bash
venv\Scripts\activate
```

#### macOS/Linux
```bash
source venv/bin/activate
```

---

## 5. Crear un archivo `.env`

En la raíz del proyecto, crea un archivo llamado `.env` para configurar variables sensibles. Ejemplo:
```
# .env
FIREBIRD_HOST=127.0.0.1
FIREBIRD_PORT=3050
FIREBIRD_DATABASE=/firebird/data/db.fdb
FIREBIRD_USER=SYSDBA
FIREBIRD_PASSWORD=P@ssw0rd
```

---

## 6. Instalar dependencias

Con el entorno virtual activo, instala las dependencias definidas en `requirements.txt`:
```bash
pip install -r requirements.txt
```

---

## 7. Ejecución del proyecto

Ejecuta el proyecto con:
```bash
python main.py
```

---

¡Y listo! Ahora tienes el proyecto configurado y listo para ejecutarse.
