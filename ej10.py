# Importamos la libreria psycopg2
import psycopg2
import random

# Creamos una funcion que se conecta a la base de datos
def conectar():
    # Creamos una variable con los datos de la conexion
    conn=None
    print("Conectando a la base de datos")
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="pecl2",
            user="postgres",
            password="0Elena0",
            port="5432"
        )

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    # Retornamos la conexion
    return conn



# Creamos una funcion que realiza una consulta a la base de datos
# y nos sólo retorna los nresult resultados por pantalla
def consultar(tabla, nresult):
    print("Consultando la base de datos" + tabla + " con " + str(nresult) + " resultados")
    # Creamos una variable con la conexion
    conn = conectar()
    try:
        # Creamos una variable con el cursor
        cur = conn.cursor()
        # Creamos una variable con la consulta
        cur.execute("SELECT * FROM "+tabla+" FETCH FIRST " + str(nresult)+ " ROWS ONLY;")
        # Creamos una variable con el resultado de la consulta
        rows = cur.fetchall()
        # Creamos una variable con el resultado de la consulta
        resultado = []
        # Recorremos el resultado de la consulta
        for row in rows:
            # Creamos una variable con el resultado de la consulta
            registro = {}
            # Recorremos las columnas del resultado
            for i in range(len(cur.description)):
                # Creamos una variable con el nombre de la columna
                nombre_columna = cur.description[i].name
                # Creamos una variable con el valor de la columna
                valor_columna = row[i]
                # Agregamos al registro la columna y el valor
                registro[nombre_columna] = valor_columna
            # Agregamos el registro al resultado
            resultado.append(registro)
        # Recorremos los nresult primeros registros
        for i in range(nresult):
            # Imprimimos el registro
            print(resultado[i])
        # Cerramos el cursor
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            # Cerramos la conexion
            conn.close()

# creamos una funciona que realiza una consulta a la base de datos basandose en el campo y el valor que le pasamos
def consultar_campo(tabla, campo, valor):
    print("Consultando la base de datos" + tabla + " con " + campo + " = " + valor)
    # Creamos una variable con la conexion
    conn = conectar()
    try:
        # Creamos una variable con el cursor
        cur = conn.cursor()
        # Creamos una variable con la consulta
        if valor.isnumeric():
            cur.execute("SELECT * FROM "+tabla+" WHERE "+campo+" = "+valor+";")
        else:
            cur.execute("SELECT * FROM "+tabla+" WHERE "+campo+" = '"+valor+"';")
        # Creamos una variable con el resultado de la consulta
        rows = cur.fetchall()
        # Creamos una variable con el resultado de la consulta
        resultado = []
        # Recorremos el resultado de la consulta
        for row in rows:
            # Creamos una variable con el resultado de la consulta
            registro = {}
            # Recorremos las columnas del resultado
            for i in range(len(cur.description)):
                # Creamos una variable con el nombre de la columna
                nombre_columna = cur.description[i].name
                # Creamos una variable con el valor de la columna
                valor_columna = row[i]
                # Agregamos al registro la columna y el valor
                registro[nombre_columna] = valor_columna
            # Agregamos el registro al resultado
            resultado.append(registro)
        # Recorremos los nresult primeros registros
        for i in range(len(resultado)):
            # Imprimimos el registro
            print(resultado[i])
        # Cerramos el cursor
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            # Cerramos la conexion
            conn.close()

if __name__ == '__main__':
    collision_id = input("Introduce el collision_id del accidente que deseas consultar: ")
    consultar_campo("final.collision_crashes_final", "collision_id", collision_id)
    consultar_campo("final.collision_vehicles_final", "collision_id", collision_id)
    consultar_campo("final.collision_crashes_final", "collision_id", collision_id)

    print("Fin del programa")