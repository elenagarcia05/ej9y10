# Esto es un ejemplo de como podemos usar python
# para conectarnos a una base de datos de postgres
# y realizar consultas a la misma.

# para instalar la libreria psycopg2
# pip install psycopg2-binary

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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("Programa para conectar a una base de datos de postgres")
    consultar("final.personafinal",10)
    consultar("final.vehiculofinal",10)
    consultar("final.collision_crashes_final",10)
    print("Fin del programa")