"""
Program name: connection.py
By: Brandon I. Pérez Sandoval
Description: This program will let you
    make the whole CRUD of a MySQL Database.
    
Warning: You'll need the library PyMySQL and FPDF, so if you don't have them use these command:
    pip install pymysql 
    pip install fpdf
"""
import pymysql
import sys
from fpdf import FPDF

def connection():
    try:
        db = pymysql.connect(host="localhost",user="root",passwd="",db="resident")
    except:
        print(">>Error en la conexión")
    else:
        print("Conexión exitosa!")
        #Prepare a cursor object using cursor() method
        cursor = db.cursor()
        #Execute SQL query
        cursor.execute("Select VERSION()")
        #Procesing query
        data = cursor.fetchone()
        print("Database version: {0}".format(data))
        cursor.close()
        return db    
#--------------------------------CRUD--------------------------
def create_tables(db):
    try:
        cursor = db.cursor()
        tablesDatabase = get_tables(db)
        if(len(tablesDatabase) == 0):
            query_trabajador = ("""
                CREATE TABLE IF NOT EXISTS Trabajadores
                (clave INT NOT NULL AUTO_INCREMENT, 
                nombre VARCHAR(255) NOT NULL,
                sueldo FLOAT default 0,                
                PRIMARY KEY(clave))
                ENGINE = InnoDB
            """)

            cursor.execute(query_trabajador)
            print("Tabla creada exitosamente!")
            cursor.close() 
        else:
            print("Tablas creada con anterioridad!")
    except:
        db.rollback()
        print(">>Algo salió mal al crear la tabla!")
        print(sys.exc_info[0])           

def show_data(db):
    try:
        cursor = db.cursor()
        tablesDatabase = get_tables(db)
        if(len(tablesDatabase) != 0):
            while(True):
                n = int(input('¿Cuántos datos desea ver? '))
                if(n < 1):
                    print(">>Error! Debe ser mayor a 1")
                else:
                    break
                        
            print('\n'*2)
            print("Trabajadores")
            cursor.execute("SELECT * FROM Trabajadores LIMIT {0};".format(n))
            score_result = cursor.fetchall()
            for row in score_result:
                key = row[0]
                name = row[1]
                salary = row[2]
                print("\nClave: {0} \t Nombre: {1} \t Sueldo: {2}".format(key,name,salary))
            print("\nTiene {0} registros en total".format(count_records(db,'Trabajadores')))
            
            save = int(input("\n¿Desea generar un archivo PDF con estos datos? (1 - Sí \t 2 - No)\nIntroduzca un número: "))
            if(save == 1):
                print("Generando archivo PDF...")
                create_pdf(db,n)
            else:
                print("OK!")

            cursor.close()
        else:
            print("No hay tablas aún!")
    except ValueError:
        print(">>Error, debe introducir un dato numérico!")
    except:
        print(">>Error al mostrar contenido!")
        print(sys.exc_info()[0])

def insert_data(db):
    try:
        records_inserted = 0
        cursor = db.cursor()
        tablesDatabase = get_tables(db)
        if(len(tablesDatabase) != 0):

            n = int(input("¿Cuántos datos desea insertar? "))                                    
            for i in range(n):
                while(True):
                    try:
                        print('\n')
                        name = input('Inserte el nombre del trabajador {0}: '.format(i+1))                        
                        if(not name or len(name.strip(" ")) == 0):
                            raise TypeError
                        else:
                            break
                    except TypeError:
                        print('>>Inserte un nombre!')
                while(True):
                    try:
                        salary = input('Inserte el sueldo del trabajador {0}: $'.format(name))
                        if(not salary or len(salary.strip(" ")) == 0):
                            salary = 0
                            salary = float(salary)
                            print(">Sueldo grabado con $0")
                            break   
                        else:                         
                            salary = float(salary)   
                            break
                    except ValueError:
                        print(">>Salario es un campo numérico!")

                query_trabajador = ("""INSERT INTO Trabajadores(clave, nombre, sueldo)
                    VALUES (NULL,'{0}','{1}');
                """.format(name, salary))
                cursor.execute(query_trabajador)
                db.commit()
                records_inserted = records_inserted+1
            else:
                print("{0} datos insertados!".format(records_inserted))            
        else:
            print("No hay tablas aún!")
    except:
        db.rollback()
        print(">>Error al insertar contenido!")
        print(sys.exc_info()[0])
    else:
        cursor.close()

def del_data(db):
    try:
        cursor = db.cursor()
        tablesDatabase = get_tables(db)
        data = count_records(db,'Trabajadores')
        if(len(tablesDatabase) != 0 and data != 0):
            opt = int(input("Elija una opción\n\n1-Borrar todos los datos\t2-Borrar datos y tablas\t\t3-Borrar 1 dato\t\t0-Cancelar\n\nOpción: "))

            if(opt == 0):
                print("\n.:Cancelar:.\nNingún registro fue afectado")
            elif(opt == 1):
                print("\n.:Borrar todos los datos:.")

                confirmation = int(input('¡Advertencia, se borrarán todos los datos!\n¿Continuar? (0 - No\t 1 - Sí)\nOpción: '))
                if(confirmation == 0):
                    print("Abortando borrado de registros!")
                elif(confirmation == 1):
                    print("\nBorrando datos...")

                    query_trabajador = ("TRUNCATE TABLE Trabajadores;")
                    deleted_data = count_records(db,'Trabajadores')
                    cursor.execute(query_trabajador)

                    print("Se borraron {0} datos!".format(deleted_data))
                else:
                    print("Opción no válida, abortando borrado de registros!")
            elif(opt == 2):
                print("\n.:Borrar datos y tablas:.")

                confirmation = int(input('¡Advertencia, se borrarán todos los datos y las tablas también!\n¿Continuar? (0 - No\t 1 - Sí)\nOpción: '))
                if(confirmation == 0):
                    print("Abortando borrado de registros!")
                elif(confirmation == 1):
                    print("\nBorrando datos...")
                    
                    deleted_data = count_records(db,'Trabajadores')
                    deleted_tables = len(get_tables(db))
                    
                    drop_tables(db)

                    print("Se borraron {0} datos y {1} tablas".format(deleted_data, deleted_tables))
            elif(opt == 3):
                print("\n.:Borrar 1 dato:.")

                while(True): 
                    try:                   
                        id = (input("Escriba la 'Clave' del usuario que desee eliminar: "))                    
                        if(not id or len(id.strip(" ")) == 0):
                            raise TypeError
                        elif(int(id) < 1):
                            raise TypeError
                        else:
                            id = int(id)
                            break
                    except TypeError:                        
                        print(">>Clave incorrecta!")
                    except ValueError:
                        print(">>La Clave es un valor numérico entero!")

                query_trabajador = ("DELETE FROM `trabajadores` WHERE clave = {0}".format(id))
                cursor.execute(query_trabajador)
                data_now = count_records(db,'Trabajadores')                
                print("Se eliminaron {0} dato(s)!".format(data-data_now))
            else:
                print("Opción no válida, abortando borrado de registros!")
        else:
            print("No hay tablas o registros aún!")
    except:
        db.rollback()
        print(">>Error al borrar datos! Operación cancelada!")
        print(sys.exc_info()[0])
    else:
        cursor.close()

def search_data(db):
    try:
        tablesDatabase = get_tables(db)
        data = count_records(db,'Trabajadores')
        if(len(tablesDatabase) != 0 and data != 0):
            while(True):
                opt = int(input("\n¿Por qué atributo desea hacer la búsqueda?\n1.- Clave\t2.- Nombre\t3.- Sueldo\nInserte un número entre 1 y 3: "))

                if(opt == 1):
                    while(True):
                        try:                
                            id = int(input("Ingrese la clave que desea encontrar: "))    
                            break    
                        except ValueError:
                            print(">>El dato clave es numérico")
                    searching_data(db, 'clave', id)
                    break
                elif(opt == 2):        
                    name = input("Ingrese el nombre que desea encontrar: ")
                    searching_data(db,'nombre', name)
                    break
                elif(opt == 3):
                    while(True):
                        try:
                            salary = float(input("Ingrese el salario que desea encontrar: $"))
                            break            
                        except ValueError:
                            print(">>El dato salario es numérico")
                    searching_data(db,'sueldo', salary)
                    break
                else:
                    print(">>Error, opción no reconocida!\nIntente de nuevo")
        else:
            print("No hay tablas o registros aún!")
    except:
        print(">>Error al buscar datos!")
        print(sys.exc_info[0])

def up_data(db):
    try:
        cursor = db.cursor()
        tablesDatabase = get_tables(db)
        data = count_records(db,'Trabajadores')
        if(len(tablesDatabase) != 0 and data != 0):
            while(True):
                opt = int(input("\n¿Qué atributos desea actualizar?\n1.- Nombre\t2.- Sueldo\t3.- Los dos\nInserte un número entre 1 y 3: "))
                if(opt == 1):
                    print("¡Actualizar nombre!")
                    id = int(input("Ingrese la clave del trabajador: "))
                    exist = count_specific_records(db, 'Trabajadores', id)
                    if(exist):
                        name = input("Ingrese el nuevo nombre: ")
                        query_name = ("""
                            UPDATE Trabajadores
                            SET nombre  = '{0}'
                            WHERE clave = {1};
                        """.format(name, id))
                        cursor.execute(query_name)
                        db.commit()
                        print("Usuario actualizado!")
                    else:
                        print("Trabajador con clave {0} no existe!".format(id))
                    break
                elif(opt == 2):
                    print("¡Actualizar sueldo!")
                    id = int(input("Ingrese la clave del trabajador: "))
                    exist = count_specific_records(db, 'Trabajadores', id)
                    if(exist):
                        salary = float(input("Ingrese el nuevo salario: "))
                        query_salary = ("""
                            UPDATE Trabajadores
                            SET sueldo = {0}
                            WHERE clave = {1};
                        """.format(salary, id))
                        cursor.execute(query_salary)
                        db.commit()
                        print("Usuario actualizado!")
                    else:
                        print("Trabajador con clave {0} no existe!".format(id))
                    break
                elif(opt == 3):
                    print("¡Actualizar nombre y sueldo!")
                    id = int(input("Ingrese la clave del trabajador: "))
                    exist = count_specific_records(db, 'Trabajadores', id)
                    if(exist):
                        name = input("Ingrese el nuevo nombre: ")
                        salary = float(input("Ingrese el nuevo salario: "))
                        query_both = ("""
                            UPDATE Trabajadores
                            SET nombre = '{0}', sueldo = {1}
                            WHERE clave = {2};
                        """.format(name, salary, id))
                        cursor.execute(query_both)
                        db.commit()
                        print("Usuario actualizado!")
                    else:
                        print("Trabajador con clave {0} no existe!".format(id))
                    break
                else:
                    print("Opción no válida, intente de nuevo")
    except ValueError:
        db.rollback()
        print(">>Error, entrada no válida!")
    except SystemError as r:
        db.rollback()
        print(">>Error al actualizar datos!")
        print(r)
    else:
        cursor.close()
#--------------------------------CRUD--------------------------
#------------------------------HELPERS-------------------------
def get_tables(db):
    cursor = db.cursor()
    tablesDatabase = []
    cursor.execute("SHOW TABLES;")
       
    if (cursor.rowcount != 0):
        tablesDatabase = [table[0] for table in cursor]

    cursor.close()
    return tablesDatabase

def drop_tables(db):
    try:        
        cursor = db.cursor()

        d_tables = ("""
            drop table IF EXISTS Trabajadores;                    
        """)
        cursor.execute(d_tables)
        print("Tablas borradas!")
        cursor.close()
    except:
        db.rollback()
        print(">>Error al borrar tablas")
        print(sys.exc_info()[0])        

def count_records(db, table):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(*) FROM {0}".format(table))
        items = cursor._rows[0][0]
        cursor.close()
        return (items)
    except:
        print(">>Error al contar registros!")

def count_specific_records(db, table, id):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(*) FROM {0} WHERE clave = {1}".format(table, id))
        items = cursor._rows[0][0]
        cursor.close()
        return (items)
    except:
        print(">>Error al contar registros!")

def searching_data(db, field, data):
    try:
        cursor = db.cursor()
        if(field == 'nombre'):
            query = ("SELECT * FROM Trabajadores WHERE {0} = '{1}'".format(field, data))
        else:
            query = ("SELECT * FROM Trabajadores WHERE {0} = {1}".format(field, data))
        cursor.execute(query)
        items = len(cursor._rows)    
        score_result = cursor.fetchall()
        print("\n\nTrabajadores")
        for row in score_result:
            key = row[0]
            name = row[1]
            salary = row[2]
            print("\nClave: {0} \t Nombre: {1} \t Sueldo: {2}".format(key,name,salary))
        print("Se encontraron {0} filas".format(items))
        cursor.close()
    except:
        print(">>Error al imprimir los datos!")
        print(sys.exc_info[0])
#------------------------------HELPERS-------------------------
#------------------------------PDF-------------------------
def create_pdf(db, limit):        
    try:
        cursor = db.cursor()

        #PDF preparation
        pdf = FPDF(orientation='P',unit='mm',format='A4')
        pdf.add_page()
        pdf.set_font('helvetica','B',15)
        pdf.cell(80)
        pdf.cell(30,10,'Registros','B',1,'C')
        pdf.ln('20')

        cursor.execute("SELECT * FROM Trabajadores LIMIT {0};".format(limit))
        score_result = cursor.fetchall()

        pdf.set_font('Arial','',12)
        for row in score_result:
            key = row[0]
            name = row[1]
            salary = row[2]
            pdf.cell(0,10,'Clave: {0} | Nombre: {1} | Sueldo: {2}'.format(key,name,salary),0,1)
        pdf.set_font('Arial','B',12)
        pdf.ln(20)
        pdf.cell(50)
        pdf.cell(70,10,'Tiene {0} registros en total'.format(count_records(db,'Trabajadores')),1,0,'C')
        cursor.close()
        print("PDF generado exitosamente!!!")
    except:        
        print(">>Error al hacer PDF")
        print(sys.exc_info[0])
    else:
        pdf.output("registros.pdf")
#------------------------------PDF-------------------------
#-------------------------------MAIN---------------------------
def main():
    try:
        db = connection()
        while True:
            print("\n\t\t\t .:Menú principal:.")
            print("\t\t  --Por favor, elija un opción--")
            opt = int(input("""1.- Crear tablas \t 2.- Mostrar datos \t 3.- Insertar datos  \n
4.- Buscar datos \t 5.- Actualizar datos \t 6.-Borrar datos
\n\t\t\t 0.- Salir\nOpción: """))
            if(opt == 0):
                print("Hasta luego!")
                break
            elif(opt == 1):
                print("\n\n.:Crear tablas:.")
                create_tables(db)
            elif(opt == 2):
                print("\n\n.:Mostrar datos:.")
                show_data(db)
            elif(opt == 3):
                print("\n\n.:Insertar datos:.")
                insert_data(db)
            elif(opt == 4):
                print("\n\n.:Buscar datos:.")
                search_data(db)
            elif(opt == 5):
                print("\n\n.:Actualizar datos:.")
                up_data(db)
            elif(opt == 6):
                print("\n\n.:Borrar datos:.")
                del_data(db)
            else:
                print("Opción incorrecta, intente de nuevo!")
    except:
        print(">>Main_Error")
    finally:
        db.close()

if __name__ == '__main__':
    main()
