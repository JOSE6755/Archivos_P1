from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
app = Flask(__name__)

CORS(app)

DB_HOST = "localhost"
DB_NAME = "Calificacion"
DB_USER = "postgres"
DB_PASS = "password"

try:
    con = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST)
    
    cur = con.cursor(cursor_factory=RealDictCursor)
    
    print(con.status)
    

    @app.route("/")
    def hello():
      return "<h1 style='color:blue'>ESTAMOS EN EL LABORATORIO DE ARCHIVOS !</h1>"

#obtengo todos los registros de mi tabla movies que cree en mi BD
    @app.route('/cargarTemporal', methods=['POST'])
    def fetch_all_movies():
      
        cur.execute("""CREATE TABLE temporal (
        NOMBRE_CLIENTE          VARCHAR(200),
        CORREO_CLIENTE          VARCHAR(200),
        CLIENTE_ACTIVO          VARCHAR(200),
        FECHA_CREACION          VARCHAR(200),
        TIENDA_PREFERIDA        VARCHAR(200),
        DIRECCION_CLIENTE       VARCHAR(200),
        CODIGO_POSTAL_CLIENTE   VARCHAR(200),
        CIUDAD_CLIENTE          VARCHAR(200),
        PAIS_CLIENTE            VARCHAR(200),
        FECHA_RENTA             VARCHAR(200),
        FECHA_RETORNO           VARCHAR(200),
        MONTO_A_PAGAR           VARCHAR(200),
        FECHA_PAGO              VARCHAR(200),
        NOMBRE_EMPLEADO         VARCHAR(200),
        CORREO_EMPLEADO         VARCHAR(200),
        EMPLEADO_ACTIVO         VARCHAR(200),
        TIENDA_EMPLEADO         VARCHAR(200),
        USUARIO_EMPLEADO        VARCHAR(200),
        CONTRASENIA_EMPLEADO    VARCHAR(200),
        DIRECCION_EMPLEADO      VARCHAR(200),
        CODIGO_POSTAL_EMPLEADO  VARCHAR(200),
        CIUDAD_EMPLEADO         VARCHAR(200),
        PAIS_EMPLEADO           VARCHAR(200),
        NOMBRE_TIENDA           VARCHAR(200),
        ENCARGADO_TIENDA        VARCHAR(200),
        DIRECCION_TIENDA        VARCHAR(200),
        CODIGO_POSTAL_TIENDA    VARCHAR(200),
        CIUDAD_TIENDA           VARCHAR(200),
        PAIS_TIENDA             VARCHAR(200),
        TIENDA_PELICULA         VARCHAR(200),
        NOMBRE_PELICULA         VARCHAR(200),
        DESCRIPCION_PELICULA    VARCHAR(200),
        ANIO_LANZAMIENTO        VARCHAR(200),
        DIAS_RENTA              VARCHAR(200),
        COSTO_RENTA             VARCHAR(200),
        DURACION                VARCHAR(200),
        COSTO_POR_DANIO         VARCHAR(200),
        CLASIFICACION           VARCHAR(200),
        LENGUAJE_PELICULA       VARCHAR(200),
        CATEGORIA_PELICULA      VARCHAR(200),
        ACTOR_PELICULA          VARCHAR(200)
        );
        COPY PUBLIC.temporal from '/home/jose675/Escritorio/Archivos_flask/BlockbusterData2.csv' DELIMITER ';' CSV HEADER;
        """)
        con.commit()

        return "Tabla temporal creada"
    
    @app.route('/eliminarTemporal',methods=['POST'])
    def eliminarT():
        cur.execute('''
        drop table if exists public.temporal;
        ''')
        con.commit()
        return jsonify(msg='Tabla Temporal Eliminada')

    @app.route('/cargarModelo',methods=['POST'])
    def LlenarDatos():
        cur.execute('''
       create table pais (
        id_pais serial primary key not null,
        nombre varchar(200) not null
        );

        insert into pais(nombre) select distinct temporal.pais_cliente from temporal 
        union
        select distinct temporal.pais_empleado from temporal
        union
        select distinct temporal.pais_tienda from temporal
        where temporal.pais_cliente!='-'and temporal.pais_empleado!='-' and temporal.pais_tienda!='-';

        create table ciudad (
        id_ciudad serial primary key not null,
            nombre_ciudad varchar(200) not null,
            pais int references pais(id_pais)

        );


        insert into ciudad (nombre_ciudad,pais) 
        select distinct
        ciudad_cliente,id_pais from temporal as t inner join pais as p on p.nombre=t.pais_cliente
        union
        select distinct 
        ciudad_empleado,id_pais from temporal as t inner join pais as p on p.nombre=t.pais_empleado
        union 
        select distinct
        ciudad_tienda,id_pais from temporal as t inner join pais as p on p.nombre=t.pais_tienda
        where ciudad_cliente!='-' and ciudad_empleado!='-' and ciudad_tienda!='-';

        create table direccion (
            id_direccion serial primary key not null,
            codigo_postal varchar(30),
            direccion varchar(200),
            id_ciudad int references ciudad(id_ciudad)
        );

        insert into direccion (direccion,codigo_postal,id_ciudad)
        select distinct
        direccion_cliente, codigo_postal_cliente,id_ciudad from temporal as t inner join ciudad as c on c.nombre_ciudad=t.ciudad_cliente
        union
        select distinct 
        direccion_empleado,codigo_postal_empleado,id_ciudad from temporal as t inner join ciudad as c on c.nombre_ciudad=t.ciudad_empleado
        union
        select distinct
        direccion_tienda,codigo_postal_tienda,id_ciudad from temporal as t inner join ciudad as c on c.nombre_ciudad=t.ciudad_tienda;

        create table tienda (
        id_tienda serial primary key not null,
            encargado varchar(200),
            nombre varchar(200),
            direccion int references direccion(id_direccion)
        );

        insert into tienda(encargado,nombre,direccion) 
        select distinct 
        encargado_tienda,nombre_tienda, id_direccion from temporal inner join direccion as d on d.direccion=temporal.direccion_tienda where encargado_tienda!='-'and nombre_tienda!='-';




        create table empleados(
            id_empleado serial primary key not null,
            nombre varchar(200),
            apellido varchar(200),
            correo varchar(200),
            activo varchar(10),
            user_name varchar(200),
            passw varchar(200),
            tienda varchar(200),
            id_tienda int references tienda(id_tienda),
            direccion int references direccion(id_direccion)
                
        );

        insert into empleados(nombre,apellido,correo,activo,user_name,passw,tienda,id_tienda,direccion)
        select distinct
        split_part(t.nombre_empleado, ' ', 1) as nombre,split_part(t.nombre_empleado, ' ', 2) as apellido,correo_empleado,empleado_activo,usuario_empleado,contrasenia_empleado,tienda_empleado,id_tienda,id_direccion
        from temporal as t inner join tienda as c on c.nombre=t.nombre_tienda
        inner join direccion as d on d.direccion=t.direccion_empleado;

        create table clientes(
            id_cliente serial primary key not null,
            nombre varchar(200),
            apellido varchar(200),
            correo varchar(200),
            fecha_Reg varchar(200),
            activo varchar(10),
            tienda_fav varchar(200),
            tienda int references tienda(id_tienda),
            direccion int references direccion(id_direccion)
        );

        insert into clientes (nombre,apellido,correo,fecha_Reg,activo,tienda_fav,tienda,direccion)
        select distinct
        split_part(t.nombre_cliente, ' ', 1) as nombre,split_part(t.nombre_cliente, ' ', 2) as apellido,
        correo_cliente,fecha_creacion,cliente_activo,tienda_preferida,id_tienda,id_direccion
        from temporal as t inner join tienda as c on c.nombre=t.tienda_preferida
        inner join direccion as d on d.direccion=t.direccion_cliente;


        create table peliculas(
            id_pelicula serial primary key not null,
            titulo varchar(200),
            descripcion text,
            anio_lanzamiento varchar(10),
            duracion varchar(100),
            clasificacion varchar(100)
            
        );

        insert into peliculas(titulo,descripcion,anio_lanzamiento,duracion,clasificacion)
        select distinct
        nombre_pelicula,descripcion_pelicula,anio_lanzamiento,duracion,clasificacion
        from temporal where nombre_pelicula!='-';


        create table traduccion(
            id_traduccion serial primary key not null,
            lenguaje varchar(200)
            
        );

        insert into traduccion (lenguaje)
        select distinct lenguaje_pelicula
        from temporal as t where t.lenguaje_pelicula!='-';

        create table pelicula_traduccion(
            id_pelicula_traduccion serial primary key not null,
            id_pelicula int references peliculas(id_pelicula),
            id_traduccion int references traduccion(id_traduccion)
        );

        insert into pelicula_traduccion(id_pelicula,id_traduccion)
        select distinct id_pelicula,id_traduccion
        from temporal as t inner join peliculas as p on p.titulo=t.nombre_pelicula
        inner join traduccion as tr on tr.lenguaje=t.lenguaje_pelicula;

        create table actor(
            id_actor serial primary key not null,
            nombre varchar(200),
            apellido varchar(200)
        );


        insert into actor (nombre,apellido)
        select distinct
        split_part(t.actor_pelicula, ' ', 1) as nombre,split_part(t.actor_pelicula, ' ', 2) as apellido
        from temporal as t where t.actor_pelicula!='-';

        create table categoria(
            id_categoria serial primary key not null,
            categoria varchar(200)
        );

        insert into categoria(categoria)
        select distinct categoria_pelicula from temporal where categoria_pelicula!='-';

        create table pelicula_categoria(
            id_pelicula_categoria serial primary key not null,
            id_pelicula int references peliculas(id_pelicula),
            id_categoria int references categoria (id_categoria)
        );

        insert into pelicula_categoria(id_pelicula,id_categoria)
        select distinct id_pelicula, id_categoria
        from temporal as t inner join peliculas as p on p.titulo=t.nombre_pelicula
        inner join categoria as c on c.categoria=t.categoria_pelicula;


        create table tienda_peliculas(
            id_tienda_peliculas serial primary key not null,
            id_tienda int references tienda(id_tienda),
            id_pelicula int references peliculas(id_pelicula),
            tienda varchar(200),
            cantidad int 
        );

        insert into tienda_peliculas(id_tienda,id_pelicula,tienda,cantidad)
        select distinct ti.id_tienda, p.id_pelicula,t.tienda_pelicula,count(id_pelicula)
        from temporal as t inner join tienda as ti on ti.nombre=t.tienda_pelicula and t.tienda_pelicula!='-'
        inner join peliculas as p on p.titulo=t.nombre_pelicula and t.nombre_pelicula!='-'
        group by id_tienda,id_pelicula,tienda_pelicula;

        create table actor_pelicula(
            id_actor_pelicula serial primary key not null,
            id_actor int references actor(id_actor),
            id_pelicula int references peliculas(id_pelicula)
        );

        insert into actor_pelicula (id_actor,id_pelicula)
        select distinct id_actor,id_pelicula 
        from temporal as t inner join actor as ac 
        on 
        ac.nombre=split_part(t.actor_pelicula, ' ', 1) and ac.apellido=split_part(t.actor_pelicula, ' ', 2)
        inner join peliculas as p on p.titulo=t.nombre_pelicula;

        create table rentas(
            id_renta serial primary key not null,
            cantidad_pago decimal,
            fecha_pago varchar(200),
            dias_renta int,
            costo_renta decimal,
            cargo_danios decimal,
            fecha_renta varchar(200),
            fecha_dev varchar(200),
            id_empleado int references empleados(id_empleado),
            id_cliente int references clientes(id_cliente),
            id_pelicula int references peliculas(id_pelicula)
            
        );

        insert into rentas(cantidad_pago,fecha_pago,dias_renta,costo_renta,cargo_danios,fecha_renta,fecha_dev,id_empleado,id_cliente,id_pelicula)
        select distinct 
        monto_a_pagar::decimal,
        fecha_pago,
        dias_renta::int,
        costo_renta::decimal,
        costo_por_danio::Decimal,
        fecha_renta,
        fecha_retorno,
        id_empleado,id_cliente,id_pelicula
        from temporal as t inner join empleados as e on e.user_name=t.usuario_empleado
        inner join clientes as c on c.correo=t.correo_cliente
        inner join peliculas as p on p.titulo=t.nombre_pelicula
        where monto_a_pagar!='0' and fecha_pago!='-' and fecha_renta!='-';
        ''')
        con.commit()
        cur.close()
        return jsonify(msg='Tablas y Datos cargados a la Base')
    
    @app.route('/consulta1', methods=['GET'])
    def consulta1():
        cur.execute("""select sum(cantidad) as total 
        from tienda_peliculas as tp inner join peliculas as p
        on p.id_pelicula=tp.id_pelicula and p.titulo='SUGAR WONKA'""")
        rows = cur.fetchall()
        cur.close()
        print(rows)

        return jsonify(rows)
    
    @app.route('/consulta2', methods=['GET'])
    def consulta2():
        cur.execute("""select nombre,apellido, round(sum(cantidad_pago))::varchar as total
        from rentas as r inner join clientes as c on c.id_cliente=r.id_cliente
        group by nombre,apellido
        having count(r.id_cliente)>=40""")
        rows = cur.fetchall()
        cur.close()
        print(rows)

        return jsonify(rows)
    
    @app.route('/consulta3', methods=['GET'])
    def consulta3():
        cur.execute("""select nombre ||' '|| apellido as nombre
        from actor where apellido like '%son%' order by nombre""")
        rows = cur.fetchall()
        cur.close()
        print(rows)

        return jsonify(rows)

    @app.route('/consulta4', methods=['GET'])
    def consulta4():
        cur.execute("""select distinct nombre,apellido,anio_lanzamiento
        from actor_pelicula as ap
        inner join actor as a on a.id_actor=ap.id_actor
        inner join peliculas as p on p.id_pelicula=ap.id_pelicula
        where p.descripcion like '%Crocodile%' and p.descripcion like '%Shark%'
        order by apellido asc""")
        rows = cur.fetchall()
        cur.close()
        print(rows)

        return jsonify(rows)
    
    
  
    
    if __name__ == "__main__":
     app.run(debug=True)        

except:
    print('Error')


