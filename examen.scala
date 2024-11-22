package examen


import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object Examen {

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
  Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
              estudiantes (nombre, edad, calificación).
            Realiza las siguientes operaciones:

            Muestra el esquema del DataFrame.
            Filtra los estudiantes con una calificación mayor a 8.
            Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */

  def schemaDataframe(df: DataFrame): Unit = {
    df.printSchema()
  }

  def filterDataframe(df: DataFrame): DataFrame = {
    df.filter("calificacion > 8")
  }

  def orderDataframe(df: DataFrame): DataFrame = {
    df.select("nombre").orderBy(desc("calificacion"))
  }


  def ejercicio1(estudiantes: DataFrame)(spark:SparkSession): DataFrame = {
    schemaDataframe(estudiantes)
    val filtrados = filterDataframe(estudiantes)
    orderDataframe(filtrados)
  }



  /**Ejercicio 2: UDF (User Defined Function)
  Pregunta: Define una función que determine si un número es par o impar.
            Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame)(spark:SparkSession): DataFrame =  {

    val udfEvenOrOdd = udf ((numero: Int) =>
      if (numero % 2 == 0) "Par"
      else "Impar"
    )

    numeros.withColumn("edad_type", udfEvenOrOdd(col("edad")))

  }


  /**Ejercicio 3: Joins y agregaciones
  Pregunta: Dado dos DataFrames,
            uno con información de estudiantes (id, nombre)
            y otro con calificaciones (id_estudiante, asignatura, calificacion),
            realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
  */
  def ejercicio3(estudiantes: DataFrame , calificaciones: DataFrame)(spark:SparkSession): DataFrame = {
    estudiantes.createOrReplaceTempView("estudiantes")
    calificaciones.createOrReplaceTempView("calificaciones")
    val query =
    """
     SELECT est.id,
            est.nombre,
            ROUND(AVG(cal.calificacion),2) AS calificacion_avg
     FROM estudiantes est
     JOIN calificaciones cal
        ON est.id = cal.id_estudiante
     GROUP
        BY est.id,
           est.nombre
    """

    spark.sql(query)
  }

  /**Ejercicio 4: Uso de RDDs
  Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

  */

  def ejercicio4(palabras: List[String])(spark:SparkSession): Array[(String, Int)] = {

    val rdd = spark.sparkContext.parallelize(palabras)
    rdd
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect() // Lo he llevado a un array porque sino no me lo ordenaba
  }


  /**
  Ejercicio 5: Procesamiento de archivos
  Pregunta: Carga un archivo CSV que contenga información sobre
            ventas (id_venta, id_producto, cantidad, precio_unitario)
            y calcula el ingreso total (cantidad * precio_unitario) por producto.
  */
  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {

    ventas.createOrReplaceTempView("ventas")
    val query =
      """
     SELECT id_producto,
            ROUND(SUM(cantidad*precio_unitario),2) AS importe_total
     FROM ventas
     GROUP
        BY id_producto
     ORDER
        BY importe_total DESC
    """

    spark.sql(query)
  }

}
