package examen


import examen.Examen._
import utils.TestInit

class examenTest extends TestInit{

  import spark.implicits._
  val in = Seq(
    ("Pepe", 67, 7.0),
    ("Juan", 56, 7.2),
    ("Ruben", 30, 10.0), //Guiño, guiño ;)
    ("Alejandro", 25, 9.0),
    ("Elena", 27, 9.5),
    ("Marta", 22, 5.0),
    ("Juncal", 44, 8.75),
    ("Homer", 38, 1.0),
    ("Batman", 35, 10.0)
  ).toDF("nombre", "edad", "calificacion")

  "schemaDataframe" should "mostrar el esquema de la tabla" in {
    schemaDataframe(in)
  }

  "filterDataframe" should "mostrar las calificaciones mayores a 8" in {
    filterDataframe(in).count() shouldBe 5
    filterDataframe(in).show()
  }

  "orderDataframe" should "ordenar por calificacion de mayor a menor los nombres" in {
    orderDataframe(in).show()
  }

  "ejercicio1" should "realizar las 3 funciones anteriores" in {
    ejercicio1(in)(spark).show()
  }

  "ejercicio2" should "indica si la columna edad es par o impar" in {
    ejercicio2(in)(spark).show()
  }

  "ejercicio3" should "hacer un join y calcular la calificacion promedio por estudiante" in {
    val estudiantes = Seq(
      ("Pepe", 3),
      ("Juan", 2),
      ("Ruben", 1)
    ).toDF("nombre", "id")

    val calificaciones = Seq(
      ("Ingles", 1, 7.0),
      ("Mate", 1, 7.2),
      ("Lengua", 1, 10.0),
      ("Ingles", 2, 9.0),
      ("Mate", 2, 9.5),
      ("Lengua", 2, 5.0),
      ("Ingles", 3, 8.75),
      ("Mate", 3, 1.0),
      ("Lengua", 3, 10.0)
    ).toDF("asignatura", "id_estudiante", "calificacion")

    ejercicio3(estudiantes,calificaciones)(spark).show()
  }

  "ejercicio4" should "contar las palabras de un RDD" in {
    val palabras = List(
      "kakarotto", "tu", "eres", "un", "guerrero", "admirable", "me", "acabo", "de", "dar", "cuenta",
      "que", "yo", "no", "sirvo", "para", "pelear", "con", "majin-bu", "tu", "eres", "el", "unico",
      "que", "puede", "derrotarlo", "kakarotto", "la", "primera", "vez", "que", "te", "vi", "fue",
      "cuando", "estaba", "en", "busca", "de", "planetas", "con", "un", "excelente", "ambiente", "para",
      "despues", "venderlos", "recuerda", "su", "pelea", "con", "goku", "a", "partir", "de", "ese",
      "momento", "solamente", "vivia", "para", "cumplir", "un", "objetivo", "el", "cual", "era",
      "superar", "los", "poderes", "de", "kakarotto", "existia", "una", "leyenda", "la", "cual",
      "decia", "queun", "super", "saiyajin", "aparecio", "entre", "1000", "si", "la", "leyenda",
      "llegara", "a", "ser", "cierta", "yo", "seria", "el", "unico", "capaz", "de", "convertirme",
      "en", "ese", "ser", "superdotado", "kakarotto", "es", "un", "guerrero", "de", "baja", "clase",
      "seria", "absurdo", "que", "el", "fuera", "ese", "super", "saiyajin", "yo", "pude", "despertar",
      "el", "super", "saiyajin", "que", "habia", "en", "mi", "al", "haber", "desatado", "mi", "furia",
      "estaba", "emocionado", "pense", "que", "finalmente", "habia", "superado", "a", "kakarotto",
      "y", "esos", "momentos", "que", "tuve", "alguna", "vez", "como", "principe", "saiyajin", "habian",
      "regresado", "pero", "a", "fin", "de", "cuentas", "no", "logre", "superarte", "al", "principio",
      "pense", "que", "era", "tu", "obligacion", "proteger", "a", "tus", "seres", "queridos", "y",
      "a", "causa", "de", "eso", "un", "poder", "totalmente", "desconocido", "brotaba", "en", "tu",
      "corazon", "quiza", "tenga", "razon", "ahora", "yo", "tengo", "el", "mismo", "deber", "piensa",
      "en", "bulma", "y", "en", "trunks", "antes", "yo", "peleaba", "porque", "todos", "hicieran",
      "mi", "voluntad", "por", "diversion", "era", "una", "delicia", "para", "mi", "matar", "a", "la",
      "gente", "y", "sobre", "todo", "para", "fortalecer", "mi", "orgullo", "pero", "kakarotto", "tu",
      "eres", "diferente", "no", "peleabas", "solamente", "para", "ganar", "siempre", "sobrepasaste",
      "los", "limites", "de", "tus", "fuerzas", "para", "no", "perder", "ante", "nadie", "por", "eso",
      "tu", "nunca", "te", "atreviste", "a", "matar", "a", "tus", "oponentes", "lo", "se", "porque",
      "tu", "nunca", "te", "atreviste", "a", "matarme", "parece", "ser", "que", "finalmente",
      "he", "comprendido", "que", "en", "mi", "corazon", "hay", "un", "poco", "de", "sentimiento",
      "que", "suelen", "tener", "los", "humanos", "pero", "no", "puedo", "soportar", "la", "idea",
      "de", "que", "existe", "un", "saiyajin", "generoso", "que", "le", "gusta", "pelear", "tu",
      "puedes", "kakarotto", "eres", "el", "numero", "uno"
    )

    ejercicio4(palabras)(spark).foreach(println)
  }

  "ejercicio5" should "calcular el ingreso total por producto" in {
    val ventas = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("src/test/resources/examen/ventas.csv")
    ejercicio5(ventas)(spark).show()
  }

}
