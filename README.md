# Proyecto_Big_Data

***kafka_wordcount.py :*** archivo que se utiliza con Spark para el consumo en Kafka y procesamiento de datos

***producer12.py :*** Archivo que se encarga de leer el dataset, generar nuevos datos y enviarlos a Kafka para su posterior consumo. El 12 es debido a que esta dirigido a la VM con ese numero

***consumer.py :*** Se encarga de consumir los datos desde Kafka una vez que estos fueron procesados en Spark

***spark-streaming-kafka... .jar :*** Archivo necesario para que Spark pueda crear el Job con el script de python.
