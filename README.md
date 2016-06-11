#Práctica 4: Procesamiento paralelo con Hadoop

 El objetivo de esta práctica es desarrollar una aplicación con Hadoop que se pueda ejecutar de manera distribuida. Por simplicidad, consideraremos el modo de ejecución standalone, que simula el clúster de Hadoop sobre una única máquina virtual de Java.

 Los ficheros 1987.csv y 1988.csv (http://stat-computing.org/dataexpo/2009/the-data.html) contienen los datos de todos los vuelos realizados en los años 1987  y  1988,  respectivamente.  Se  trata  de  ficheros  en  formato  CSV  (Comma  Separated Values), donde el separador es el carácter ‘,’. La primera línea contiene el nombre de cada campo, y el resto contiene los respectivos valores.

 Estamos interesados en extraer información de utilidad de dichos ficheros, y, debido a su tamaño, es necesario pensar en estrategias de proceso típicas de Big Data. En concreto, se propone obtener:

1.  Para cada año, el retraso acumulado en cuanto al despegue y al aterrizaje.

2.  Cuántos vuelos se han producido en cada mes (considerando ambos años).

3.  Cuántos vuelos han salido cada hora (considerando ambos años).

4.  La distancia recorrida acumulada para cada día de la semana.

5.  Cuántos vuelos han salido de cada origen diferente.

Para ello, se utilizará el API de Java de Hadoop en modo standalone. Para cada punto de información a extraer, se debe proporcionar una clase mapper, una reducer, así como un programa principal que crea la tarea y la configuración y la envía a ejecutar.





