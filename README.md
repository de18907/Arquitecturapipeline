# Arquitecturapipeline
![image](https://github.com/user-attachments/assets/970fc2fa-28a3-4ec5-b982-65e06678cef6)


1- Descargamos docker desde la pagina official. Descargamos Docker Destop.
https://www.docker.com/
![image](https://github.com/user-attachments/assets/bb957995-2cc8-4fde-9a2d-65f730922216)


2- Ya descargado lo abrimos y le damos a siguiente. Iniciamos seccion.


3- En la pagina oficial de Airbyte lo descargamos abctl para ser instalado.
https://docs.airbyte.com/using-airbyte/getting-started/oss-quickstart?_gl=1*157wgtu*_gcl_au*MTM3MzUxODI0NC4xNzQwNjc3MjEz


4- En el buscador de windows escribimos Environment Variables para asi dentro de path y agregamos la ruta de donde esta el archivo. Tomar en cuenta poner de 3re lugar.
![image](https://github.com/user-attachments/assets/427ede49-e725-4fb1-9d85-73bd91114b2d)


5- En PowerShell corremos abctl local credentials para ver las credenciales.


6- En PowerShell corremos abctl local install --low-resource-mode para desplegar en un contenedor en docker.


7- Cuando finalice, validar que este corriendo y entrar a la ruta configurada.
http://localhost:8000/


8- En un mismo docker-compose se configuro para el funcionamiento AirFlow, PostgreSQL, dentro de la carpeta Proyecto seria corrre docker-compose up
PostgreSQL: http://localhost:5432/
airflow: http://localhost:8080/


9- Script Pyhton, dentro de la carpeta python se ejecuta los siguientes comandos (los cuales estos funcionaran para que sea instalado en docker al momento de subir el proyecto) 
docker build -t imagen_api_yfinance .
docker run -d -p 3000:3000 -v ${pwd}:/code imagen_api_yfinance
