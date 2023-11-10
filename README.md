# mindtickle_assignment

Clone the repository and follow the below steps:

1. python and config file is inside the mindtickle folder. 
2. config.json file conatins all the input parameters required to connect to postgre db and mysql db.
3. To load output file to azure blob storage, uncomment the blob_connection function and lines below the 'save file to azure blob storage'.
4. Output csv file will be generated and saved at the same location from where code is run.

Required Lib:

pip install mysql-connector-python
pip install psycopg2
pip install pandas

Extras:

1. Instead of config we can use env file to save our password and username.
2. As I don't have azure account I created a function to create blob connection but commented it.
   
