# BackupDB

Simply backup and restore MySQL database regarding tables and columns existence.

## The need

My need was the database consistency  between the application (started with with Django 0.96) 
and now. I couldn't backup the database with `python manage.py dumpdata`  and restore it with 
`python manage.py loaddata`!

So, I wrote this simple script.


## Usage

The `backupdb.py` script have several options

```
--backup : do a backup
--restore : Do a restoration
--directory DIR : Set the backup directory or where the backup files lives
--file CFG_FILE : Set the configuration file
--verbose : Display what the script is doing instead of nothing
``` 

You can mix both backup and restore action. First the script will backup, then it will 
restore data.

## Configuration
  
You need to fill a file: `config.cfg`  with three keys:

* `[ŧables]`: a liste of all tables to backup or restore.
* `[backup]`: whcich contains several key
    * host: the server host name (default=localhost)
    * user: the username server
    * password: the user password on the server
    * port: If the MySQL port is not the default one (default=3306)
    * database: the database name 
* `[restore]`: Same options than backup

## Django specific

For Django the procedure is :

1) Backup the database
    
    
    ./backup.py --backup --directory data
    
    
2) Create the tables with 


    ./manage.py migrate

3) Flush the database 


    ./manage.py flush
     
But the database is not empty. Open a connection to you server:

    mysql -uYOUR_NAME -p YOUR_DATABASE

then type

    DELETE FROM django_content_type;
    DELETE FROM auth_user;
    DELETE FROM django_site;

4) Restore the database

   
    ./backup.py --restore --directory data
    

## Help? 

Please fell free to ask some feature or to contribute to this project