# boopDB
boopdb is a file manager developed specifically for those who don't want to learn all of that wacky json file nonsense. Inspired by SQLAlchemy's ORM, boopDB literally just handles the creation, reading, and updating of any files.
##### boopDB is also specifically made for discord applications.
---
In order to use this package you want to call the mainhandler class, which is appropriately named `SuckMyTinyPenis`. 
```py
import discord
import boopdb

myBot = discord.Client()
myBot.db = boopdb.boopDB() # Sets an attribute so you can use the database within any cog!
```
The code seen above will basically create an empty instance of the database. (Meaning that there are no actual tables inside the system yet!). You may need to create a separate script, or if you're wiser, use a terminal to import the package and create the tables manually. Below is a simple example using a script :)
```py
from boopdb import DatabaseTable, boopDB

# A list to hold all of your tables
all_of_my_wacky_tables = []

# Creating the table
myTable = DatabaseTable(
	"economy",
	perGuild = False # This is optional.
)

# Adding columns
myTable.addColumn("userid", int)
myTable.addColumn("money", int)

# Adding the table to the wacky table list.
all_of_my_wacky_tables.append(myTable)

myDatabase = boopDB(
	tables = all_of_my_wacky_tables # This will initialize the tables in the system.
)
```
