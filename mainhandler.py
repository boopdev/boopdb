import json
import asyncio
from typing import Optional, List, Union
from discord import Client, Guild
import logging

from os.path import exists
from os import mkdir

from .classes import DatabaseColumn, DatabaseTable, QueryHandler
from .merges import SchemaHandler

boopdb_logging_formatting = logging.Formatter('%(asctime)s | %(name)s> %(levelname)s %(message)s')

class boopDB(object):

    def logging_get_handler(self):
        """
            Gets the logging handler
        """
        handler = logging.FileHandler("boopdb.log", mode="w", encoding="utf-8")
        handler.setFormatter(boopdb_logging_formatting)

        return handler

    def setup_logging(self):
        """
            Sets up logging for this system.
        """

        logger = logging.getLogger("BOOPDB")
        logger.setLevel(logging.INFO)
        logger.addHandler(self.logging_get_handler())

        return logger



    def __init__(
            self, 
            *,
            client : Optional[Client] = None,
            tables : Optional[List[DatabaseTable]] = None,
            alternative_root_name : Optional[str] = None

        ) -> None:
        """
        Creates a wacky dandy thing that has functions to help you do filesystem
        stuff, without any knowledge of knowing how to do file system stuff
        """

        # First we assign basic data to this object
        self.client = client


        self.logger = self.setup_logging()
        print(self.logger, type(self.logger), sep="\t")
        self.logger.info("Setting things up!")


        # Allowing a user to set a custom root name
        self.root_name = alternative_root_name

        if self.root_name is None: # Defaulting if one doesn't exist.
            self.root_name = "boopDatabase"

        # After we set the root name, we ensure that that file directory exists.
        self.ensureRootFilesystem()


        self.logger.info("Set database root_name to: %s" % self.root_name)


        # Oh boy, here we go.
        self._raw_table_data = tables

        # Ensure that all tables know that they're in the system. References for good measure never hurts
        for table in self._raw_table_data:
            table._database = self
            table._init_post_database_assign()

        # Setting up a simple reference for when we actually fetch stuff from this database
        self.tableRef = {table.name : table for table in self._raw_table_data}

        self.logger.info("Found %s tables" % len(self.tableRef))

        SchemaHandler(self).compare_schema()
        
        

    def ensureRootFilesystem(self) -> None:
        """
            Ensures that the root directory actually does exists.
            If it doesn't, it creates the new folder.
            If it does exist, this function does an epic backflip.
        """

        if exists(f'./{self.root_name}/'):
            # The sickest backflip you've ever seen.
            self.logger.critical("Doing an epic backflip")

        else:
            # Creating the directory if it doesnt exist.
            mkdir(f'./{self.root_name}')


    def fetchDataFromTable(self, table : Union[DatabaseTable, str], *, guild : Optional[Guild] = None) -> QueryHandler:
        """
            Fetches data from a table, returns a QueryHandler.
        """

        if not isinstance(table, DatabaseTable):

            # Return an error if the table doesn't even exist.
            if table.lower() not in self.tableRef.keys():
                raise ValueError("The table name provided is not in the reference, maybe you spelt it wrong!")
                return

            table = self.tableRef[table.lower()]

        if table.per_guild:
            
            if guild is None:
                raise ValueError("No guild provided for guild-specific tableset. Make sure you actually apply that kwarg.")


            # If the guild file doesn't exist, make it exist
            if not exists(table.fullFilePath % {"guildid" : guild.id}):

                self.logger.info("Creating guild file in table %s for guild: %s" % (table.name, guild.name))
                with open(table.fullFilePath % {"guildid" : guild.id}, mode="w+") as f:
                    json.dump([], f)
                

            # Opening the specific guild file
            with open(table.fullFilePath % {"guildid" : guild.id}) as j:
                data = json.load(j)

        else:

            with open(table.fullFilePath) as j:
                data = json.load(j)

        return QueryHandler(table=table, results=data, guild=guild)
            
            

    def insertDataIntoTable(self, table : str, data : list, *, guild : Optional[Guild] = None) -> QueryHandler:
        """
            Inserts data into a table
        """
        
        # Allowing the use of actual table objects in case anybody would want to do that
        if not isinstance(table, DatabaseTable):

            # Return an error if the table doesn't even exist.
            if table.lower() not in self.tableRef.keys():
                raise ValueError("The table name provided is not in the reference, maybe you spelt it wrong!")
                return

            table = self.tableRef[table.lower()]

        # Ensuring the user provided a proper list of shit to insert
        if not len(data) == len(table.columns):
            raise ValueError(f"Insertion error occured when inserting {len(data)} values into database `{table.name}`, but it only supports {len(table.columns)}")

        # Ensure that the types are correct for all values as well.
        for i, d in enumerate(data, start=0):
            
            if not isinstance(d, table._columnTypeReference[i]):
                raise ValueError(f"Value `{d}` is of type `{type(d)}` but column `{table.columns[i].name}` only supports type `{table._columnTypeReference[i]}`")

        # Using our own method to fetch that juicy data
        tableData = self.fetchDataFromTable(table=table, guild=guild)
        tableData = tableData.All() # Switching over to raw data


        # Putting the data that is going to be inserted into its own dictionary
        new_data = {}
        for i, col in enumerate(table._columnStrList, start=0):
            new_data[col] = data[i]

        # Actually append the new data
        tableData.append(new_data)

        # Actually updating the file
        if table.per_guild: # Route for guild-specific data

            with open(table.fullFilePath % {"guildid" : guild.id}, "w") as j:
                json.dump(tableData, j, indent=4)

        else: # Route for global data

            with open(table.fullFilePath, mode="w") as j:
                json.dump(tableData, j, indent=4)

        return QueryHandler(table, tableData, guild=guild) # Returns a QueryHandler with new data because why not
