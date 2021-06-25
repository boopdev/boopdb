import json
import asyncio
from typing import Optional, Any, List
from discord import Client, Guild

from os.path import exists
from os import mkdir

from classes import DatabaseColumn, DatabaseTable

class SuckMyTinyPenis(object):
    def __init__(
            self, 
            client : Client,
            *,
            loop : Optional[asyncio.BaseEventLoop] = None,
            tables : Optional[List[DatabaseTable]] = None,
            alternative_root_name : Optional[str] = None

        ):
        """
        Creates a wacky dandy thing that has functions to help you do filesystem
        stuff, without any knowledge of knowing how to do file system stuff
        """

        # First we assign basic data to this object
        self.client = client




        # Ensure that this thing has its loop properly set up, in case for whatever reason the user
        # would ever want to provide an alternative event loop.
        if loop is None:
            self.loop = self.client.loop # Default to client loop if no loop is provided
        else:
            self.loop = loop




        # Allowing a user to set a custom root name
        self.root_name = alternative_root_name

        if self.root_name is None: # Defaulting if one doesn't exist.
            self.root_name = "boopDatabase"

        # After we set the root name, we ensure that that file directory exists.
        self.ensureRootFilesystem()



        # Oh boy, here we go.
        self._raw_table_data = tables

        # Ensure that all tables know that they're in the system. References for good measure never hurts
        for table in self._raw_table_data:
            table._database = self
            table._init_post_database_assign()

        # Setting up a simple reference for when we actually fetch stuff from this database
        self.tableRef = {table.name : table for table in self._raw_table_data}


    def ensureRootFilesystem(self):
        """
            Ensures that the root directory actually does exists.
            If it doesn't, it creates the new folder.
            If it does exist, this function does an epic backflip.
        """

        if exists(f'./{self.root_name}/'):
            # The sickest backflip you've ever seen.
            print("Doing an epic backflip.")

        else:
            # Creating the directory if it doesnt exist.
            mkdir(f'./{self.root_name}')


    def fetchDataFromTable(self, table : str, *, guild : Optional[Guild] = None):
        """
            Fetches data from a table, returns a QueryHandler.
        """

        # Return an error if the table doesn't even exist.
        if table.lower() not in self.tableRef.keys():
            raise ValueError("The table name provided is not in the reference, maybe you spelt it wrong!")
            return

        table = self.tableRef[table.lower()]

        if table.per_guild:
            with open()

    def insertDataIntoTable(self, table : str, data : list):
        """
            Inserts data into a table
        """
        
        # Ensure that the table actually exists
        if table.lower() not in self.tableRef.keys():
            raise ValueError("The table name provided is not in the reference, maybe you spelt it wrong!")
            return

        

        
        

        




