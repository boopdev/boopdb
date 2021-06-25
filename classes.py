'''

    You shouldn't need to touch any function in this file that starts with an underscore ( _ ).
    these functions are meant for backend, if you alter them, the entire thing will be altered.

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    This message is solely left for Hayla. I know your trying to understand this code right?
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

'''

from os import mkdir
from os.path import exists
from funcs import createEmptyJsonFile
from typing import Any, Type
import json


class DatabaseTable(object):
    def __init__(self, name : str, *, perGuild : bool = False):
        """
            Our magic table stuff. This stuff will basically dictate the actual file, and not the data in it.
        """

        self.name = name.lower()
        self.per_guild = perGuild # Whether or not the database will be put it its own folder, and used per-guild.
        self.columns = []

        self._database = None

    @property
    def fileName(self):
        if not self.per_guild: # If it's not supposed to be a guild-specific filesystem
            return self.name.replace(' ', '_') + ".json"
        else: # If it is supposed to be guild-specific
            return self.name.replace(' ', '_')

    @property
    def fullFilePath(self):
        """
            Returns the entire path of the file
        """
        return f"./{self._database.root_name}/{self.fileName}"

    @property
    def _columnTypeReference(self):
        """
            A list which has all of the column types in it.
        """
        return [col.type for col in self.columns]

    def addColumn(self, name, type):
        """
            Creates a database column through a specific
        """

        # Create an actual column object
        col = DatabaseColumn(
            table=self,
            columnId=len(self.columns),
            name=name,
            type=type
        )

        # Then append that to the list.
        self.columns.append(col)

        # idk then return the column or something?
        return col

    def getColumnByID(self, id : int):
        """
            Returns the index of a specific column
        """
        for column in self.columns:
            if column.columnId == id:
                return column
        return None

    def _init_post_database_assign(self):
        """
            A function which is called post database assign.
            This is where the program ensures the folders and files exist.
        """

        if self.per_guild:
            mkdir(self.fullFilePath)
        else:
            createEmptyJsonFile(self.fullFilePath)

    def fetchAllData(self):
        with open(self.fullFilePath, 'r') as js:
            data = json.load(js)

        return QueryHandler(self, data)

class DatabaseColumn(object):
    def __init__(self, table : DatabaseTable, columnId : int, name : str, type : type):
        self.name = name.lower() # We aint about that gay ass uppercase name shit
        self.type = type
        self.columnId = 0


        self._database = None

    def _initialize_with_database(self, database):
        """
            This function is ran immediately after the database acknowledges that it has this column.
            This is basically reserved for any sorting issues I have to fix later on.
        """

        self._database = database
        return

    def _check_value(self, value):
        """
            Checks to see if the value provided is one that abides by the type of this column.
        """
        return isinstance(value, self.type) # Just a simple check

class QueryHandler(object):
    """
        The class which allows users to handle data recieved from the handler.
        This makes it easier to find certain things rather than having to sift through tons
        and tons of queries.
    """

    def __init__(self, table : DatabaseTable, results):
        
        self.table = table
        self.columns = self.table.columns

        self.results = results

    def All(self):
        """
            Just returns all of the results as a dict
        """
        return self.results

    def First(self):
        """
            Returns the first value in the database, this would also be the oldest value.
        """
        return self.results[0]

    def Seek(self, column_name : str, value : Any):
        """
            Searches for a specific value in all rows based on a singular column.
            This returns another QueryHandler. So you'll be able to search again,
            and use any of the other functionalities that come with this class.
        """

        # Fetching the actual column object so we can do some wacky stuff with it
        specific_column = None
        for c in self.columns:
            if c.name.lower() == column_name.lower():
                specific_column = c
                break

        if specific_column is None:
            raise ValueError("Column `%s` does not exist" % column_name.lower())


        if not specific_column._check_value(value):
            raise TypeError("Type provided for search query is not corresponding type to column. Column type is `%s`, you provided value with type: `%s`" % (specific_column.type, type(value)))

        return QueryHandler(self.table, [res for res in self.results if res[specific_column.name.lower()] == value])