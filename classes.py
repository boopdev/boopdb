from os import mkdir
from os.path import exists, splitext
from typing import Any, Dict, Iterable, Optional, Union, List
import json
from discord import Guild
import copy
import glob

from .enums import DatabaseUpdateType, SortBy

import logging

class DatabaseTable(object):
    def __init__(self, name : str, *, perGuild : bool = False) -> None:
        """
            Our magic table stuff. This stuff will basically dictate the actual file, and not the data in it.
        """

        self.name = name.lower()
        self.per_guild = perGuild # Whether or not the database will be put it its own folder, and used per-guild.
        self.columns = []

        self._database = None

    @property
    def fileName(self) -> str:
        if not self.per_guild: # If it's not supposed to be a guild-specific filesystem
            return self.name.replace(' ', '_') + ".json"
        else: # If it is supposed to be guild-specific
            return self.name.replace(' ', '_')

    @property
    def fullFilePath(self) -> str:
        """
            Returns the entire path of the file
        """
        if self.per_guild:
            return f"./{self._database.root_name}/{self.fileName}/%(guildid)s.json"

        return f"./{self._database.root_name}/{self.fileName}"

    @property
    def _columnTypeReference(self) -> List[type]:
        """
            A list which has all of the column types in it.
        """
        return [col.type for col in self.columns]

    @property
    def _columnStrList(self) -> List[str]:
        """
            Just returns a list of all of the columns' names.
        """
        return [i.name for i in self.columns]

    def _fetchAllParticipatingGuilds(self) -> List[int]:
        """
            Returns a list of guild ids who are using this table if it is per-guild.
            If the table is not per-guild this will return an empty list.

            This is purely based on filenames.
        """
        if not self.per_guild:
            return []

        g = glob.glob(f"./{self._database.root_name}/{self.fileName}/*.json")
        return list(map(lambda i: int(splitext(i)[0][-18:]), g))

    def addColumn(self, name, type, *, default_value = None):
        """
            Creates a database column through a specific
        """

        # Create an actual column object
        col = DatabaseColumn(
            table=self,
            columnId=len(self.columns),
            name=name,
            type=type,
            default_value=default_value
        )

        col.columnId = len(self.columns)
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

    def _init_post_database_assign(self) -> None:
        """
            A function which is called post database assign.
            This is where the program ensures the folders and files exist.
        """
        self._database.logger.info("Ensuring directories exist for table `%s`" % self.name)

        if self.per_guild:

            if not exists(f"./{self._database.root_name}/{self.fileName}/"):
                mkdir(f"./{self._database.root_name}/{self.fileName}/")
                self._database.logger.info("Table `%s` is per-guild, created directory for per-guild data storage." % self.name)
        else:

            if not exists(self.fullFilePath):   

                with open(self.fullFilePath, 'w+') as j:
                    json.dump([], j)
                    j.close()

                self._database.logger.info("Table `%s` had no json file, created a new empty one for you <3" % self.name)

        self._database.logger.info("Assigned table `%s` with %s columns (per-guild: %s)" % (self.name, len(self.columns), self.per_guild))

    def fetchAllData(self, guild : Guild = None, *, RAW_ONLY : bool = False):
        if guild == None:

            with open(self.fullFilePath, 'r') as js:
                data = json.load(js)

        else:

            with open(self.fullFilePath % {"guildid" : guild.id}) as js:
                data = json.load(js)

        if RAW_ONLY:
            return data

        return QueryHandler(self, data, guild)

    def updateTableData(self, new_data : List[dict], guild : Guild = None) -> None:
        
        # Dump new data into file
        if guild is None:
            with open(self.fullFilePath, 'w+') as j:
                json.dump(
                    new_data,
                    j,
                    indent = 4
                )
            return

        with open(self.fullFilePath % {"guildid" : guild.id}, 'w+') as j:
            json.dump(
                new_data,
                j,
                indent=4
            )


class DatabaseColumn(object):
    def __init__(self, table : DatabaseTable, columnId : int, name : str, type : type, default_value : Any = None):
        self.name = name.lower() # We aint about that gay ass uppercase name shit
        self.type = type
        self.columnId = 0
        self.default_value = default_value


        self._database = None

    def _initialize_with_database(self, database) -> None:
        """
            This function is ran immediately after the database acknowledges that it has this column.
            This is basically reserved for any sorting issues I have to fix later on.
        """

        self._database = database
        return

    def _check_value(self, value) -> bool:
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

    def __init__(self, table : DatabaseTable, results, guild : Guild) -> None:
        
        self.table = table
        self.columns = self.table.columns

        self.guild = Guild

        self.results = results
        self.__index_all_results() # Basically allows us to tell apart all of the data
 
    def __index_all_results(self) -> None:
        if len(self.results) > 0:
            if "__META_DB_INDEX" in self.results[0].keys():
                print("already was indexed")
                return
        
        for index, item in enumerate(self.results):
            # print(item, type(item), sep="\t") # Debug purposes
            item["__META_DB_INDEX"] = index

    def __get_unindexed_all_results(self) -> List[dict]:
        x = copy.deepcopy(self.results)
        for item in x:
            del item['__META_DB_INDEX']
        return x

    def __unindex_this(self, d : List[Dict]) -> List[dict]:
        d = copy.deepcopy(d)
        for item in d:
            del item['__META_DB_INDEX']
        return d

    def __fetch_all_item_indexes(self) -> Iterable:
        return [k['__META_DB_INDEX'] for k in self.results]

    def All(self) -> List[dict]:
        """
            Just returns all of the results as a dict
        """
        # Default return if nothing matches the query
        if len(self.results) < 1:
            return []

        return self.__get_unindexed_all_results()

    def First(self, n : int = 1) -> Union[dict, list]:
        """
            Returns the first `n` items from the results.
            Default value is 1, and when `n` is 1 the function will
            return it as a dict, otherwise it will return it as a list.

            If the size of the result set is smaller than `n`, this function
            will return the equivalent of calling QueryHandler.All()
        """
        # Default return if nothing matches the query

        if len(self.results) < 1:
                return None

        if n > 1:  
            unind = self.__get_unindexed_all_results()[:n]

        else:
            unind = self.__get_unindexed_all_results()[0]

        return unind

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

        return QueryHandler(self.table, [res for res in self.results if res[specific_column.name.lower()] == value], guild=self.guild)

    def AdvancedFilter(self, column_name : str, func : callable):
        """
            This filter will basically return all the values that are true based on the `func` argument.

            Only one argument will be provided to the function. Which will be the column value for the row.
            This function WILL be ran for EVERY row you have in the database. So please dont make it too cpu-intensive.

            This function returns a new QueryHandler instance with all of the results that are true based on the function.
        """
        
        # This var will store the DatabaseColumn object
        specific_column = None

        # Searching for our column
        for c in self.columns:
            if c.name.lower() == column_name.lower():
                specific_column = c
                break

        # Return an error if the column doesnt exist
        if specific_column is None:
            raise ValueError("Column `%s` does not exist" % column_name.lower())

        # Return the query handler
        return QueryHandler(
            table = self.table,
            results = [i for i in self.results if func(i[specific_column.name])],
            guild=self.guild
        )

    def Update(self, column_name : str, value : Any, update_type : DatabaseUpdateType = DatabaseUpdateType.SET, overwrite_file : bool = False):
        """
            Sets the value for all data in a specific column which also resides in `self.results`.

            Meaning that anything that you have in this specific object, will have a column updated
            when this function is used.

            This function returns this same QueryHandler object, but the results are updated
            to fit the new updates made to them.
        """

        # This var will store the DatabaseColumn object
        specific_column = None

        # Searching for our column
        for c in self.columns:
            if c.name.lower() == column_name.lower():
                specific_column = c
                break

        # Return an error if the column doesnt exist
        if specific_column is None:
            raise ValueError("Column `%s` does not exist" % column_name.lower())

        # Then we check and see if the value that is to be updated is actually of the type that the column requests.
        if not specific_column._check_value(value):
            raise TypeError("Type provided for search query is not corresponding type to column. Column type is `%s`, you provided value with type: `%s`" % (specific_column.type, type(value)))

        new_data = self.results
        # Now we update each result
        for result in new_data:
            self.table._database.logger.info("Updating data in table %s. (Setting: %s) Value=%s" % (self.table.name, update_type.name, value))
            
            # Setting the value has no requirements
            if update_type == DatabaseUpdateType.SET:
                result[specific_column.name.lower()] = value


            # Incrementing the value of the column requires additional checks to ensure both the value provided, and the column's type, can be added to
            elif update_type == DatabaseUpdateType.INCREMENT:
                
                # Making sure you can actually add to the item
                if not hasattr(result[specific_column.name.lower()], "__add__"):
                    raise TypeError(f"Error when updating value `{specific_column.name.lower()}`. Type `{type(result[specific_column.name.lower()])}` has no built-in adding feature.")

                # Checking the value provided to make sure you can add with that too
                if not hasattr(value, "__add__"):
                    raise TypeError(f"Error while updating value `{specific_column.name.lower()}`. The value you provided of type `{type(value)}` is not one that can be added to.")

                # Actually adding to the value
                result[specific_column.name.lower()] += value

            # Decrementing the value of the column also requires additional checks.
            elif update_type == DatabaseUpdateType.DECREMENT:
                
                # Making sure you can actually subtract from the item
                if not hasattr(result[specific_column.name.lower()], "__sub__"):
                    raise TypeError(f"Error when updating value `{specific_column.name.lower()}`. Type `{type(result[specific_column.name.lower()])}` has no built-in subtraction feature.")

                # Checking the value provided to make sure you can actually subtract from it.
                if not hasattr(value, "__sub__"):
                    raise TypeError(f"Error while updating value `{specific_column.name.lower()}`. The value you provided of type `{type(value)}` is not one that can be subtracted from.")

                # Actually decrementing the value.
                result[specific_column.name.lower()] -= value


        # Now we fetch old data for comparison
        old_data = self.table.fetchAllData(self.guild)

        # Attempting to merge the data
        merged_data = []
        new_data_indexes = self.__fetch_all_item_indexes()

        # Allocating spots for new data in old data's spaces
        for row in old_data.results:
            # Keep the old data if no new data is created
            if row['__META_DB_INDEX'] not in new_data_indexes:
                merged_data.append(row)

        # Actually doing the merge
        merged_data.extend(new_data)

        # Sort the indexes so it stays in order
        merged_data.sort(
            key = lambda x: x['__META_DB_INDEX'],
            reverse=False
        )

        unindexed_merged = self.__unindex_this(merged_data)
        self.table.updateTableData(unindexed_merged)

        return QueryHandler(self.table, unindexed_merged, self.guild)

    def Sort(self, column_name : str, sort_type : SortBy = SortBy.ASCENDING):
        """
            Sorts the data by a column, By default it sorts by ascending.
            Setting `sort_type` to SortBy.DESCENDING will obviously set it to
            descending
        """

        return QueryHandler(self.table, sorted(self.results, reverse=bool(sort_type.value)), self.guild)

