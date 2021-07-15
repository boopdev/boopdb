import logging
from os.path import exists
from datetime import datetime
from typing import DefaultDict, List
import json
from pydoc import locate

from discord import Object

class defaultValueNotFound(Exception):
    pass

class SchemaHandler(object):

    def __init__(self, database):

        self.db = database
        self.logging = self.db.logger

    @property
    def schema_dir(self) -> str:
        """
            Returns the directory to the schema file.
        """

        return f"./{self.db.root_name}/schema.json"



    def check_for_existing_schema(self) -> bool:
        """
            Checks for an existing schema. If it doesn't exist then this creates one.
            This will return false if one didn't exist. Or true if one already existed.
        """
        
        self.logging.info("Checking for existing schema...")

        if not exists(self.schema_dir):

            self.logging.info("Schema file didn't exist! So we're creating one for you :)")
            with open(self.schema_dir, 'w+') as f:
                f.write("[]")

            self.logging.info("Schema exist, that means you're good to go!")
            return False
        
        return True



    def generate_schema_file(self) -> List[dict]:
        """
            Generates a schema file based on the tables which exist in the database.
        """
        self.check_for_existing_schema()
        
        self.logging.info("Generating schema file based on current database tables.")

        generatingSchema = {}
        for table in self.db.tableRef.values():
            currentSchema = {
                    "METADATA" : {
                        "SCHEMA_GENERATED" : datetime.now().timestamp(),
                        "PER_GUILD" : table.per_guild
                    },

                    "COLUMNS" : {
                        c.name : {
                            "TYPE" : str(c.type),
                            "COLUMN_ID" : c.columnId,
                            "DEFAULT_VALUE" : c.default_value
                        } for c in table.columns
                }
            }
            generatingSchema[table.name] = currentSchema

        return generatingSchema

    def read_previous_schema(self) -> List[dict]:
        """
            Opens and reads a previously existing schema if it exists.
            If it doesn't exist this function returns an empty list.
        """

        self.check_for_existing_schema()

        with open(self.schema_dir) as schema:
            data = json.load(schema)

        return data

    def compare_schema(self) -> None:
        """
            Opens the currently existing schema, and generates a new schema.
            If differences are found, this manager will create the data to fix
            the missing data.

            If no default value is provided to the columns, then this will throw an
            error.
        """

        self.check_for_existing_schema()

        old_data = self.read_previous_schema()
        fresh_data = self.generate_schema_file()

        comparedSchema = []

        if len(old_data) == 0 and old_data != fresh_data:
            self.logging.info("Found an empty schema file, since no differences are possible, writing fresh data")
            newSchema = self.generate_schema_file()

            with open(self.schema_dir, "w") as f:
                json.dump(newSchema, f, indent=4)

            return True

        for k in fresh_data.keys():

            if k not in old_data.keys():
                self.logging.info("Found new table `%s`... Added to schema" % k)
                comparedSchema.append(fresh_data[k])
                continue

            if fresh_data[k] != old_data[k]:
                comparedSchema.append(fresh_data[k])

                if len(fresh_data[k]['COLUMNS']) != len(old_data[k]['COLUMNS']):
                    self.logging.info("Found differences in table `%s`... Attempting to resolve." % k)

                    if len(fresh_data[k]['COLUMNS']) > len(old_data[k]['COLUMNS']):
                        missing_columns = [i for i in fresh_data[k]['COLUMNS'] if i not in old_data[k]['COLUMNS']]
                        self.logging.info("Found `%s` new columns for table `%s`... Attempting to add." % (len(missing_columns), k))

                        # This method only works for non-per-guild systems
                        if not self.db.tableRef[k].per_guild:
                            all_data = self.db.tableRef[k].fetchAllData(RAW_ONLY=True)

                            for row in all_data:
                                for c in missing_columns:

                                    if 'DEFAULT_VALUE' not in fresh_data[k]['COLUMNS'][c].keys():
                                            raise defaultValueNotFound("When adding new columns they need a default value so we can update the old data.")

                                    row["c"] = fresh_data[k]['COLUMNS'][c]['DEFAULT_VALUE']

                            self.db.tableRef[k].updateTableData(all_data)

                        else:
                            guildids = self.db.tableRef[k]._fetchAllParticipatingGuilds()
                            guilds = map(lambda d: Object(id=d), guildids)

                            for g in list(guilds):
                                all_data = self.db.tableRef[k].fetchAllData(guild=g, RAW_ONLY=True)

                                for row in all_data:
                                    for c in missing_columns:

                                        if 'DEFAULT_VALUE' not in fresh_data[k]['COLUMNS'][c].keys():
                                            raise defaultValueNotFound("When adding new columns they need a default value so we can update the old data.")

                                        row["c"] = fresh_data[k]['COLUMNS'][c]['DEFAULT_VALUE']

                                self.db.tableRef[k].updateTableData(all_data, guild=g)
                                self.logging.info("Set default values for `%s` rows in table `%s` for `%s` missing columns" % (len(all_data), k, len(missing_columns)))

                        continue
                
                    else:
                        removed_columns = [i for i in old_data[k]['COLUMNS'] if i not in fresh_data[k]['COLUMNS']]

                        all_data = self.db.tableRef[k].fetchAllData(RAW_ONLY=True)
                        all_data = {k : v for k, v in all_data.items if k not in removed_columns} # what the fuck

                        self.db.tableRef[k].updateTableData(all_data)

                        continue

            else:
                comparedSchema.append(old_data[k])

        if comparedSchema == old_data:
            return False

        else:

            with open(self.schema_dir, "w") as f:
                json.dump(comparedSchema, f, indent=4)

            return True
            

            
