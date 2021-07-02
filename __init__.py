import mainhandler
from classes import DatabaseColumn, DatabaseTable
from enums import *


if __name__ == "__main__":

    import discord

    MyTables = []

    t1 = DatabaseTable("money", perGuild=False)
    t1.addColumn("userid", int)
    t1.addColumn("money", int)

    MyTables.append(t1)

    db = mainhandler.SuckMyTinyPenis(
        discord.Client(),
        tables=MyTables
    )

    #db.insertDataIntoTable(
    #    "money",
    #    (1234567891011, 500)
    #)
    #db.insertDataIntoTable(
    #    "money",
    #    (9876543211011, 246)
    #)

    data = db.fetchDataFromTable("money")
    user_data = data.AdvancedFilter("money", lambda m: m > 400 or m < 300)
    user_data.Update("money", 10, update_type = DatabaseUpdateType.INCREMENT)

    print(user_data)
