import mainhandler
from classes import DatabaseColumn, DatabaseTable


if __name__ == "__main__":

    import discord

    MyTables = []

    t1 = DatabaseTable("money", perGuild=False)
    t1.addColumn("userid", int)
    t1.addColumn("money", int)

    MyTables.append(t1)

    mainhandler.SuckMyTinyPenis(
        discord.Client(),
        tables=MyTables
    )