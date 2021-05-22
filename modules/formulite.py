'''
FORMUlite provides a "simple" ORM to perform asynchronous connection within an sqlite database.
currently under development.

Current Version 0.5
'''
import aiosqlite
import os
from .databasemanager import DatabaseManager

class formulite:
    '''
    A wrapper with the same name as the module, to make things easier.
    Should not be initialized, all methods are static
    '''
    _manager_instance = None # Singleton

    @staticmethod
    def _database_location(dbname='database.db', dbpath="resources/"):
        return f"{dbpath}{dbname}"

    @staticmethod
    def clear_database(dbname='database.db', dbpath="resources/"):
        '''Destroy the database file, if it exists.'''
        db = formulite._database_location(dbname, dbpath)
        if os.path.exists(db):
            os.remove(db)

    @staticmethod
    async def _getInstance(dbname, dbpath):
        '''internal method to get the single database manager instance'''
        if not os.path.exists(dbpath):
            os.mkdir(dbpath)
        if formulite._manager_instance is None:
            connection = await aiosqlite.connect(formulite._database_location(dbname, dbpath))
            formulite._manager_instance = DatabaseManager(connection, dbpath)
            await formulite._manager_instance._load_entities()
        return formulite._manager_instance

    @classmethod
    async def manager(cls, dbname='database.db', dbpath="resources/"):
        '''Get the single database manager instance (use this)'''
        return await cls._getInstance(dbname, dbpath)
