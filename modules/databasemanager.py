'''
Main formulite class, used to interact with the database
'''
from .entity import Entity
import os
from .formuliteutils import utils as f_utils

class DatabaseManager:
    '''
    The main goal of this module.
    It is responsible for communicating with the SQLite database.
    '''
    def __init__(self, connection, filepath="resources/"):
        self.conn = connection
        self._typeflag = None
        self.file_path = filepath
        self.entities = {}

    async def close(self):
        '''should be called at the end of execution'''
        await self.conn.close()

    ### SETUP OPERATIONS ###
    # these operations should initialize the ORM
    # They must be performed BEFORE any tables are created

    def set_entity(self, name, **kargs):
        '''
        The user must define the database entities
        Internally, a class for access to the table will be stored in the file dbobjects.py
        attribute names and their respective types should be provided,
        as there is no mapping between python duck typing and SQL types
        '''
        self.entities[name] = Entity(name, kargs)

    def append_primary_key(self, entity, *attrs):
        '''appends the given attributes to the given entity primary key list'''
        for att in attrs:
            self.entities[entity].primary_key.append(att)

    def set_primary_key(self, entity, *attrs):
        ''' makes the given attributes primary key for the given entity'''
        self.entities[entity].primary_key = attrs

    def set_foreign_key(self, entity, attrib, ref_entity):
        '''
        makes the given attribute a foreign key for the given entity
        If append is set to true, the attribute will be appended to the primary key
        foreign keys should only be set after primary keys are set
        '''
        self.entities[entity].foreign_key[attrib] = ref_entity
        
    def set_clear(self, entity_obj):
        '''Destroy the database objects file, if it exists.'''
        if os.path.exists(entity_obj.auto_filename()):
            os.remove(entity_obj.auto_filename())

    def set_clear_all(self):
        '''Destroy all database objects files'''
        for entity in self.entities.values():
            self.set_clear(entity)

    ### CREATE ###

    async def create_tables(self):
        '''
        sends in the queries for creating all the tables predicted in the setup operations
        this operation should only be called once. To add new tables after the database is created, see add_table()
        '''
        for entity in self.entities.values():
            entity.writedown(self.file_path)
            #print(entity.create_table_query(None, True))
            await self.conn.execute(entity.create_table_query())
        await self.conn.commit()

    async def add_table(self, entity):
        '''Adds a single table to the database, entity must be generated / set separately'''
        entity.writedown(self.file_path)
        await self.conn.execute(entity.create_table_query())
        await self.conn.commit()

    ### LOAD ###
    # The manager should be able to work with existing databases, after their creation.
    # Entities should therefore be loaded in when the manager is initialized, if they exist

    def loaded(self):
        return not not self.entities

    async def _load_entities(self):
        tablenames = await self.conn.execute_fetchall(f"SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
        if tablenames:
            #print(tablenames)
            for table, in tablenames:
                e_class = f_utils.load_module(table.lower(), self.file_path + Entity.get_filename(table), table)
                self.entities[table] = Entity(table, e_class._attribute_types)

    ### BUILD ###
    # The object constructor will not be available by default, this would require a complex dynamic import
    # so build functions are provided to work as a factory for instances / table rows, which can later be included to the DB

    def build(self, tablename, **kargs):
        '''Calls the appropriate constructor for the corresponding table.'''
        e_class = f_utils.load_module(tablename.lower(), self.file_path + Entity.get_filename(tablename), tablename)
        return e_class(**kargs)

    ### INSERT ###

    async def insert(self, Obj):
        '''Insert instance into database.'''
        c_name = Obj.__class__.__name__

        # make sure the object has not been previously inserted
        pk_dict = {}
        for pk in Obj.__class__._primary_key:
            pk_dict[pk] = getattr(Obj, pk)
        if await self.exists(c_name, **pk_dict):
            return

        vals = []
        for key in self.entities[c_name].args_dict.keys():
            vals.append( (getattr(Obj, key)) )
        vals_concat = ""
        for value in vals:
            if isinstance(value, str):
                vals_concat += f"\"{value}\""
            else:
                vals_concat += str(value)
            if value != vals[-1]:
                vals_concat += ", "

        keyjoin = ", ".join( self.entities[c_name].args_dict.keys() )
        sql = f"INSERT INTO {c_name} ({keyjoin}) VALUES ({vals_concat})"
        await self.conn.execute(sql)
        await self.conn.commit()

    async def build_and_insert(self, tablename, **kargs):
        '''Insert instance into database right after instantiation, then returns it'''
        obj = self.build(tablename, **kargs)
        await self.insert(obj)
        return obj

    ### UPDATE ###

    async def update(self, Obj):
        '''Update a database instance (single row)'''
        c_name = Obj.__class__.__name__

        pk_list = []
        for pk in Obj.__class__._primary_key:
            pk_list.append(f"{pk}={getattr(Obj, pk)}")
        cond_string = " AND ".join(pk_list)        

        up_list = []
        for attribute in Obj.__class__._attribute_types.keys():
            if attribute not in Obj.__class__._primary_key:
                up_list.append(f"{attribute}={getattr(Obj, attribute)}")
        set_string = ", ".join(up_list)

        sql = f"UPDATE {c_name} SET {set_string} WHERE {cond_string}"
        await self.conn.execute(sql)
        await self.conn.commit()

    ### SELECT ###

    async def exists(self, tablename, **kargs):
        '''
        check if an object already exists in the database
        only use this if you dont need the returned object further in your application
        '''
        obj_count = await self.count(tablename, f_utils.where(**kargs))
        return obj_count > 0

    async def select(self, tables_obj, cols_obj="*", *args):
        '''Use select_from()'''
        # I should perform some kind of type checking here, and throw an error if needed
        sql = f"SELECT {str(cols_obj)} FROM {str(tables_obj)}"
        for arg in args:
            sql += f" {str(arg)}" # whitespace is relevant here
        return await self.conn.execute_fetchall(sql)

    async def select_from(self, tables_obj, cols_obj="*", *args):
        '''
        Returns a list of objects from the database that match the passed in conditions, if any
        User must know some prior SQL to write the proper query (args should be in correct order)
        '''
        tablename = str(tables_obj)
        rows = await self.select(tables_obj, cols_obj, *args)

        result = []
        for row in rows:
            arg_dict = dict( zip(self.entities[tablename].args_dict.keys(), row) )
            result.append(self.build(tablename, **arg_dict))

        return result

    async def select_all_from(self, tables_obj, *args):
        '''Helper'''
        return await self.select_from(tables_obj, "*", *args)

    async def count(self, tables_obj, *args):
        '''Helper for selecting the count of rows from a given table'''
        count_tuple = await self.select(tables_obj, "count(*)", *args)
        return count_tuple[0][0]

    ### DROP / DELETE ###

    async def drop_table(self, tablename):
        '''Delete a table from the database'''
        sql = f"DROP TABLE {tablename}"
        await self.conn.execute(sql)
        await self.conn.commit()

    async def drop_tables(self, *tables):
        '''Helper to delete multiple tables'''
        for t in tables:
            await self.drop_table(t)

    async def reset(self):
        '''Erases all tables, but keeps the file. See formulite.clear_database()'''
        await self.drop_tables(*list(self.entities.keys()))

    ### ALTER ###
    # These should be used with caution
    # Upon altering a table structure, the object structure should be manually altered to match it
    # Remember to update rows / reload objects after using these methods

    async def add_column(self, tablename, col_name, col_type):
        '''Adds a new column to a table.'''
        self.entities[tablename].add_attribute(col_name, col_type, self.file_path)
        sql = f"ALTER TABLE {tablename} ADD {col_name} {col_type}"
        await self.conn.execute(sql)
        await self.conn.commit()

    async def add_columns(self, tablename, **columns):
        '''Adds multiple columns to a table, in a more pythonic syntax'''
        for key, value in columns.items():
            await self.add_column(tablename, key, value)

    async def drop_column(self, tablename, column): # not yet supported
        # verify if column is part of primary_key before removal

        # adjust the constructor in the corresponding object file

        # only now drop the column in the database
        sql = f"ALTER TABLE {tablename} DROP COLUMN {column}"
        await self.conn.execute(sql)
        await self.conn.commit()
        