'''
Entity class to be used within formulite
'''
import os

def ident(level):
    """Function to help with identation"""
    return "    " * level

def _init_header(filename, rewrite=False):
    """Function to write an auto-generated indicator in the file header"""
    if (not rewrite) and (not os.path.exists(filename)):
        with open(filename, "w+") as f:
            f.write("\'\'\' This file is automatically generated. \'\'\'\n\n")

class Entity:
    """This META-class represents the tables in the database"""
    def __init__(self, e_name, args_dict):
        self.e_name = e_name
        self.args_dict = args_dict
        self.primary_key = []
        self.foreign_key = {}
    
    # WRITE to file operation is split into several small methods (abstraction)
    # use the writedown() method in the end.

    @staticmethod
    def get_filename(classname):
        return f"{classname}.py"

    def auto_filename(self):
        return Entity.get_filename(self.e_name)

    def _write_dict(self, file_obj, dict_name, dictionary):
        leftbrack = '{'
        rightbrack = '}'
        file_obj.write(f"{ident(1)}{dict_name} = {leftbrack}\n")
        pairs = []
        for key, value in dictionary.items():
            pairs.append(f"\"{key}\":\"{value}\"")
        joined_pairs = ", ".join(pairs)
        file_obj.write(f"{ident(2)}{joined_pairs}\n{ident(1)}{rightbrack}\n\n")

    def _write_name(self, file_obj):
        file_obj.write(f"class {self.e_name}:\n\n")

    def _write_attrs(self, file_obj):
        self._write_dict(file_obj,"_attribute_types", self.args_dict)

    def _write_PK(self, file_obj):
        pk_list_str = "\", \"".join(self.primary_key)
        file_obj.write(f"{ident(1)}_primary_key = [\"{pk_list_str}\"]\n\n")

    def _write_FK(self, file_obj):
        self._write_dict(file_obj,"_foreign_key", self.foreign_key)

    def _write_constructor(self, file_obj):
        file_obj.write(ident(1) + "def __init__(self, **kargs):\n")
        for key, value in self.args_dict.items():
            file_obj.write(ident(2) + f"self.{key} = kargs[\"{key}\"]\n")
        file_obj.write("\n")

    def writedown(self, file_path="", filename=None, rewrite=False):
        """method to actually write the entity object model in the corresponding file"""
        if filename is None:
            filename = self.auto_filename()
        filename = file_path + filename
        if not self.primary_key:
            print(f"Primary key not informed, object {self.e_name} could not be written")
            return
        _init_header(filename, rewrite)
        with open(filename, "a") as obj_file:
            self._write_name(obj_file)
            self._write_attrs(obj_file)
            self._write_PK(obj_file)
            if self.foreign_key:
                self._write_FK(obj_file)
            self._write_constructor(obj_file)

    def joined_primary_key(self, pk=None):
        '''helper to join the primary key in case of composite key'''
        if pk is not None:
            return ", ".join(pk)
        return ", ".join(self.primary_key)

    def create_table_query(self, readable=False):
        '''returns the query used for table creation'''
        endl = " "
        if readable:
            endl = "\n"
        sql = f"CREATE TABLE IF NOT EXISTS {self.e_name}({endl}"
        for key, value in self.args_dict.items():
            sql += f"{key} {value},{endl}"
        #pkeys = ", ".join(self.primary_key)
        sql += f"PRIMARY KEY({self.joined_primary_key(self.primary_key)})"
        if self.foreign_key:
            for key, value in self.foreign_key.items():
                sql += F",{endl}FOREIGN KEY ({key}) REFERENCES {value} ({key})"
        return sql + f"{endl})"

    def add_attribute(self, col_name, col_type, file_path="", filename=None):
        if filename is None:
            filename = self.auto_filename()
        self.args_dict[col_name] = col_type
        self.writedown(file_path, filename, rewrite=True)
