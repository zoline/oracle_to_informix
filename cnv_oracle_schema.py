# pip install oracledb 
# HR/1dlsvmfk

import os, sys
import argparse,configparser 
import csv,re

import getpass
import oracledb

class Oracle_Source:
    CFG_FILE="oracle.cfg"
    TYPE_CONV_DICT = {}
    DEVICE_PARTITIONS = ['p_device_part_1','p_device_part_2','p_device_part_3','p_device_part_4','p_device_part_5','p_device_part_n']
    DEVICE_TBLSPACES  = ['statdbs1','statdbs2','statdbs3','statdbs4','statdbs5','statdbs6']
    DEVICE_HVALUES    = ["devicenum < 'N'","devicenum >= 'N' and devicenum < 'NR03'","devicenum >= 'NR03' and devicenum < 'P'",
                         "devicenum >= 'P' and devicenum < 'PL03'","devicenum >= 'PL03'","devicenum is  NULL"]
    GENDATE_PARTITIONS = ['p_gendate_part_1','p_gendate_part_2','p_gendate_part_3','p_gendate_part_4','p_gendate_part_5',
                              'p_gendate_part_n','p_gendate_part_r']
    GENDATE_TBLSPACES  = ['statdbs6','statdbs5','statdbs4','statdbs3','statdbs2','statdbs1','statdbs1']
    GENDATE_HVALUES    = ["'2029','2024','2019'","'2028','2023','2018'","'2027','2022','2017'","'2026','2021','2016'",
                          "'2025','2020','2015'",'NULL','REMAINDER']
    

    @classmethod
    def make_cnv_dict(cls,conv_file):
        #print(conv_file)
        with open(conv_file) as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            headers = next(reader,None)
            #print(headers)
            for owner,table,colname,newtype in reader:
                cols=(owner,table,colname)
                tab_col_name =':'.join(cols).upper()
                cls.TYPE_CONV_DICT[tab_col_name] = newtype
                
    @classmethod
    def get_cnv_dict_type(cls, owner,table,column):
        cols=(owner,table,column)
        tab_col_name =':'.join(cols).upper()
        if tab_col_name in cls.TYPE_CONV_DICT:
            return cls.TYPE_CONV_DICT[tab_col_name]
        else:
            return None
                       
    @classmethod
    def get_cnv_rule_type(cls,coltype,length,precision,scale,avg_col_length,char_length):
        if coltype.startswith('TIMESTAMP'): 
                return cls.cnv_timestamp_type(coltype,length,precision,scale,avg_col_length,char_length)
        elif coltype.startswith('INTERVAL'): 
                return cls.cnv_interval_type(coltype,length,precision,scale,avg_col_length,char_length)
        else:
            match coltype:
                case 'NUMBER':
                    return cls.cnv_number_type(length, precision, scale)
                case 'CHAR'|'NCHAR'|'VARCHAR'|'VARCHAR2'|'NVARCHAR2':
                    return cls.cnv_char_type(coltype,length,precision,scale,avg_col_length,char_length)
                case 'CLOB'|'LONG'|'BLOB'|'RAW'|'BFILE'|'LONG RAW': 
                    return cls.cnv_blob_type(coltype,length,precision,scale,avg_col_length,char_length)
                case 'FLOAT'|'BINARY_FLOAT'|'BINARY_DOUBLE':
                    return cls.cnv_float_type(coltype,length,precision,scale,avg_col_length,char_length)
                case 'DATE':
                    return "datetime year to second"
                case 'XMLTYPE':
                    return 'lvarchar(8192)'
                case 'MDSYS.SDO_GEOMETRY':
                    return 'geometry'
                case _:
                    return "%s" % coltype

    @classmethod
    def cnv_interval_type(cls,coltype,length,precision,scale,avg_col_length,char_length):      
        if coltype.startswith('INTERVAL YEAR'):
            return coltype.lower()
        elif coltype.startswith('INTERVAL DAY'):
                match scale:
                    case 0:
                        return coltype
                    case 1 | 2 | 3 | 4 | 5:
                        oldval = "SECOND(%s)" % scale
                        newval = "fraction(%s)" % scale
                        return coltype.replace(oldval,newval,1).lower()
                    case 6 | 7 | 8 | 9:
                        oldval = "SECOND(%s)" % scale
                        newval = "fraction(5)" 
                        return coltype.replace(oldval,newval,1).lower()
                    case _:
                        return "INTERVAL(?)"
        else:
            return "INTERVAL(?)"
        

    @classmethod
    def cnv_timestamp_type(cls,coltype,length,precision,scale,avg_col_length,char_length):      
        match scale:
            case 0:
                return "datetime year to second"
            case 1 | 2 | 3 | 4 | 5:
                return "datetime year to fraction(%s)" % (scale)
            case 6 | 7 | 8 | 9:
                return "datetime year to fraction(5)"
            case _:
                return "DATETIME(?)"
                

    @classmethod
    def cnv_float_type(cls,coltype,length,precision,scale,avg_col_length,char_length):      
        match coltype:
            case  'FLOAT'|'BINARY_FLOAT':
                return "float"
            case  'BINARY_DOUBLE':
                return "double"
            case _:
                return "FLOAT(?)"
                
                
    @classmethod
    def cnv_blob_type(cls,coltype,length,precision,scale,avg_col_length,char_length):      
        match coltype:
            case  'CLOB':
                return "clob"
            case  'BLOB' | 'RAW' | 'LONG RAW':
                return "blob"
            case  'LONG':
                return "text"
            case 'BFILE':
                return "blob"
            case _:
                return "BLOB(?)"
                
    @classmethod
    def cnv_char_type(cls,coltype,length,precision,scale,avg_col_length,char_length):      
        #print(coltype,length,precision,scale,avg_col_length,char_length)
        match coltype:
            case  'CHAR' | 'NCHAR':
                return "%s(%s)" % (coltype.lower(),length)
            case  'VARCHAR'| 'VARCHAR2':
                if length > 255:
                    return "lvarchar(%s)" % length
                elif length == 1:
                    return "char(1)"
                else:
                    return "varchar(%s)" % ("%s,%s" % (length, avg_col_length) if avg_col_length is not None and avg_col_length > 1 else length) 
            case  'NVARCHAR2':
                if length > 122:
                    return "lvarchar(%s)" % length
                else:
                    return "nvarchar(%s,%s)" % ("%s,%s" % (length, avg_col_length) if avg_col_length is not None and avg_col_length > 1 else length) 
            case _:
                return "CHARACTER(?)"
                
        
    @classmethod
    def cnv_number_type(cls,length,precision,scale):    
        #print(length, precision, scale)  
        if precision is None and scale == 0:
            return "integer"
        elif precision is None and scale is None:
            return "decimal(32)"
        elif scale > 0:
            return "decimal(%s,%s)" % (precision, scale)
        else:
            if precision < 5:
                return "smallint"
            elif precision >= 5 and precision <= 9:
                return "integer"
            elif precision >= 10 and precision <= 18:
                return "bigint"
            elif precision == 38 and scale == 0:
                return "integer"
            else:
                return "NUMBER(?)"

            
        
    @classmethod
    def get_null_string(cls, nullable):
        pass
    
    @classmethod
    def get_default_string(cls,data_default):
        pass
    
    
        
    def __init__(self, ):
        self.conn = None

        properties = configparser.ConfigParser()
        properties.read(Oracle_Source.CFG_FILE)
        self.conf = properties['CONFIG']

        self.username = self.conf['ORACLE_USER']
        self.password=self.conf['ORACLE_PASSWORD']

        Oracle_Source.make_cnv_dict(self.conf['TYPE_CONV_FORCE_TABLE'])
        ##print(conf['CONNECT_STRING'])

    def connect(self):
        """ Connect to oracle database swith username and password
            Oracle Connection info was saved in oracle.cfg """

        self.conn= oracledb.connect(user=self.username, password=self.password, dsn=self.conf['CONNECT_STRING'])
        if self.conn is None:
            print("Cananot connect to Oracle") 
            sys.exit(-1)

    def get_cursor():
        return self.conn.cursor()


    def get_tables(self, filter=None):
       table_query="""
        SELECT OWNER, TABLE_NAME,TABLESPACE_NAME,STATUS, NVL(INITIAL_EXTENT,0)/1024 AS INITIAL_EXTENT, NVL(NEXT_EXTENT,0)/1024 AS NEXT_EXTENT, PARTITIONED, READ_ONLY,AVG_ROW_LEN
        FROM all_tables 
        WHERE %s
          -- AND tablespace_name is not NULL AND NUM_ROWS IS NOT NULL 
        ORDER BY  OWNER, TABLE_NAME
       """
       if filter is None:
           query = table_query % ( "1=1" )
       else:
           query = table_query % filter
       with self.conn.cursor() as cur:
           cur.execute(query)
           res = cur.fetchall()
           return res

    def get_table_info(self, owner, table):
        table_query = """
         SELECT table_name, owner, Tablespace_name, Num_Rows 
         FROM all_tables 
         WHERE tablespace_name is not NULL 
           AND NUM_ROWS IS NOT NULL 
           AND OWNER = '%s' AND TABLE_NAME = '%s'
        """
        query = table_query % (owner.upper(), table.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchone()
            return res

    def get_notnull_constraint(self, owner, table, column):
        constraint_query = """
          SELECT A.CONSTRAINT_NAME 
            FROM ALL_CONSTRAINTS A, ALL_CONS_COLUMNS B 
           WHERE A.OWNER  = '%s'
             AND A.TABLE_NAME = '%s'
             AND A.CONSTRAINT_TYPE='C'
             AND A.OWNER = B.OWNER
             AND B.COLUMN_NAME ='%s'
             AND A.CONSTRAINT_NAME  = B.CONSTRAINT_NAME               
        """
        query = constraint_query % (owner.upper(), table.upper(), column.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchone()
        if res is None:
            return None
        else:
            (constraint_name,) = res 
            return " CONSTRAINT %s NOT NULL " % constraint_name.lower()

            
    def get_check_constraints(self, owner, table):
        check_query = """
          SELECT A.CONSTRAINT_NAME, A.SEARCH_CONDITION_VC
            FROM ALL_CONSTRAINTS A, ALL_CONS_COLUMNS B 
           WHERE A.OWNER  = '%s'
             AND A.TABLE_NAME = '%s'
             AND A.CONSTRAINT_TYPE='C'
             AND A.OWNER = B.OWNER
             AND A.SEARCH_CONDITION_VC  NOT LIKE '%% IS NOT NULL'   
             AND A.CONSTRAINT_NAME  = B.CONSTRAINT_NAME       
             """        
        check_string = ""
        query = check_query % (owner.upper(), table.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for CONSTRAINT_NAME, SEARCH_CONDITION in res:
                check_string += "alter table %s.%s add constraint %s  CHECK ( %s ) ;\n" % (owner.lower(),table.lower(),CONSTRAINT_NAME.lower(), SEARCH_CONDITION)
            return check_string
        
    def get_unique_constraints(self, owner, table):
        unqiue_query = """
              SELECT A.CONSTRAINT_NAME, LISTAGG(B.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY B.POSITION) AS COLUMNS
                FROM ALL_CONSTRAINTS A, ALL_CONS_COLUMNS B 
               WHERE A.OWNER  = '%s'
                 AND A.TABLE_NAME = '%s'
                 AND A.CONSTRAINT_TYPE='U'
                 AND A.OWNER = B.OWNER  
                 AND A.CONSTRAINT_NAME  = B.CONSTRAINT_NAME   
            GROUP BY A.CONSTRAINT_NAME     
             """        
        unique_string = ""
        query = unqiue_query % (owner.upper(), table.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for CONSTRAINT_NAME, COLUMNS in res:
                unique_string += "alter table %s.%s add constraint %s  unique ( %s ) ;\n" % (owner.lower(),table.lower(), CONSTRAINT_NAME.lower(), COLUMNS.lower())
            return unique_string

    def get_primary_constraints(self, owner, table):
        primary_query = """
              SELECT A.CONSTRAINT_NAME, LISTAGG(B.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY B.POSITION) AS COLUMNS
                FROM ALL_CONSTRAINTS A, ALL_CONS_COLUMNS B 
               WHERE A.OWNER  = '%s'
                 AND A.TABLE_NAME = '%s'
                 AND A.CONSTRAINT_TYPE='P'
                 AND A.OWNER = B.OWNER  
                 AND A.CONSTRAINT_NAME  = B.CONSTRAINT_NAME   
            GROUP BY A.CONSTRAINT_NAME     
             """        
        primary_string = ""
        query = primary_query % (owner.upper(), table.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for CONSTRAINT_NAME, COLUMNS in res:
                primary_string += "ALTER TABLE  %s.%s ADD  CONSTRAINT %s  PRIMARY KEY  ( %s ) ;\n" % (owner,table, CONSTRAINT_NAME.lower(), COLUMNS)
            return primary_string   
        
   


  
    
    def get_foreignkey_constraints(self, owner, table):
        fk_query = """
                SELECT FK.CONSTRAINT_NAME, FK.REF_NAME, FK.FK_COLUMNS,  R.TABLE_NAME, LISTAGG(R.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY R.POSITION) AS REF_COLUMNS
                FROM (
                            SELECT A.OWNER AS OWNER,
                                    A.CONSTRAINT_NAME AS CONSTRAINT_NAME,
                                    A.R_CONSTRAINT_NAME AS REF_NAME,
                                    LISTAGG(B.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY B.POSITION) AS FK_COLUMNS
                                FROM ALL_CONSTRAINTS A, ALL_CONS_COLUMNS B 
                            WHERE A.OWNER  = '%s'
                                AND A.TABLE_NAME = '%s'
                                AND A.CONSTRAINT_TYPE='R'
                                AND A.OWNER = B.OWNER  
                                AND A.CONSTRAINT_NAME  = B.CONSTRAINT_NAME   
                            GROUP BY A.OWNER,A.CONSTRAINT_NAME,A.R_CONSTRAINT_NAME
                    ) FK,  ALL_CONS_COLUMNS R
                WHERE FK.OWNER = R.OWNER
                    AND FK.REF_NAME = R.CONSTRAINT_NAME
                GROUP BY FK.CONSTRAINT_NAME, FK.REF_NAME,  R.TABLE_NAME, FK.FK_COLUMNS  
             """        
        fk_string = ""
        query = fk_query % (owner.upper(), table.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for CONSTRAINT_NAME, REF_NAME, FK_COLUMNS, REF_TABLE,REF_COLUMNS in res:
                fk_string += "ALTER TABLE %s.%s ADD  CONSTRAINT %s  FOREIGN KEY ( %s ) REFERENCES %s " % (owner,table, CONSTRAINT_NAME.lower(), FK_COLUMNS, REF_TABLE)
                if FK_COLUMNS != REF_COLUMNS:
                    fk_string += " (%s) ;\n" % REF_COLUMNS;
                else:
                    fk_string += ";\n";
            return fk_string   
         
                
    def get_columns(self,owner,table):
        column_query = """
         SELECT COLUMN_ID,	COLUMN_NAME,	DATA_TYPE,  	DATA_LENGTH,	DATA_PRECISION,	DATA_SCALE,	
                NULLABLE,	DEFAULT_ON_NULL	,  DEFAULT_LENGTH,	DATA_DEFAULT ,AVG_COL_LEN,CHAR_LENGTH,IDENTITY_COLUMN 
                --,	NUM_DISTINCT,	
                -- LOW_VALUE,	HIGH_VALUE	,DENSITY, NUM_NULLS	,
                -- CHARACTER_SET_NAME	,CHAR_LENGTH, CHAR_USED	,IDENTITY_COLUMN,
                -- EVALUATION_EDITION
          FROM ALL_TAB_COLS	 
         WHERE OWNER = '%s' AND TABLE_NAME = '%s'
           AND USER_GENERATED = 'YES' 
         ORDER BY COLUMN_ID
        """
        column_string = ""
        query = column_query % (owner.upper(), table.upper())
        #print (query)
    
    
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
            for COLUMN_ID, COLUMN_NAME,	DATA_TYPE,  DATA_LENGTH,	DATA_PRECISION,	DATA_SCALE,	NULLABLE,  DEFAULT_ON_NULL,  DEFAULT_LENGTH,	DATA_DEFAULT, AVG_COL_LEN, CHAR_LENGTH, IDENTITY_COLUMN in res:  
                column_string += '     ' if column_string == '' else '    ,'
                column_string += " %s " % COLUMN_NAME.lower()
                
                col_type = self.get_cnv_dict_type(owner,table,COLUMN_NAME)
                if col_type is None:
                   col_type = self.get_cnv_rule_type(DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,AVG_COL_LEN,CHAR_LENGTH)
                
                column_string += "\t\t" if len(COLUMN_NAME) < 9 else "\t"
                column_string += col_type
                
                if DATA_DEFAULT is not None:
                    column_string += " default %s " % DATA_DEFAULT   
                
                if NULLABLE == 'N':
                    #constr_string = self.get_notnull_constraint(owner,table,COLUMN_NAME)
                    #if constr_string is None:
                    #    column_string += " NOT NULL "
                    #else:
                    #    column_string += constr_string  
                    column_string += " NOT NULL "
                column_string += "\n"
            return column_string                

            #print(column_string)
    
    
    def get_sequences(self, owner):
        sequence_query = """
                SELECT SEQUENCE_NAME, INCREMENT_BY, MIN_VALUE, MAX_VALUE, CYCLE_FLAG, ORDER_FLAG, CACHE_SIZE, LAST_NUMBER, SCALE_FLAG FROM ALL_SEQUENCES
                WHERE SEQUENCE_OWNER = '%s'  
             """        
        sequence_string = ""
        query = sequence_query % (owner.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for SEQUENCE_NAME, INCREMENT_BY, MIN_VALUE, MAX_VALUE, CYCLE_FLAG, ORDER_FLAG, CACHE_SIZE, LAST_NUMBER, SCALE_FLAG in res:
                sequence_string += "CREATE SEQUENCE  %s.%s INCREMENT BY  %s  MAXVALUE %s MINVALUE %s  " %(owner,SEQUENCE_NAME,INCREMENT_BY, MAX_VALUE,MIN_VALUE)
                sequence_string += " NOCYCLE "  if CYCLE_FLAG == 'N' else  " CYCLE "
                sequence_string += " NOCACHE "  if CACHE_SIZE == 0  else    " CACHE %s " % CACHE_SIZE 
                sequence_string += " NOORDER "  if ORDER_FLAG == 'N' else  " ORDER "
                sequence_string += ";\n"              
                sequence_string += "ALTER SEQUENCE  %s.%s restart with %s ;\n" % (owner,SEQUENCE_NAME,LAST_NUMBER )       
            return sequence_string
        
    def get_synonyms(self, owner):
        synonym_query = """
                    SELECT SYNONYM_NAME, TABLE_OWNER, TABLE_NAME, DB_LINK, ORIGIN_CON_ID FROM ALL_SYNONYMS
                    WHERE OWNER='%s'
             """        
        synonym_string = ""
        query = synonym_query % (owner.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for SYNONYM_NAME, TABLE_OWNER, TABLE_NAME, DB_LINK, ORIGIN_CON_ID in res:
                synonym_string += "CREATE SYNONYM  %s.%s FOR  %s.%s  ; " %(owner,SYNONYM_NAME,TABLE_OWNER,TABLE_NAME) 
            return synonym_string
        
    def get_procedures(self, owner):
        procedure_query = """
                SELECT T1.OBJECT_NAME AS PROCEDURE_NAME,
                LISTAGG(T2.TEXT,'') WITHIN GROUP (ORDER BY  T2.LINE ) AS SOURCE 
                FROM ALL_OBJECTS T1, ALL_SOURCE T2 
                WHERE T1.OBJECT_NAME = T2.NAME
                AND T1.OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')
                AND T1.OWNER = '%s'
                GROUP BY T1.OBJECT_NAME
             """        
        procedure_string = ""
        query = procedure_query % (owner.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for PROCEDURE_NAME, SOURCE  in res:
                procedure_string += "CREATE OR REPLACE %s \n\n" %(SOURCE) 
            return procedure_string   
        
    def get_views(self, owner):
        view_query = """
                    SELECT A.VIEW_NAME, A.TEXT_VC,
                            LISTAGG('"'||B.COLUMN_NAME||'"',',') WITHIN GROUP (ORDER BY  B.COLUMN_ID) AS COLUMNS 
                    FROM ALL_VIEWS A, ALL_TAB_COLS B
                    WHERE A.OWNER='%s'
                        AND A.OWNER=B.OWNER
                        AND A.VIEW_NAME = B.TABLE_NAME 
                    GROUP BY A.VIEW_NAME, A.TEXT_VC
             """        
        view_string = ""
        query = view_query % (owner.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for VIEW_NAME,TEXT_VC, COLUMNS  in res:
                view_string += "CREATE OR REPLACE VIEW %s.%s (%s) AS \n%s\n\n" %(owner,VIEW_NAME,COLUMNS,TEXT_VC) 
            return view_string      
    
    def get_tiggers(self, owner):
        trigger_query = """
                SELECT TRIGGER_NAME,
                       STATUS,
                       DESCRIPTION,
                       TRIGGER_BODY 
                 FROM ALL_TRIGGERS A
                WHERE A.OWNER='%s' 
             """        
        trigger_string = ""
        query = trigger_query % (owner.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for TRIGGER_NAME,STATUS, DESCRIPTION,TRIGGER_BODY  in res:
                trigger_string += "CREATE OR REPLACE TRIGGER %s.%s%s\n\n" %(owner,DESCRIPTION,TRIGGER_BODY) 
                if STATUS == 'DISABLED':
                    trigger_string += "SET TRIGGER %s DISABLED;" % (TRIGGER_NAME.lower())
            return trigger_string    

    def get_part_colname(self,owner,type,name):
        column_query = """
                    SELECT NAME,LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_POSITION) AS PARTCOLS 
                      FROM ALL_PART_KEY_COLUMNS
                     WHERE OWNER='%s' 
                       AND OBJECT_TYPE = '%s'
                       AND NAME = '%s'
                     GROUP BY NAME
             """        
        column_string = ""
        query = column_query % (owner.upper(), type.upper(), name.upper())
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchone()
        if res is None:
            return None
        else:
           (name,partcols) = res 
           return partcols
        

    def get_table_partition(self, owner, table):
        partition_query = """
		SELECT B.TABLE_NAME,B.PARTITIONING_TYPE, B.PARTITION_COUNT,
			LISTAGG(C.PARTITION_NAME,',') WITHIN GROUP (ORDER BY  C.PARTITION_POSITION) AS PARTITIONS,
			LISTAGG(C.TABLESPACE_NAME,',') WITHIN GROUP (ORDER BY  C.PARTITION_POSITION) AS TABLSPACES,
			LISTAGG( 
				DECODE(B.PARTITIONING_TYPE,'LIST',
					extractvalue(dbms_xmlgen.getxmltype(
       		         			'SELECT HIGH_VALUE
       		           		  FROM ALL_TAB_PARTITIONS 
     							 WHERE TABLE_OWNER = '''||A.OWNER||''' AND TABLE_NAME = ''' ||A.TABLE_NAME ||  
     							 ''' AND PARTITION_NAME = ''' || C.PARTITION_NAME || ''' AND PARTITION_POSITION = '''|| C.PARTITION_POSITION || ''''), '//text()' )
     			,' ') ,',') WITHIN GROUP (ORDER BY  C.PARTITION_POSITION) AS HIGH_VALUES	
		FROM ALL_TABLES A, ALL_PART_TABLES B, ALL_TAB_PARTITIONS C
		WHERE A.OWNER='%s'
                  AND A.TABLE_NAME = '%s'
		  AND A.OWNER = B.OWNER 
		  AND A.OWNER = C.TABLE_OWNER
		  AND A.TABLE_NAME = B.TABLE_NAME 
		  AND A.PARTITIONED ='YES'
		  AND A.TABLE_NAME = C.TABLE_NAME
		GROUP BY B.TABLE_NAME,B.PARTITIONING_TYPE,B.PARTITION_COUNT
             """        
        partition_string = ""
        query = partition_query % (owner.upper(), table.upper())
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchone()
        if res is None:
            return None
        else:
            part_column = self.get_part_colname(owner,'TABLE',table)
            (TABLE_NAME,PARTITIONING_TYPE,PARTITION_COUNT,PARTITIONS,TBLSPACES,HIGH_VALUES) = res	
            partitions = re.split(r',',PARTITIONS)
            tblspaces = re.split(r',',TBLSPACES)
            hvalues = re.split(r',',HIGH_VALUES)

            partition_string += "  fragment by "
            match PARTITIONING_TYPE:
                case 'LIST':
                      partition_string += " list ( %s )\n" % ( part_column )
                      part_string = ""
                      for (partition, tblspace, hvalue) in zip(partitions,tblspaces,hvalues):
                           part_string += '    ' if part_string == "" else '   ,'
                           part_string += "partition  %s values (%s) in  %s  \n" % (partition, hvalue, tblspace)
                      partition_string += part_string
                      return partition_string
                case 'HASH':
                      #print("HASH %s " % part_column)
                      match  part_column:
                          case 'DEVICENUM':          # FOR TEST 
                               partition_string += " expression  \n" 
                               part_string = ""
                               for (partition, tblspace, hvalue) in zip(self.DEVICE_PARTITIONS,self.DEVICE_TBLSPACES,self.DEVICE_HVALUES):
                                   part_string += '    ' if part_string == "" else '   ,'
                                   part_string += " partition  %s (%s) in  %s  \n" % (partition, hvalue, tblspace)
                               partition_string += part_string
                               return partition_string
                          case  'GENDATETIME':
                               partition_string += " LIST (GENDATETIME[1,4])  \n" 
                               part_string = ""

                               for (partition, tblspace, hvalue) in zip(self.GENDATE_PARTITIONS,self.GENDATE_TBLSPACES,self.GENDATE_HVALUES):
                                   part_string += '    ' if part_string == "" else '   ,'
                                   part_string += " partition  %s " % (partition)
                                   part_string += " VALUES " if hvalue != 'REMAINDER' else ' REMAIDNER '
                                   part_string += " (%s) " % hvalue if  hvalue != 'REMAINDER' else '      '
                                   part_string += "in  %s  \n" % (tblspace)
                               partition_string += part_string
                               return partition_string

                          case  _:
                               return ""
                case _:
                    return ""
                

    def get_index_partition(self, owner, index):
        partition_query = """
		SELECT B.INDEX_NAME,B.PARTITIONING_TYPE, B.PARTITION_COUNT,
			LISTAGG(C.PARTITION_NAME,',') WITHIN GROUP (ORDER BY  C.PARTITION_POSITION) AS PARTITIONS,
			LISTAGG(C.TABLESPACE_NAME,',') WITHIN GROUP (ORDER BY  C.PARTITION_POSITION) AS TABLSPACES,
			LISTAGG( 
				DECODE(B.PARTITIONING_TYPE,'LIST',
					extractvalue(dbms_xmlgen.getxmltype(
       		         			'SELECT HIGH_VALUE
       		           		  FROM ALL_IND_PARTITIONS 
     							 WHERE INDEX_OWNER = '''||A.OWNER||''' AND INDEX_NAME = ''' ||A.INDEX_NAME ||  
     							 ''' AND PARTITION_NAME = ''' || C.PARTITION_NAME || ''' AND PARTITION_POSITION = '''|| C.PARTITION_POSITION || ''''), '//text()' )
     			,' ') ,',') WITHIN GROUP (ORDER BY  C.PARTITION_POSITION) AS HIGH_VALUES	
		FROM ALL_INDEXES A, ALL_PART_INDEXES B, ALL_IND_PARTITIONS C
		WHERE A.OWNER='%s'
          AND A.INDEX_NAME = '%s'
		  AND A.OWNER = B.OWNER 
		  AND A.OWNER = C.INDEX_OWNER
		  AND A.INDEX_NAME = B.INDEX_NAME 
		  AND A.PARTITIONED ='YES'
		  AND A.INDEX_NAME = C.INDEX_NAME
		GROUP BY B.INDEX_NAME,B.PARTITIONING_TYPE,B.PARTITION_COUNT
             """        
        partition_string = ""
        query = partition_query % (owner.upper(), index.upper())
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchone()
        if res is None:
            return None
        else:
            part_column = self.get_part_colname(owner,'INDEX',index)
            (INDEX_NAME,PARTITIONING_TYPE,PARTITION_COUNT,PARTITIONS,TBLSPACES,HIGH_VALUES) = res	
            partitions = re.split(r',',PARTITIONS)
            tblspaces = re.split(r',',TBLSPACES)
            hvalues = re.split(r',',HIGH_VALUES)

            partition_string += "  fragment by "
            match PARTITIONING_TYPE:
                case 'LIST':
                      partition_string += " list ( %s )\n" % ( part_column )
                      part_string = ""
                      for (partition, tblspace, hvalue) in zip(partitions,tblspaces,hvalues):
                           part_string += '    ' if part_string == "" else '   ,'
                           part_string += "partition  %s values (%s) in  %s  \n" % (partition, hvalue, tblspace)
                      partition_string += part_string
                      return partition_string
                case 'HASH':
                      #print("HASH %s " % part_column)
                      match  part_column:
                          case 'DEVICENUM':          # FOR TEST 
                               partition_string += " expression  \n" 
                               part_string = ""
                               for (partition, tblspace, hvalue) in zip(self.DEVICE_PARTITIONS,self.DEVICE_TBLSPACES,self.DEVICE_HVALUES):
                                   part_string += '    ' if part_string == "" else '   ,'
                                   part_string += " partition  %s (%s) in  %s  \n" % (partition, hvalue, tblspace)
                               partition_string += part_string
                               return partition_string
                          case  'GENDATETIME':
                               partition_string += " LIST (GENDATETIME[1,4])  \n" 
                               part_string = ""

                               for (partition, tblspace, hvalue) in zip(self.GENDATE_PARTITIONS,self.GENDATE_TBLSPACES,self.GENDATE_HVALUES):
                                   part_string += '    ' if part_string == "" else '   ,'
                                   part_string += " partition  %s " % (partition)
                                   part_string += " VALUES " if hvalue != 'REMAINDER' else ' REMAINDER '
                                   part_string += " (%s) " % hvalue if  hvalue != 'REMAINDER' else '      '
                                   part_string += "in  %s  \n" % (tblspace)
                               partition_string += part_string
                               return partition_string
                          case  _:
                               partition_string += " HASH ( %s )\n" % ( part_column )
                               part_string = ""
                               for (partition, tblspace, hvalue) in zip(partitions,tblspaces,hvalues):
                                   part_string += '    ' if part_string == "" else '   ,'
                                   part_string += "partition  %s values (???) in  %s  \n" % (partition, tblspace)
                               partition_string += part_string
                               return partition_string
                          
                case _:
                    return ""


        return ""   
    
    def get_indexes(self, owner, table):
        indexes_query = """
            SELECT  INDEX_NAME, UNIQUENESS, TBLSPACE_NAME,PARTITIONED,
                    LISTAGG(INDEX_COLNAME,',') WITHIN GROUP (ORDER BY  COLUMN_POSITION ) AS COLUMNS
              FROM ( 
                    SELECT A.INDEX_NAME AS INDEX_NAME,A.TABLESPACE_NAME AS TBLSPACE_NAME,A.PARTITIONED AS PARTITIONED,
                           DECODE(A.UNIQUENESS,'NONUNIQUE',' ',A.UNIQUENESS) AS UNIQUENESS, 
		                   B.COLUMN_POSITION AS COLUMN_POSITION,
		                   CASE 
                                WHEN C.USER_GENERATED = 'YES' THEN B.COLUMN_NAME ||' '||DECODE(B.DESCEND,'ASC','',B.DESCEND)
     		                    ELSE extractvalue(dbms_xmlgen.getxmltype(
                                                   'SELECT DATA_DEFAULT 
                                                      FROM ALL_TAB_COLS 
          			                                  WHERE TABLE_NAME = '''||A.TABLE_NAME||''' AND COLUMN_NAME = ''' ||  B.COLUMN_NAME || ''''
        			                              ), '//text()' ) ||' '||DECODE(B.DESCEND,'ASC','',B.DESCEND)
     		               END AS INDEX_COLNAME
		              FROM ALL_INDEXES A, ALL_IND_COLUMNS B, ALL_TAB_COLS C
		            WHERE A.OWNER='%s'
		              AND A.TABLE_NAME = '%s'
		              AND A.OWNER = B.INDEX_OWNER 
		              AND A.OWNER = C.OWNER
		              AND A.TABLE_NAME = C.TABLE_NAME
		              AND B.COLUMN_NAME = C.COLUMN_NAME
		              AND A.INDEX_NAME = B.INDEX_NAME 
                      AND A.INDEX_NAME NOT IN
                      (
                          SELECT CONSTRAINT_NAME
                            FROM ALL_CONSTRAINTS 
                           WHERE OWNER=A.OWNER
                             AND TABLE_NAME=A.TABLE_NAME
                             AND CONSTRAINT_TYPE='U'
                      )
		) D
        GROUP BY D.INDEX_NAME,D.TBLSPACE_NAME,D.PARTITIONED,D.UNIQUENESS
        """        
        indexes_string = ""
        query = indexes_query % (owner.upper(), table.upper())
        #print (query)
        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        if res is None:
            return None
        else:
            for INDEX_NAME, UNIQUENESS,TBLSPACE_NAME,PARTITIONED,COLUMNS in res:
                indexes_string += "create "
                if UNIQUENESS == "UNIQUE":
                    indexes_string += "   unique "
                else: 
                    indexes_string += "   " 
                
                indexes_string += "index %s.%s on %s ( %s ) " % ( owner.lower(), INDEX_NAME.lower(), table.lower(), COLUMNS.lower())
                if PARTITIONED != 'YES':
                    indexes_string += "\n in  %s " % TBLSPACE_NAME
                else:   
                    indexes_string += "\n %s    " % self.get_index_partition(owner,INDEX_NAME)
                indexes_string += ";\n"
            return indexes_string   
        


    def make_user_schema(self,owner):
        owner = owner.upper()
        table_filter = "OWNER = '%s' " % owner
        res= self.get_tables(table_filter)
        table_statement = ""
        if res is None:
            print ("No table found !!!\n")
        else:
            for  OWNER, TABLE_NAME,TABLESPACE_NAME,STATUS, INITIAL_EXTENT, NEXT_EXTENT, PARTITIONED,READ_ONLY,AVG_ROW_LEN in res:
                table_statement = "create table \"%s\".%s \n  ( \n" % (owner.lower(), TABLE_NAME.lower())
                table_statement += self.get_columns(owner,TABLE_NAME)           
                table_statement += "  )\n"

                init_extent = INITIAL_EXETNT if INITIAL_EXTENT >  16  else 64;
                next_extent = NEXT_EXETNT if NEXT_EXTENT >  16  else 64;

                if PARTITIONED == 'YES':
                    table_statement += self.get_table_partition(OWNER,TABLE_NAME);
                else:
                    table_statement += " in %s " %  (TABLESPACE_NAME)  

                table_statement += "   extent size %s next size %s lock mode row;\n" % (init_extent, next_extent) 

                table_statement += self.get_check_constraints(owner,TABLE_NAME)       
                table_statement += "\n"                     
                table_statement += self.get_unique_constraints(owner,TABLE_NAME)                            
                table_statement += "\n"                     
                table_statement += self.get_primary_constraints(owner,TABLE_NAME)                            
                table_statement += "\n"                     
                table_statement += self.get_indexes(owner,TABLE_NAME) 
                table_statement += "\n"                     
                table_statement += self.get_foreignkey_constraints(owner,TABLE_NAME)                           
                table_statement += "\n"                     
                print(table_statement)
        
        owner = owner.upper()
        sequence_string = self.get_sequences(owner)
        print(sequence_string)
        print("\n")
        synonym_string = self.get_synonyms(owner) 
        print("\n")
        print(synonym_string)
        print("\n")
        procedure_string = self.get_procedures(owner) 
        print(procedure_string)
        print("\n")
        view_string = self.get_views(owner) 
        print(view_string)
        print("\n")
        trigger_string = self.get_tiggers(owner) 
        print(trigger_string)
        print("\n")
        print('-------------------------------------')

def main(argv, args):
    """main"""
    oracle = Oracle_Source()
    oracle.connect()
    owner=args['u']

    res=oracle.make_user_schema(owner)
       
        
if '--version' in sys.argv:
	print(__version__)
elif __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', help=' : Please set the user name')
    args = vars(parser.parse_args())
    argv = sys.argv
    main(argv,args)
