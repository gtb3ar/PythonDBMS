import sqlite3
class SQL_Handler:
    #Class Variables
    connection = None
    cursor = None

    #Method Constructs Class
    def __init__(self):
        self.openConnection()
    #Method opens and sets connection and cursor
    def openConnection(self):
        try:
            self.connection = sqlite3.connect('reviews.db')
            self.cursor = self.connection.cursor()
        except sqlite3.Error as error:
            print("Error while connecting to sqlite", error)
    #Method Closes Connection to SQLite
    def closeConnection(self):
        if self.connection:
            self.closeCursor()
            cursor = None
            self.connection.close()
            self.connection = None
            print("Closed Connection")
    #Method aquires cursor from connection
    def openCursor(self):
        try:
            self.cursor = self.connection.cursor()
        except sqlite3.Error as error:
                print("Error while opening Cursor", error)
    #Method closes and removes cursor reference
    def closeCursor(self):
        if self.cursor:
            self.cursor.close()
    #Method executes a given string query and disposes of cursor
    def executeQuery(self,query):
        output = ""
        try:
            self.openCursor()
            self.cursor.execute(query)
            output = self.cursor.fetchall()
            self.closeCursor()
        except sqlite3.Error as error:
            print("Error while executing Query", error)
        return output
    #Method Finalizes added and updated entries to disk
    def commitData(self):
        self.connection.commit()
    #Seperates array into single string for items seperated by comma ie. Reviews, Place
    def bundleComma(self,targets):
        targetQuery = ""
        target_count = 0
        if isinstance(targets, str):
            targetQuery = targets
        else:
            for target in targets:
                if(target_count == 0):
                    targetQuery = target 
                else:
                    targetQuery = targetQuery + "," + target
            
                target_count = target_count + 1
        return targetQuery
    #Seperates array into targets to be returned by select by converting array into quotated string
    def bundleSelect(self,targets):
        targetQuery = ""
        target_count = 0
        for target in targets:
            if(target_count == 0):
                targetQuery = "'"+target+"'"
            else:
                targetQuery = targetQuery + "," + "'"+target+"'"
            
            target_count = target_count + 1
        return targetQuery
    #Method seperates the conditions into arrays to be performed by JOIN clause or WHERE clause
    def seperateConditionalArray(self,conditionals):
        join_conditions = []
        where_conditions = []
        for condition in conditionals:
            x, y = condition
            if '.' in x:
                join_conditions.append(condition)
            else:
                where_conditions.append(condition)

        return join_conditions, where_conditions
            
    #Method bundles where clause into single string to be appended
    def bundleWhereClause(self,conditionals):
        conditionQuery = ""
        condition_count = 0
      
        if(len(conditionals) != 0):
            conditionQuery = " Where "
            for condition in conditionals:
                x, y = condition
                if(condition_count == 0):
                    conditionQuery = conditionQuery + "["+x+"]="+"'"+str(y)+"'"
                else:
                    conditionQuery = conditionQuery + " AND " + "["+x+"]="+"'"+str(y)+"'"
                condition_count = condition_count + 1
        return conditionQuery
    #Method bundles join clause into single string to be appended
    def bundleJoinClause(self,conditionals,table):
        conditionQuery = ""
        condition_count = 0
        if(len(table) > 1 and not isinstance(table,str)):
            table.remove(table[0])
        if(len(conditionals) > 0):
            conditionQuery = " Join "+ self.bundleComma(table) + " ON "
            for condition in conditionals:
                x, y = condition
                if(condition_count == 0):
                    conditionQuery = conditionQuery + str(x) +"="+ str(y)
                else:
                    conditionQuery = conditionQuery + " AND " + str(x) +"="+ str(y)
                condition_count = condition_count + 1
        return conditionQuery
    
    #Method converts sort array into a single string with either 'Asc' or 'Desc' located at the end
    def bundleSortByQuery(self,query):
        sortbyQuery = ""
        if(len(query) > 1):
            end_clause = query[len(query)-1]
            query.remove(end_clause)
            sortbyQuery = " Order by " + self.bundleComma(query) + " " + end_clause
            
            
        return(sortbyQuery)
    #Method converts update value array into a single string query
    def bundleUpdateQueryValues(self,values):
        valueQuery = ""
        value_count = 0
      
        if(len(values) != 0):
            for value in values:
                x, y = value
                if(value_count == 0):
                    valueQuery = valueQuery + "["+x+"]="+"'"+str(y)+"'"
                else:
                    valueQuery = valueQuery + " , " + "["+x+"]="+"'"+str(y)+"'"
                value_count = value_count + 1
        return valueQuery
    #Method returns a column count for a table
    def queryColumnCount(self,table):
        query = "select count(*) from pragma_table_info('"+table+"')"
        return self.executeQuery(query)[0][0]

    #Method reads for multiple columns, tables and conditional 
    def readValues(self,columns,table,conditionals,sortby):
        join, where = self.seperateConditionalArray(conditionals)
        if(len(join) > 0):
            tableQuery = self.bundleComma(table[0])
        else:
            tableQuery = self.bundleComma(table)
        targetQuery = self.bundleComma(columns)
        
        joinQuery = self.bundleJoinClause(join,table)
        conditionQuery = self.bundleWhereClause(where)
        sortQuery = self.bundleSortByQuery(sortby)
        
        select_query = "Select " + targetQuery + " From " + tableQuery + joinQuery + conditionQuery + sortQuery       
        return self.executeQuery(select_query)
    #Method updates a table entry
    def updateEntry(self,table,values,conditions):
        valueQuery = self.bundleUpdateQueryValues(values)
        conditionQuery = self.bundleWhereClause(conditions)
        updateQuery = "Update " + table + " Set " + valueQuery  + conditionQuery
        print(updateQuery)
        self.executeQuery(updateQuery)
        self.commitData()
    #Appends entry to table
    def addEntry(self,table,values):
        length = len(values)
        if(length == self.queryColumnCount(table)):
            insert_query = "Insert Into " + table + " Values (" + self.bundleSelect(values) + ")"
            self.executeQuery(insert_query)
            self.commitData()
        else:
            print("Error: Submitted values have incompatible length with table")
    #MAIN
def main():
    handler = SQL_Handler()
    
    idexample = 4

    dataToSelect = ["Users.User_Name","Reviews.Review_Name"]
    table1 = ["Users","Reviews","Place"]
    conditions = [("Users.User_Name","Reviews.User_Name"),("Place.Place_id","Reviews.Place_id"),("Place.Place_id",idexample)]
    sort = [("Reviews.Place_id","Asc")]
    
    test1 = handler.readValues(dataToSelect,table1,conditions,sort)
    print(test1) 

    #table2 = "Place"
    #dataToUpdate = ([("Address","Jackson St.")])
    #conditions2 = [("Place_id","2")]
    #handler.updateEntry(table2,dataToUpdate,conditions2)

    #table3 = "Place"
    #dataToAdd = ("21","Burgers Stall","3pm-12pm","875 Jefferson St","628-5312","Cheap")
    #handler.addEntry(table3,dataToAdd)

    handler.closeConnection()
if __name__ == "__main__":
    main()
