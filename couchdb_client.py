import couchdb
import requests
from requests.auth import HTTPBasicAuth



def get_mango_query (key, logic, value):
    # type Check
    if logic in ["in","and","or"]:
        if not isinstance(value, list): 
            raise ValueError(f"{logic} 연산자는 list 타입이어야 합니다.")
        
    if logic in ["not"]: 
        if not isinstance(value, dict):
            raise ValueError("not 연산자는 dict 타입이어야 합니다.")
        
    if logic in ["==","!=",">","<",">=","<=","like","in", "regex"]: # selector query
        return {    
            "==": { key: { "$eq": value } },
            "!=": { key: { "$ne": value } },
            ">":  { key: { "$gt": value } },
            "<":  { key: { "$lt": value } },
            ">=": { key: { "$gte": value } },
            "<=": { key: { "$lte": value } },
            "like": { key: { "$regex": value } },
            "regex": { key: { "$regex": value } },
            "in": { key: { "$in": value } }
        }[logic]
    
    elif logic in ["and", "or", "not"]:
        return {    
            "and" : {"$and" : value}, # list
            "or" : {"$or" : value}, # list
            "not" : {"$not" : value} 
        }[logic]
    else: 
        raise ValueError(f"지원하지 않는 연산자: {logic}")




def mango_query_builder (selector, limit=None, sort=None):
    def recursion_query (conds):
        querys = []
        for i in range(len(conds[2])):
            query = recursion_query (conds[2][i]) if conds[2][i][1] in ['and','or'] else \
                    get_mango_query (conds[2][i][0], conds[2][i][1], conds[2][i][2])
            querys.append(query)
        return querys
    
    # get selector
    condition = selector
    query = recursion_query (condition) if condition[1] in ['and','or'] else \
            get_mango_query (condition[0], condition[1], condition[2])
    query = { "selector": query }    

    # get limit
    if limit != None: 
        query['limit'] = limit
    
    # get sort
    if sort != None:
        sort_key, sort_type = sort
        if sort_type in ['asc','desc']:
            query['sort'] = [ { sort_key: sort_type } ]
            query['use_index'] = sort_key
    return query









# couchDB 구조 - (DB - docs) 
# table : {DB_Name}_{Table_Name} : table이 없기에, DB에 이름으로 구분
class CouchDB_Manger:
    def __init__(self, db_name):
        ip, port = "192.168.17.51", "5984"
        self.server = couchdb.Server(f"http://admin:password@{ip}:{port}/")
        self.tables = self.get_tables(db_name)


    def get_tables (self, db_name): # create & get
        server = self.server
        if f"{db_name}_task" not in server : server.create(f"{db_name}_task")
        if f"{db_name}_episode" not in server : server.create(f"{db_name}_episode")
        if f"{db_name}_robot" not in server : server.create(f"{db_name}_robot")
        if f"{db_name}_camera" not in server : server.create(f"{db_name}_camera")
        if f"{db_name}_etc" not in server : server.create(f"{db_name}_etc")
        return {
            'task': server[f"{db_name}_task"],
            'episode': server[f"{db_name}_episode"],
            'robot': server[f"{db_name}_robot"],
            'camera': server[f"{db_name}_camera"],
            'etc': server[f"{db_name}_etc"], }


    def insert (self, table, doc):     
        return table.save(doc) # doc_ip, doc_rev
    
    def delete (self, table, mango_query):  
        for doc in table.find(mango_query): table.delete(doc)

    def select (self, table, mango_query): 
        return [doc for doc in table.find(mango_query)]
        
    def update (self, table, mango_query, keys, values):
        for doc in table.find(mango_query):
            for key,value in zip(keys,values): doc[key] = value
            table.save(doc)

    def clean_cache(self,table): # 물리적 데이터 정리
        table.compact()
        table.cleanup()


from datetime import datetime


if __name__ == "__main__":
    dbm = CouchDB_Manger(db_name="test")
    task_table = dbm.tables['task']

    for i in range(10):
        now = datetime.now()
        now_str = datetime.strftime(now, "%Y-%m-%d %H:%M:%S.%f")
        dbm.insert(task_table,{"task_name":"task2", "created_time":now_str})

        mquery = mango_query_builder(
            ('task_name','like','tas'),
        )
        datas = dbm.select(task_table, mquery)


    dbm.clean_cache(task_table)
    



