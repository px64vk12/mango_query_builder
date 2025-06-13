import couchdb
import requests
from datetime import datetime
import json
import time



def get_now_str ():
    now = datetime.now()
    now_str = datetime.strftime(now, "%Y-%m-%d %H:%M:%S.%f")
    return now_str



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
    """
    mango_query_builder(
        (None, 'and', [
            ('task_name','like','tas'),
            ('task_name','==','task2'),
        ]),
        limit = 100,
        sort = ('task_name', 'asc') # 'desc'
    )
    """
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
        self.ip, self.port = "192.168.17.51", "5984"
        self.user, self.password = "admin", "password"
        self.server = couchdb.Server(f"http://{self.user}:{self.password}@{self.ip}:{self.port}/")

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


    def create_index (self, table, index_name): # indexing 조회속도 향상
        self.request_db(f"/{table.name}/_index", {
                "index": {"fields": [index_name]},
                "name": f"{index_name}_idx",
                "type": "json" })

    def bulk_insert (self, table, docs): # 한번에 적재
        self.request_db(f"/{table.name}/_bulk_docs", {"docs": docs})


    def request_db (self, url, data):
        response = requests.post(
            f"http://{self.ip}:{self.port}/{url}",
            headers={"Content-Type": "application/json"},
            auth=(self.user, self.password),
            data=json.dumps(data))
        #print(response.status_code)
        return response.json()

# 15hz * 120sec image = 1.6gb
# 15hz * 120sec H.264 = 30mb
# 1000 episode = 30mb * 1000 = 30gb

# benchmark
# meta_data 1mb         : 5000docs
# read speed            : 10000docs / 1sec
# single insert speed   : 10000docs - 200sec, 50docs / 1sec
# bulk insert speed (100개씩): 10000 docs - 5.4sec, 2000docs /1sec

if __name__ == "__main__":
    dbm = CouchDB_Manger(db_name="test")
    task_table = dbm.tables['task']
    dbm.create_index(task_table, "created_time")


    if 0: # single insert test
        ts = time.time()
        for i in range(10000):
            now_str = get_now_str()
            dbm.insert(task_table,{"task_name":"task2", "created_time":now_str, })
            if i%500 == 0: print(time.time() - ts)


    if 1: # bulk insert test
        ts = time.time()
        for i in range(100):
            now_str = get_now_str()
            datas = [{"task_name":"task2", "created_time":now_str}
                     for i in range(100) ]
            dbm.bulk_insert(task_table,datas)
            if i%5 == 0: print(time.time() - ts)


    if 1: # select test
        mquery = mango_query_builder (
            ('task_name','like','tas'),
            sort = ('created_time', 'asc'),
        )
        ts = time.time()
        datas = dbm.select(task_table, mquery)
        print(time.time() - ts)
        print(len(datas))


    dbm.clean_cache(task_table)
    



