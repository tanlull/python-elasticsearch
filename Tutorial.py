# Import Elasticsearch package 
from elasticsearch import Elasticsearch 
database = "localhost"

# Connect to the elastic cluster
def connect():
    es=Elasticsearch([{'host':database,'port':9200}])
    #print(es)
    return es

# Insert
def insert(es ,index, data):
    #print(data)
    #Now let's store this document in Elasticsearch 
    res = es.index(index='megacorp',doc_type='employee',id=index,body=data)
    return res

def insertData(es):
    e1={
            "first_name":"nitin",
            "last_name":"panwar",
            "age": 27,
            "about": "Love to play cricket",
            "interests": ['sports','music'],
        }
    res = insert(es,1,e1)
    print(res)

    e2={
        "first_name" :  "Jane",
        "last_name" :   "Smith",
        "age" :         32,
        "about" :       "I like to collect rock albums",
        "interests":  [ "music" ]
    }
    res= insert(es,2,e2)
    print(res)

    e3={
        "first_name" :  "Douglas",
        "last_name" :   "Fir",
        "age" :         35,
        "about":        "I like to build cabinets",
        "interests":  [ "forestry" ]
    }
    res = insert(es,3,e3)
    print(res)

def deleteByID(es,id):
    res=es.delete(index='megacorp',doc_type='employee',id=id)
    print(res['result'])
    return res

def queryByID(es,id):
    res=es.get(index='megacorp',doc_type='employee',id=id)
    #print(res)
    print(res['_source'])
    return res
    #{u'_type': u'employee', u'_source': {u'interests': [u'forestry'], u'age': 35, u'about': u'I like to build cabinets', u'last_name': u'Fir', u'first_name': u'Douglas'}, u'_index': u'megacorp', u'_version': 1, u'found': True, u'_id': u'3'}

def queryAll(es):
    res= es.search(index='megacorp',body={'query':{'match_all':{}}})
    print('Got %d hits:' %res['hits']['total'])
    return res

def query(es):
    #First Name
    res= es.search(index='megacorp',body={'query':{'match':{'first_name':'nitin'}}})

    #boolean
    res= es.search(index='megacorp',body={
        'query':{
            'bool':{
                'must':[{
                        'match':{
                            'first_name':'nitin'
                        }
                    }]
            }
        }
    })

    #Filter
    res= es.search(index='megacorp',body={
        'query':{
            'bool':{
                'must':{
                    'match':{
                       'first_name':'nitin'
                    }
                },
                "filter":{
                    "range":{
                        "age":{
                            "gt":25
                        }
                    }
                }
            }
        }
    })

    print(res['hits']['hits'])

def fullTextSearch(es):
    e4={
    "first_name":"asd",
    "last_name":"pafdfd",
    "age": 27,
    "about": "Love to play football",
    "interests": ['sports','music'],
    }
    res=es.index(index='megacorp',doc_type='employee',id=4,body=e4)
    #print(res['created'])
    res= es.search(index='megacorp',body={
            'query':{
                'match':{
                    "about":"play cricket"
                }
            }
        })
    for hit in res['hits']['hits']:
        print(hit['_source']['about'])
        print(hit['_score'])
        print('**********************')

def phraseSearch(es):
    res= es.search(index='megacorp',body={
        'query':{
            'match_phrase':{
                "about":"play cricket"
            }
        }
    })
    for hit in res['hits']['hits']:
        print(hit['_source']['about'])
        print(hit['_score'])
        print('**********************')


if __name__ == '__main__':
    es = connect()
    #insertData(es)
    #res = queryByID(es,3)
    #res = deleteByID(es,3)
    res = queryAll(es)

    #res=query(es)
    #res = fullTextSearch(es)
    #res = phraseSearch(es)
    print(res)
   


