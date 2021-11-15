


###************************************************ CLIENT MATCHING  *******************************************************

import os
import pandas as pd
import sys
import psycopg2

import elasticsearch
from elasticsearch import Elasticsearch
from opensearch import OpenSearch
import time

es = OpenSearch(['https://vetsuccess-es:phUB6qfTtcDteMs$@search-es-production-rmuulr74qhxwofwrsc3ogvfgoi.us-east-1.es.amazonaws.com:443'])

data_path = "data_client_all3.csv"
df_vso_nodata_before = pd.read_csv(data_path)
df_vso_nodata_before_check_null = df_vso_nodata_before

statement = """
select practice_identifier , practice_id 
from practice_data_sources pds
where data_source = 'vetsource'
and connection_status = 'confirmed'
"""

conn=psycopg2.connect(dbname= 'citus', host='c.fyrstoirz55b3zklfjd4mxo7uli.db.citusdata.com', port= '5432', user= 'ba_user', password= 'KWbXfDwH-aTLykxY9R-VyQ')
cursor = conn.cursor()

df_practice_identifier = pd.read_sql(statement, con=conn)

df_vso_nodata_before['practice_key'] = df_vso_nodata_before['practice_key'].apply(str)
df_vso_nodata_before = df_vso_nodata_before.merge(df_practice_identifier, left_on='practice_key', right_on='practice_identifier')
df_vso_nodata_before = df_vso_nodata_before.loc[df_vso_nodata_before['practice_id'].notnull()]

#df_vso_nonull = df_vso_nodata_before.dropna() 
df_vso_nonull = df_vso_nodata_before
df_vso_nonull['zipcode'] = df_vso_nonull['zipcode'].fillna(0)

# normalizing the data and then removing the duplicates
for col in df_vso_nonull.columns:
    df_vso_nonull[col] = df_vso_nonull[col].apply(str)
    df_vso_nonull[col] = df_vso_nonull[col].str.lower()
    df_vso_nonull[col] = df_vso_nonull[col].str.lstrip()
    df_vso_nonull[col] = df_vso_nonull[col].str.rstrip()

    
df_vso_nonull = df_vso_nonull.drop_duplicates()

# getting different manipulations of address, renaming the main field address1 in raw database
df_vso_nonull = df_vso_nonull.rename(columns={'address1': 'address'})
df_vso_nonull['address1'] = df_vso_nonull['address']
remove_str = ['avenue', 'drive', 'road', 'rd.', 'ave.', 'dr.','st.', 'blvd.', 'ln.', 'rd', 'ave', 'dr','st', 'blvd', 'ln'  ]

for str_remove in remove_str:
    df_vso_nonull.loc[:,('address1')] = df_vso_nonull['address1'].astype(str).str.replace(str_remove, '')

df_vso_nonull['address2'] = df_vso_nonull['address1']
df_vso_nonull.loc[:,('address2')] = df_vso_nonull.address1.str.split().str[0] + ' ' + df_vso_nonull.address1.str.split().str[1]

for col in df_vso_nonull.columns:
    df_vso_nonull[col] = df_vso_nonull[col].str.lstrip()
    df_vso_nonull[col] = df_vso_nonull[col].str.rstrip()


def case1(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='clients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"pms_id": customer_external_id} }}},
                                                        #{ "constant_score": { "filter": { "match": { "last_name": last_name }}}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

def case2(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='clients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": { "filter": { "term": { "pms_id.keyword": customer_external_id} }}},
                                                        #{ "constant_score": { "filter": { "match": { "last_name": last_name }}}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits


def case3(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                              "must": [
                                                { "constant_score": { "filter": { "match": { "name": pet_name } }}},
                                                { "constant_score": { "filter": { "match": { "primary_client_pms_id": customer_external_id } }}}
                                              ],
                                              "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits 

# this is the case where the client external_id is missing
def case4(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='clients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": { "filter": { "match": { "first_name": first_name }}}},
                                                        { "constant_score": { "filter": { "match": { "last_name": last_name }}}},
                                                        { "constant_score": { "filter": { "match": { "name": pet_name }}}}
                                                        ],
                                              "should": [
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode } } } },
                                                        { "constant_score": { "filter": { "match": { "primary_address_state": state } } } }
                                                       
                                              ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})

    return hits

# related to address with client database
def case5(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='clients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"first_name": first_name} }}},
                                                        { "constant_score": { "filter": { "match": { "last_name": last_name }}}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}}}
                                                        ],
                                                "should": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address1 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address2 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address3 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address4 }}, "boost": 2}},
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

def case6(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='clients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": { "filter": { "match": { "last_name": last_name }}}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}}}
                                                        ],
                                                "should": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address1 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address2 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address3 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address4 }}, "boost": 2}}
                                                        
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits
## rest of the queries we are trying to match using patients database

def case7(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        {  "constant_score": { "filter": { "match": {"pms_id":  pet_external_id} } }},
                                                        {  "constant_score": {  "filter": { "match": { "primary_client_last_name": last_name } } }},
                                                        { "constant_score": { "filter": { "match": { "primary_client_first_name": first_name } } }}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

def case8(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "term": {"pms_id.keyword":  pet_external_id} }}} , 
                                                        { "constant_score": { "filter": { "match": { "primary_client_last_name": last_name } }}}, 
                                                        { "constant_score": { "filter": { "match": { "primary_client_first_name": first_name } }}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})

    return hits

def case9(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"name": pet_name} }}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}, "boost": 2}}
                                                        ],
                                                "should": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address2 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address1 }}, "boost": 2}},
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits


def case10(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"name": pet_name} }}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}, "boost": 2}}
                                                        ],
                                                "should": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address2 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address1 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_client_last_name": last_name }}, "boost": 2}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits


def case11(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"name": pet_name} }}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address4 }}, "boost": 2}}
                                                        ],
                                                "should": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address3 }}, "boost": 2}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}, "boost": 1}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

def case12(first_name, last_name, customer_external_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                         { "constant_score": {  "filter": { "match": {"primary_client_first_name": first_name} }}},
                                                        { "constant_score": { "filter": { "match": { "primary_client_last_name": last_name }}}},
                                                        { "constant_score": { "filter": { "match": { "primary_client_pms_id": customer_external_id }}}}
                                                        ],
                                                
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

# these are the potenial where we might not have the client information associated with the data
def case13(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": { "filter": { "term": {"pms_id.keyword":  pet_external_id} }}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})

    return hits

def case14(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"pms_id":  pet_external_id} }}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})

    return hits

a = []
count_total = 0 #counting the total number of cases
count_notfount= 0
count_found = 0
didnt_work_cases = []
count_track = 0
k=1
count_error = 0

for i, row in df_vso_nonull.iterrows():
    try:
        count_track += 1

        if i%100 == 0:
            print(i)

        if count_track%100 == 0:
            print(f'cases found: {count_found}, cases not found: {count_notfount}, error count = {count_error} , count_total = {count_total}\n\n\n')
        
        count_total += 1
        customer_ship_to_key = (row["customer_ship_to_key"])
        first_name = (row["first_name"])
        last_name = (row["last_name"])
        address = (row["address"])
        state = (row["state"])
        city = (row["city"])
        zipcode = str(int(float(row["zipcode"])))
        practice_key = (row["practice_key"])
        pet_name = (row["pet_name"])
        pet_external_id = (row["pet_external_id"])
        pet_key = (row["pet_key"])
        practice_id = (row["practice_id"])
        customer_external_id = (row["customer_external_id"])
        visitor_id = (row["visitor_id"])
        address1 = (row["address1"]) # we dont have the key words like dr, ln and more
        address2 = (row["address2"]) # just picking first two words from the string
        address3 = max(address1.split(), key=len) # biggest word in the string.. its generally the key word in the address.
        address4 = address1.split()[0] # just the first word in the string, its mostly yhe hosue number

        # case 1: we used term for the cases where the pms_id contains special characters
    
        hits = case1(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)

        if hits['hits']['total']['value'] == 1:
            
            #hint  = hits['hits']['hits'][0]['_source']
            data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                    practice_id, pet_name, 'case1', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
            a.append(data)
            count_found += 1
            print(i,' case1')

        else:
            # Case2: using match for pms_id to not to be case sensitive
            hits = case2(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)

            if hits['hits']['total']['value'] == 1:
                
                #hint  = hits['hits']['hits'][0]['_source']
                data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address,address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                        practice_id,pet_name, 'case2', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]    
                a.append(data)
                count_found += 1
                print(i, ' case2 ')

            else:
                # case3 where the client name is not present so only ising the pet external id
                hits = case3(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                if hits['hits']['total']['value'] == 1:
                    
                    #hint  = hits['hits']['hits'][0]['_source']
                    data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address,address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                            practice_id, pet_name, 'case3', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                    a.append(data)
                    count_found += 1
                    print(i, ' case3')

                else:
                    # case4
                    hits = case4(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                    if hits['hits']['total']['value'] == 1:
                        
                        #hint  = hits['hits']['hits'][0]['_source']
                        data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                                practice_id, pet_name, 'case4', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        a.append(data)
                        count_found += 1
                        print(i, ' case4 ****************')

                    else:
                        # case 5
                        hits = case5(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)
                        pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id

                        if hits['hits']['total']['value'] == 1 and hits['hits']['max_score'] > 3:
                            
                            #hint = hits['hits']['hits'][0]['_source']
                            data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address,address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                                    practice_id, pet_name, 'case5_pet_match', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                            a.append(data)
                            count_found += 1
                            print(i, ' case5 *************')

                        else:
                            # case6
                            hits = case6(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)
                            if hits['hits']['total']['value'] == 1 and hits['hits']['max_score'] > 3:
                        
                                #hint  = hits['hits']['hits'][0]['_source']
                                data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                                        practice_id, pet_name,'case6_pet_match', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                a.append(data)
                                count_found += 1
                                print(i, ' case6 *************')

                            else:
                                # case7
                                hits = case7(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                                if hits['hits']['total']['value'] == 1:
                        
                                    #hint  = hits['hits']['hits'][0]['_source']
                                    data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                                            practice_id,pet_name, 'case7_pet_match',hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                    a.append(data)
                                    count_found += 1
                                    print(i, ' case7 *************')

                                else:
                                    # case8
                                    hits = case8(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                                    if hits['hits']['total']['value'] == 1:
                        
                                        #hint  = hits['hits']['hits'][0]['_source']
                                        data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                                                practice_id, pet_name, 'case8_pet_match', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                        a.append(data)
                                        count_found += 1
                                        print(i, ' case8 *************')

                                    else:
                                        
                                        hits = case9(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)
                                        if hits['hits']['total']['value'] >= 1 and hits['hits']['max_score'] > 3:
                
                                            #hint  = hits['hits']['hits'][0]['_source']
                                            data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, address1, address2, address3, address4, state, city, zipcode, pet_external_id, 
                                                    practice_id,pet_name, 'case9', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                            a.append(data)
                                            count_found += 1
                                            print(i, ' case9 *************')
                                        else:
                                            
                                            hits = case10(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)
                                            if hits['hits']['total']['value'] >= 1 and hits['hits']['max_score'] > 3:
                
                                                #hint  = hits['hits']['hits'][0]['_source']
                                                data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, address1, address2, address3, address4, state, city, zipcode, pet_external_id, practice_id, pet_name, 
                                                        'case10', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                     
                                                a.append(data)
                                                count_found += 1
                                                print(i, ' case10 *************')

                                            else:
                                                hits = case11(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)
                                                if hits['hits']['total']['value'] >= 1 and hits['hits']['max_score'] > 3:
                    
                                                    #hint  = hits['hits']['hits'][0]['_source']
                                                    data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, state, address1, address2, address3, address4, city, zipcode, pet_external_id, practice_id, pet_name, 
                                                            'case11', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                                                    a.append(data)
                                                    count_found += 1
                                                    print(i, ' case11 *************')
                                                
                                                else:
                                                    hits = case12(first_name, last_name, customer_external_id)
                                                    if hits['hits']['total']['value'] >= 1:
                        
                                                        #hint  = hits['hits']['hits'][0]['_source']
                                                        data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address, address1, address2, address3, address4, state, city, zipcode, pet_external_id, practice_id, pet_name, 
                                                                'case12', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                                                        a.append(data)
                                                        count_found += 1
                                                        print(i, ' case12 *************')

                                                    else:
                                                        hits = case13(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                                                        if hits['hits']['total']['value'] >= 1:
                                                        
                                                            #hint = hits['hits']['hits'][0]['_source']
                                                            data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address1, state, address1, address2, address3, address4, city, zipcode, pet_external_id, practice_id, pet_name, 
                                                                    'case13', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                                            a.append(data)
                                                            count_found += 1

                                                            print('case 13 found: ', hits['hits']['total']['value'])

                                                    
                                                        else:
                                                            hits = case14(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id) 
                                                            if hits['hits']['total']['value'] >= 1:
                            
                                                        
                                                                #hint = hits['hits']['hits'][0]['_source']
                                                                data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address1, state, address1, address2, address3, address4, city, zipcode, pet_external_id, practice_id, pet_name, 
                                                                        'case14', hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                                                a.append(data)
                                                                count_found += 1

                                                                print('case 14 found: ', hits['hits']['total']['value'])

                                                            else:
                                                                print(i, 'not found', customer_external_id, customer_ship_to_key, first_name, last_name, pet_name, address1, address1, address2, address3, address4, state, city, zipcode, pet_external_id, practice_id)
                                                                data = [i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address1, address1, address2, address3, address4, state, 
                                                                        city, zipcode, pet_external_id, practice_id, pet_name, 'not found', '', '', '']
                                                                a.append(data)
                                                                count_notfount += 1
            
        if count_total % 20000 == 0:
            print('*****************************                         storing                        **************************\n\n\n\n')
            df_clients_new_found =pd.DataFrame(a, columns = ['i', 'visitor_id', 'customer_external_id', 'customer_ship_to_key', 'first_name', 'last_name', 'address1','address1', 'address2', 'address3', 'address4', 'state', 'city', 'zipcode', 
                                                                'pet_external_id', 'practice_id', 'pet_name', 'status', 'num_hits', 'hits', 'first_hit'])
                        
            timestr = time.strftime("%Y%m%d_%H%M%S")
            df_clients_new_found.to_csv("clients_matches/df_clients_all_{}_{}.csv".format( k, timestr))
            k+=1

            print("we stopped at: ",i)
            print(f'total count of search is: {count_total}')
            print(f'total not found is:       {count_notfount}')
            print(f'total found cases are:    {count_found}')
            print(f'total ERROR cases are:    {count_error}')
            print('\n\n\n')

    except:
        print('viola.. this didnt work .. ERROR HAPPENED .....' )
        print(i, 'ERROR HAPPENED', i, customer_external_id, customer_ship_to_key, first_name, last_name, address1, state, city, zipcode, pet_external_id, practice_id, 'not_found')
        didnt_work_cases.append([i, visitor_id, customer_external_id, customer_ship_to_key, first_name, last_name, address1, state, city, zipcode, pet_external_id, practice_id, pet_name,'error'])
        count_error += 1



print("we stopped at: ",i)
print(f'total count of search is {count_total}')
print(f'total not found is: {count_notfount}')
print(f'total found cases are {count_found}')

df_clients_new_found = pd.DataFrame(a, columns = ['i', 'visitor_id', 'customer_external_id', 'customer_ship_to_key', 'first_name', 'last_name', 'address1','address1', 'address2', 'address3', 
                                                  'address4', 'state', 'city', 'zipcode', 'pet_external_id', 'practice_id', 'pet_name', 'status', 'num_hits', 'hits', 'first_hit'])
df_clients_new_found.to_csv("clients_matches/df_clients_new_found_all.csv")

df_clients_didnt_work = pd.DataFrame(didnt_work_cases, columns = ['i', 'visitor_id', 'customer_external_id', 'customer_ship_to_key', 'first_name', 'last_name', 'address1', 'state', 'city',
                                                                 'zipcode', 'pet_external_id', 'practice_id','pet_name', 'status'])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
df_clients_didnt_work.to_csv("clients_matches/df_clients_error_all.csv")




