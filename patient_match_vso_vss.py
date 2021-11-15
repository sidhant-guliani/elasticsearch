# patient matching 

import os
import pandas as pd
import sys
import psycopg2

import elasticsearch
from elasticsearch import Elasticsearch
from opensearch import OpenSearch

from queries_vso_vss1 import queries
import time

es = OpenSearch(['https://vetsuccess-es:phUB6qfTtcDteMs$@search-es-production-rmuulr74qhxwofwrsc3ogvfgoi.us-east-1.es.amazonaws.com:443'])

data_path = "model1QA.csv" # this contains the data for all the patients for which we are trying to find data
df_vso_nodata_before = pd.read_csv(data_path)
#df_vso_nodata_before_check_null = df_vso_nodata_before
#df_vso_nodata_before['zipcode'] = df_vso_nodata_before['zipcode']

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

#df_vso_nonull = df_vso_nodata_before.dropna() # commented this because we are not dropping the ones for which we dont have any data.
df_vso_nonull = df_vso_nodata_before
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
remove_str = ['avenue', 'drive', 'road', 'rd.', 'ave.', 'dr.','st.', 'blvd.', 'ln.', 'ave', 'blvd', 'ln']

for str_remove in remove_str:
    df_vso_nonull.loc[:,('address1')] = df_vso_nonull['address1'].astype(str).str.replace(str_remove, '')

df_vso_nonull['address2'] = df_vso_nonull['address1']
df_vso_nonull.loc[:,('address2')] = df_vso_nonull.address1.str.split().str[0] + ' ' + df_vso_nonull.address1.str.split().str[1]


# we are getting the data with nulls (CASE2)
#df_data_missing = df_vso_nodata_before.loc[~df_vso_nodata_before.index.isin(df_vso_nodata_before.dropna().index)]

## dirty step to do:
#df_vso_nonull = df_data_missing


# getting rid of the practices for which we dont have practice id in VSS data
#unique_practices = df_vso_nonull.practice_id.unique()
#print(f'total unique practices in the dataset are: {len(unique_practices)}')
#pract_not_found = []

#for pract in unique_practices:
#    hit = es.search(index='patients', body={"query": {"bool": {"filter": { "match": { "practice_id": pract } }}}})
#    if hit['hits']['total']['value'] == 0:
#        pract_not_found.append(pract)

#df_tofind_round2 = df_data_missing[~df_data_missing.practice_id.isin(pract_not_found)]
#print(f'total number of practices with no data: {len(pract_not_found)} whcih corresponds to {len(df_tofind_round2)} of data')

for col in df_vso_nonull.columns:
    df_vso_nonull[col] = df_vso_nonull[col].str.lstrip()
    df_vso_nonull[col] = df_vso_nonull[col].str.rstrip()


def case1(pet_external_id, last_name, first_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        {  "constant_score": { "filter": { "term": {"pms_id.keyword":  pet_external_id} } }},
                                                        {  "constant_score": {  "filter": { "match": { "primary_client_last_name": last_name } } }},
                                                        { "constant_score": { "filter": { "match": { "primary_client_first_name": first_name } } }}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

def case2(pet_external_id, last_name, first_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"pms_id":  pet_external_id} }}} , 
                                                        { "constant_score": { "filter": { "match": { "primary_client_last_name": last_name } }}}, 
                                                        { "constant_score": { "filter": { "match": { "primary_client_first_name": first_name } }}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

def case3(pet_external_id, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": { "filter": { "term": {"pms_id.keyword":  pet_external_id} }}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

def case4(pet_external_id, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"pms_id":  pet_external_id} }}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

def case5(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                              "must": [
                                                { "constant_score": { "filter": { "prefix": { "pms_id": pet_external_id } }}},
                                                { "constant_score": { "filter": { "prefix": { "primary_client_pms_id": customer_external_id } }}}
                                              ],
                                              "should": [
                                                { "constant_score": { "filter": { "match": { "primary_client_first_name": first_name } }, "boost": 2 } },
                                                { "constant_score": { "filter": { "match": { "primary_client_last_name": last_name } }, "boost": 4 } },
                                                { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode } } } },
                                                { "constant_score": { "filter": { "match": { "primary_address_state": state } } } },
                                                { "constant_score": { "filter": { "match": { "name": pet_name } }, "boost": 2 } }
                                              ],
                                              "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits 

# queries for CASE2
# below are the queries for the records where we are not able to match the cases where pms_id or customer_id are missing
# matching by name and customer id

def case6(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                              "must": [
                                                { "constant_score": { "filter": { "match": { "name": pet_name } }}},
                                                { "constant_score": { "filter": { "match": { "primary_client_pms_id": customer_external_id } }}}
                                              ],
                                              "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits 


# in this case we are focused on the cases where the clientid/practiceid is missing hence matching by pet name, customer name and postal code
def case7(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                              "must": [
                                                { "constant_score": { "filter": { "match": { "name": pet_name } }}},
                                                { "constant_score": { "filter": { "match": { "primary_client_first_name": first_name }}}},
                                                { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}}},
                                                { "constant_score": { "filter": { "match": { "primary_client_last_name": last_name }}}}
                                              ],
                                              "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits 

# same as above but we are using term in zipcode as there are some special characters in the zipcode
def case8(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                              "must": [
                                                { "constant_score": { "filter": { "match": { "name": pet_name } }}},
                                                { "constant_score": { "filter": { "match": { "primary_client_first_name": first_name }}}},
                                                { "constant_score": { "filter": { "term": { "primary_address_postal_code.keyword": zipcode }}}},
                                                { "constant_score": { "filter": { "match": { "primary_client_last_name": last_name }}}}
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



# this case is abd for the cases where the po box is there
def case10(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"name": pet_name} }}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address4 }}}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address3 }}}}
                                                        ],
                                                "should": [
                                                        { "constant_score": {  "filter": { "match": {"primary_address_line_1": address1 }}, "boost": 3}},
                                                        { "constant_score": {  "filter": { "match": {"primary_address_line_1": address2 }}, "boost": 2}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}, "boost": 1}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits


def case11(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                              "must": [
                                                { "constant_score": { "filter": { "match": { "name": pet_name } }}},
                                                { "constant_score": { "filter": { "match": { "primary_client_first_name": first_name }}}},
                                                { "constant_score": { "filter": { "match": { "primary_client_last_name": last_name }}}}
                                              ],
                                              "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits 

def case12(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='patients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match": {"name": pet_name} }}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}}}
                                                        ],
                                                "should": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address2 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address1 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_client_last_name": last_name }}, "boost": 2}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits

## case 13 and 14 are client related cases, these shows that the corresponding client is in the data but there is no 
def case13(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='clients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"first_name": first_name} }}},
                                                        { "constant_score": { "filter": { "match": { "last_name": last_name }}}}
                                                        ],
                                                "should": [
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address1 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address2 }}, "boost": 2}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address3 }}, "boost": 2}},
                                                        { "constant_score": {  "filter": { "match_phrase": {"primary_address_line_1": address4 }}, "boost": 2}}
                                                        ],
                                                "filter": [{ "match": { "practice_id": practice_id } }]
                                            }}})
    return hits


def case14(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city):
    hits = es.search(index='clients', body={"query": { 
                                            "bool": {
                                                "must": [
                                                        { "constant_score": { "filter": { "match": { "last_name": last_name }}}},
                                                        { "constant_score": { "filter": { "match": { "primary_address_postal_code": zipcode }}, "boost": 2}}
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


a = []  

count_total = 0 # counting the total number of cases
count_notfount= 0
count_found = 0
didnt_work_cases = []
count_track = 0
count_error = 0
k=1

for i, row in df_vso_nonull.iterrows():
    try:
        count_track += 1
        if i%100 == 0:
            print(i)

        if count_track%100 == 0:
            print('\n\n\n')
            print(f'cases found: {count_found}, cases not found: {count_notfount} \n\n\n')
        
        count_total += 1
        customer_external_id = (row["customer_external_id"])
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
        pet_id = (row["pet_id"])
        practice_id = (row["practice_id"])
    
        address1 = (row["address1"]) # we dont have the key words like dr, ln and more
        address2 = (row["address2"]) # just picking first two words from the string
        address3 = max(address1.split(), key=len) # biggest word in the string.. its generally the key word in the address.
        address4 = address1.split()[0] # just the first word in the string, its mostly the house number

    
        # case 1: we used term for the cases where the pms_id contains special characters
        hits = case1(pet_external_id, last_name, first_name, practice_id)

        if hits['hits']['total']['value'] == 1:
            #hint  = hits['hits']['hits'][0]['_source']
            data = [i, pet_id, pet_external_id, practice_id, pet_name,
                    first_name, last_name, zipcode, state, city, 'case1',address, address1, address2, address3, address4,customer_external_id,
                    hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
            a.append(data)
            count_found += 1
            #print('case1 found')

        else:
            # Case2: using match for pms_id to not to be case sensitive
            hits = case2(pet_external_id, last_name, first_name, practice_id)

            if hits['hits']['total']['value'] == 1:
                #hint  = hits['hits']['hits'][0]['_source']
                data = [i, pet_id, pet_external_id, practice_id, pet_name,
                        first_name, last_name, zipcode, state, city, 'case2',address, address1, address2, address3, address4,customer_external_id,
                        hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                a.append(data)
                count_found += 1
                #print('case2')

            else:
                # case3 where the client name is not present so only ising the pet external id
                hits = case3(pet_external_id, practice_id)
            
                if hits['hits']['total']['value'] == 1:
                    
                    #hint  = hits['hits']['hits'][0]['_source']
                    data = [i, pet_id, pet_external_id, practice_id, pet_name,
                            first_name, last_name, zipcode, state, city, 'case3',address, address1, address2, address3, address4,customer_external_id,
                            hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                    a.append(data)
                    count_found += 1
                    #print('case3')

                else:
                    # case4
                    hits = case4(pet_external_id, practice_id)
                    
                    if hits['hits']['total']['value'] == 1:
                        
                        #hint  = hits['hits']['hits'][0]['_source']
                        data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                first_name, last_name, zipcode, state, city, 'case4',address, address1, address2, address3, address4,customer_external_id,
                                hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                        a.append(data)
                        count_found += 1
                        #print('case4')

                    else:
                        # case 5
                        hits = case5(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                        pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id

                        if hits['hits']['total']['value'] == 1:
                            
                            #print(i, 'CASE 5 found: ')
                            #print('pet_external_id, pet_id, customer_external_id, first_name, last_name, zipcode, state, pet_name')
                            #print(pet_external_id, pet_id, customer_external_id, first_name, last_name, zipcode, state, pet_name)
                            
                            #hint = hits['hits']['hits'][0]['_source']
                            data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                    first_name, last_name, zipcode, state, city, 'case5',address, address1, address2, address3, address4,customer_external_id,
                                    hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                            a.append(data)
                            count_found += 1
                            #print('case5')

                        else:
                            # case6
                            hits = case6(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                    
                            if hits['hits']['total']['value'] == 1:
                        
                                #hint  = hits['hits']['hits'][0]['_source']
                                data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                        first_name, last_name, zipcode, state, city, 'case6',address, address1, address2, address3, address4,customer_external_id,
                                        hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                                a.append(data)
                                count_found += 1
                                #print('case6')

                            else:
                                # case7
                                hits = case7(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                    
                                if hits['hits']['total']['value'] == 1:
                        
                                    #hint  = hits['hits']['hits'][0]['_source']
                                    data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                            first_name, last_name, zipcode, state, city, 'case7',address, address1, address2, address3, address4, customer_external_id,
                                            hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                                    a.append(data)
                                    count_found += 1
                                    #print('case7')
                                    
                                else:
                                    # case8
                                    hits = case8(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                                    if hits['hits']['total']['value'] == 1:
                        
                                        #hint  = hits['hits']['hits'][0]['_source']
                                        data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                                first_name, last_name, zipcode, state, city, 'case8',address, address1, address2, address3, address4, customer_external_id,
                                                hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                        
                                        a.append(data)
                                        count_found += 1
                                        #print('case8')

                                    else:
                                        # case9
                                        hits = case9(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)
                    
                                        if hits['hits']['total']['value'] >= 1 and hits['hits']['max_score'] > 3:
                        
                                            #hint  = hits['hits']['hits'][0]['_source']
                                            data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                                        first_name, last_name, zipcode, state, city, 'case9',address, address1, address2, address3, address4,customer_external_id,
                                                        hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                        
                                            a.append(data)
                                            count_found += 1
                                            #print('case9')

                                        else:
                                            hits = case10(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)
                                            if hits['hits']['total']['value'] >= 1 and hits['hits']['max_score'] > 3:
                
                                                #hint  = hits['hits']['hits'][0]['_source']
                                                data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                                        first_name, last_name, zipcode, state, city, 'case10',address, address1, address2, address3, address4,customer_external_id,
                                                        hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                            
                                                a.append(data)
                                                count_found += 1
                                                #print('case10')

                                            else:
                                                hits = case11(pet_external_id, customer_external_id, first_name, last_name, zipcode, state, pet_name, practice_id)
                                                if hits['hits']['total']['value'] >= 1 :
                                                    #hint  = hits['hits']['hits'][0]['_source']
                                                    data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                                            first_name, last_name, zipcode, state, city, 'case11',address, address1, address2, address3, address4, customer_external_id,
                                                            hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                                    a.append(data)
                                                    count_found += 1
                                                    #print('case11')

                                                else:
                                                    hits = case12(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)

                                                    if hits['hits']['total']['value'] >= 1 and hits['hits']['max_score'] > 3:
                
                                                        #hint  = hits['hits']['hits'][0]['_source']
                                                        data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                                                first_name, last_name, zipcode, state, city, 'case12',address, address1, address2, address3, address4, customer_external_id,
                                                                hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                                        a.append(data)
                                                        count_found += 1
                                                        #print('case12')

                                                    else:
                                                        hits = case13(first_name, last_name, zipcode, pet_name, practice_id, address, address1, address2, address3, address4, state, city)

                                                        if hits['hits']['total']['value'] >= 1 and hits['hits']['max_score'] > 3:
                
                                                            #hint  = hits['hits']['hits'][0]['_source']
                                                            data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                                                    first_name, last_name, zipcode, state, city, 'case13',address, address1, address2, address3, address4, customer_external_id,
                                                                    hits['hits']['total']['value'], hits['hits']['hits'], hits['hits']['hits'][0]['_source']]
                                                            a.append(data)
                                                            count_found += 1
                                                            #print('case13')

                                                        else:
                                                            print(i, 'not found', pet_external_id, last_name, first_name, practice_id, pet_name, pet_id, address, address1, address2, address3, address4)
                                                            data = [i, pet_id, pet_external_id, practice_id, pet_name,
                                                                    first_name, last_name, zipcode, state, city, 'not found', address, address1, address2, address3, address4, customer_external_id,
                                                                    '', '', '']
                                                            a.append(data)
                                                            count_notfount += 1


        if count_total % 20000 == 0:
            print('*****************************storing**************************\n\n\n\n\n\n')
            df_clients_new_found = pd.DataFrame(a, columns = ['count_i', 'pet_id', 'pet_external_id', 'practice_id', 'pet_name',
                                                    'first_name', 'last_name', 'zipcode', 'state', 'city', 'strategy', 'address', 'address1', 'address2', 'address3', 'address4', 'customer_external_id',
                                                    'total_hits', 'all_hits', 'first_hit'])
                        
            timestr = time.strftime("%Y%m%d_%H%M%S")
            df_clients_new_found.to_csv("patients_matches/df_patients_case2_found_round2_{}_{}.csv".format( k, timestr))
            k+=1

            print("we stopped at: ",i)
            print(f'total count of search is: {count_total}')
            print(f'total not found is:       {count_notfount}')
            print(f'total found cases are:    {count_found}')
            print(f'total ERROR cases are:    {count_error}')
            print('\n\n\n')
            
    except:
        print('viola.. this didnt work .. ERROR HAPPENED .....' )
        print(i, 'ERROR HAPPENED', pet_external_id, last_name, first_name, practice_id, pet_name, pet_id, address, '\n\n')
        didnt_work_cases.append(row)#([i, 'Error happened', pet_external_id, last_name, first_name, practice_id, pet_name, pet_id, customer_external_id, address, zipcode, state, city])
        count_error += 1

print("we stopped at: ",i)
print(f'total count of search is {count_total}')
print(f'total not found is: {count_notfount}')
print(f'total found cases are {count_found}')
print(f'total ERROR cases are {count_error}')

df_patient_new_found = pd.DataFrame(a, columns = ['count_i', 'pet_id', 'pet_external_id', 'practice_id', 'pet_name',
                                                 'first_name', 'last_name', 'zipcode', 'state', 'city', 'strategy', 'address', 'address1', 'address2', 'address3', 'address4', 'customer_external_id',
                                                 'total_hits', 'all_hits', 'first_hit'])

df_patient_didnt_work = pd.DataFrame(didnt_work_cases)

df_patient_new_found.to_csv("patients_matches/df_patient_new_found_all_{}.csv".format(time.strftime("%Y%m%d_%H%M%S")))
df_patient_didnt_work.to_csv("patients_matches/df_patient_error_all_{}.csv".format(time.strftime("%Y%m%d_%H%M%S")))



