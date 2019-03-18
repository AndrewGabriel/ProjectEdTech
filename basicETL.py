#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 16 11:16:37 2019

@author: andrewgabriel
"""


import os
import sys
import json
import pandas
import numpy
import random
import collections
import itertools


def get_random_students(student_value_counts,percentiles,num_students=25):
    # Percentiles has already been filtered so only need to query between rows
    # value counts are the value counts
    
    per_index   = percentiles.index.tolist()
    
    data_dict   = {} # maps two percentile strings to the data
    
    
    
    for i in range(1,len(per_index)):
        start,end   = percentiles[per_index[i-1]] , percentiles[per_index[i]]
        
        if start==end:
            continue 
        
        vc = student_value_counts.copy()
        vc = vc[vc >= start]
        vc = vc[vc < end]
        
        students = vc.index.copy().tolist()
        
        if len(students) > num_students:
            random.shuffle(students)
            students = students[:num_students]
            
        if len(students)==0:
            continue
            
            
        key = '___'.join([per_index[i-1],per_index[i]])
        data_dict[key] = students
        print(key)
        
    return data_dict
        

def get_student_data(student_dict,infile):
    
    all_students = itertools.chain.from_iterable(student_dict.values())
    all_students = list(all_students)
    all_students_set = set(all_students)
    print(students)
    student_row_dict    = collections.defaultdict(list)
    
    sep = '\t'
    chunksize = 10**5
    counter = 0
    
    for chunk in pandas.read_csv(infile,sep=sep,chunksize=chunksize):
    
        data = chunk.copy()
        data = data[list(map(lambda x: x in all_students_set,data['Anon Student Id'].tolist()))]
        print(counter)
        counter+=1
        
        for student in all_students:
            filtered_data = data[data['Anon Student Id'] == student].copy()
            if len(filtered_data)==0:
                continue
        
            filtered_data = filtered_data.drop('Anon Student Id',axis=1)
            filtered_data.index = [x for x in range(len(filtered_data))]
            filtered_data = filtered_data.transpose().to_json()
            filtered_data = json.loads(filtered_data)
            
            # add all the rows
            for i in range(len(filtered_data)):
                student_row_dict[student].append(filtered_data[str(i)])
                
    return student_row_dict
            
        
        
def write_out_features(student_row_dict):
    out_dir = '../DATA/RAW_DATA/'
    
    for key in student_row_dict:
        outfilename = out_dir + str(key) + '.json'
        with open(outfilename,'w') as file_handle:
            json.dump(student_row_dict[key],file_handle)
            
            
def etl_session_id(in_dir= '../DATA/RAW_DATA/',out_dir='../DATA/SessionGrouped/'):
    try:
        os.mkdir(out_dir)
    except:
        pass
    
    # for each student, group by session id
    
    infiles = os.listdir(in_dir)
    
    for filename in infiles:
        
        data = json.load(open(in_dir+filename))
        data = pandas.DataFrame(data)
        
        num_sessions = len(data['Session Id'].value_counts())
        session_data = data['Session Id'].value_counts().index.tolist()
       
        if num_sessions==0:
            continue # Bad Data
            
        out_data = {}
        out_data['ordered_keys'] = []
        out_data['SessionData'] = {}
        
        if num_sessions == 1:
            out_data['SessionData'][session_data[0]] = json.loads(data.to_json())
       
        else:
            
            grouped_sessions    = [list(x) for x in data.groupby('Session Id')]
            session_start_times = {x[0]:x[1]['Time'].min() for x in grouped_sessions}
            session_start_times = pandas.Series(session_start_times).sort_values()

            out_data['ordered_keys'] = session_start_times.index.tolist()
            
            for session_value in session_data:
                sub_data = data[data['Session Id'] == session_value]
                out_data['SessionData'][session_value] = json.loads(sub_data.to_json())
        
        
        outfilename = out_dir + filename
        with open(outfilename,'w') as file_handle:
            json.dump(out_data,file_handle)
        
        
        
    
            
    
    
        
    
        