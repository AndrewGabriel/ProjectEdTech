#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 22:27:14 2019

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
import copy


def create_features(data_dict,student_id):
    # create the features/targets for each session value
    # student response times
    
    keys = data_dict['ordered_keys']
    history = {} # any lagging variables for computation from prior sessions for that
                # specific student
    
    # TODO: Have to develop a methodology of transfer learning
    # to learn from prior user behavior and other user data in 
    # training sets as qualifiers in a neural network architecture
    # Kind of like a hybrid autoencoder model of LSTM and situational data
    
    features,targets = {},{}
    
    for key in keys:
        session_df  = data_dict['SessionData'][str(key)]
        # problem times
        col = 'Problem Start Time'
        problems = [list(x) for x in session_df.groupby('Problem Name')]
        
        for problem,problem_df in problems:
            col = 'Problem Start Time'
            start_time = problem_df[col].max()
            unique_key = str(student_id) + '___' + problem + '___' + str(start_time)
            features[unique_key] = {}
            
            features[unique_key]['CompletionTime'] = problem_df['Time'].max()-problem_df[col].max()
            col = 'Student Response Type'
            attempts = (problem_df['Student Response Type'] == 'ATTEMPT').sum()
            features[unique_key]['NumAttempts'] = attempts
            features[unique_key]['NumHints'] = len(problem_df) - attempts
            
            # numerous time features
            time_series = (problem_df['Time'].shift(1) - problem_df['Time'].shift(0)).dropna()
            if len(time_series):
                time_series_stats = time_series.describe()
                for ts_key in time_series_stats.index:
                    features[unique_key]['TIME_' + ts_key] = time_series_stats[ts_key]
            
                # add in log differences
                time_series = time_series.map(lambda x: numpy.log(1.+x))
                time_series_stats = time_series.describe()
                for ts_key in time_series_stats.index:
                    features[unique_key]['TIME_' + ts_key + '_LOG'] = time_series_stats[ts_key]
            
            features[unique_key]['PROBLEM'] = problem
            features[unique_key]['AREA'] = problem_df['KC (Area)'].ix[0]
            features[unique_key]['TOPIC'] = problem_df['KC (Topic)'].ix[0]
            features[unique_key]['STUDENT_ID'] = student_id
            
            targets[unique_key] = problem_df['CF (earned_proficiency)'].max() # 1 or 0
            
            # TODO: Do problems need to be involved across sessions or do they start
            # from scratch for every session?
    pandas.Series(targets).to_csv("./targets.csv")
    return features,targets


def compute_aggregate_features(features):
    agg_features = {} # map problem type to aggregate features
    
    data = {}
    for key in features:
        for session_key in features[key]:
            data[session_key] = features[key][session_key]
            
    data = copy.deepcopy(data)
    features_df = pandas.DataFrame(data).transpose()
    print(features_df)
    features_df.to_csv("./intermediate_features.csv")
    
    # do the group bys for Problem Topic 
    
    problems = [list(x) for x in features_df.groupby('PROBLEM')]
    for problem,df in problems:
        
        time_cols = [x for x in df.columns if x.startswith('TIME')]
        agg_features[problem] = {}
        for time_col in time_cols:
            col = df[time_col]
            stats = col.describe()
            for index in stats.index:
                agg_features[problem][time_col + '_' + index] = stats[index]
        
        #agg_features[problem]['PercentCompleted'] = 1. * df['CF (earned_proficiency)'].sum() / len(df)
        
    topics = [list(x) for x in features_df.groupby('TOPIC')]
    for topic,df in topics:
        agg_features[topic] = {}
        time_cols = [x for x in df.columns if x.startswith('TIME')]
        
        for time_col in time_cols:
            col = df[time_col]
            stats = col.describe()
            for index in stats.index:
                agg_features[problem][time_col + '_' + index] = stats[index]
        
        #agg_features[topic]['PercentCompleted'] = 1. * df['CF (earned_proficiency)'].sum() / len(df)
        #agg_features[topic]['NumProblemsAttempted'] = len(df)
        
    return agg_features
        
    
    
def individual_comparisions():
    # TOOD: Compare each student in the training set to the student's performance
    # across the factors of the aggregate features, ranking statistics
    # TODO: have to store this in an array for testing - Paper does not mention
    # these steps
    pass
        



def read_in_data():
    in_dir = '../DATA/SessionGrouped/'
    data    = {}
    infiles     = os.listdir(in_dir)
    for filename in infiles:
        key = filename.split('.')[0]
        data[key] = json.load(open(in_dir+filename))
        for session_id in data[key]['SessionData']:
            data[key]['SessionData'][session_id] = pandas.DataFrame(data[key]['SessionData'][session_id])
    return data