# -*- coding: utf-8 -*-
"""
Created on Mon Apr 23 10:25:47 2018

@author: rthomas
"""
# Import toolboxes used by the script and its functions
import os
import numpy as np
import pandas as pd
from urllib.request import urlopen
import json
import datetime

# Set list to capture summary information for reporting at then end of the run
summary = []

# Set time run started
start = datetime.datetime.now()

# Add start time and holding space for end time to summary information
summary.append(["Processing started:" , (start.strftime('%Y-%m-%d %H:%M:%S'))]) 
summary.append(["Processing finished:" , ""])
summary.append(["" , ""])

# Add filepaths for input file here
inputfile = os.path.normpath('C:/Users/rthomas/Documents/GitHub/ICES2NVS_semantic_map/chem_mapping/MI_biota_aphia_added.xlsx')

# Add filepaths for mapping files here
mapfile = os.path.normpath('C:/Users/rthomas/Documents/GitHub/ICES2NVS_semantic_map/chem_mapping/mappings/unmapped_substances.xlsx')
biotamap = os.path.normpath('C:/Users/rthomas/Documents/GitHub/ICES2NVS_semantic_map/chem_mapping/mappings/biota_synonym_mapping.xlsx')
p02_file = os.path.normpath('C:/Users/rthomas/Documents/GitHub/ICES2NVS_semantic_map/mappings/ICES2P02_mapping.txt')

# Generate output file name
fileout = os.path.normpath(inputfile[:-5]+'_mapped.xlsx')

#%% Get the latest semantic model vocabulary contents from the NVS Sparql endpoint

a1 = "http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A++++PREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A++++%0D%0A++++select+%3F"
a2 = "+%3F"
a3 = "%0D%0A++++where+%7B%0D%0A++++%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2F"
a4 = "%2Fcurrent%2F%3E+skos%3Amember+%3Furl+.%0D%0A++++%3Furl+skos%3AprefLabel+%3F"
a5 = "+.%0D%0A++++%3Furl+skos%3Anotation+%3Fc+.%0D%0A++++%3Furl+owl%3Adeprecated+%27false%27+.%0D%0A++++BIND%28replace%28str%28%3Fc%29%2C%27SDN%3A"
a6 = "%3A%3A%27%2C%27%27%2C%27i%27%29+AS+%3F"
a7 = "%29%0D%0A++++%7D&output=csv&stylesheet="

S06 = pd.read_csv(a1+'S06'+a2+'S06_label'+a3+'S06'+a4+'S06_label'+a5+'S06'+a6+'S06'+a7)
S07 = pd.read_csv(a1+'S07'+a2+'S07_label'+a3+'S07'+a4+'S07_label'+a5+'S07'+a6+'S07'+a7)
S02 = pd.read_csv(a1+'S02'+a2+'S02_label'+a3+'S02'+a4+'S02_label'+a5+'S02'+a6+'S02'+a7)
S26 = pd.read_csv(a1+'S26'+a2+'S26_label'+a3+'S26'+a4+'S26_label'+a5+'S26'+a6+'S26'+a7)
P01 = pd.read_csv(a1+'P01'+a2+'P01_label'+a3+'P01'+a4+'P01_label'+a5+'P01'+a6+'P01'+a7)

# Build P01 semantic model
#driver = {'S06': 'skos:narrower',
#          'S07': 'skos:narrower',
#          'S27': 'skos:narrower',
#          'S02': 'skos:related',
#          'S26': 'skos:narrower',
#          'S25': 'skos:narrower'}
#vocab='S27'
#relation='skos:narrower'

#query = """PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
#PREFIX owl: <http://www.w3.org/2002/07/owl#>
    
#select distinct ?%s ?P01
#where {
#<http://vocab.nerc.ac.uk/collection/%s/current/> skos:member ?urla .
#?urla owl:deprecated 'false' .
#?urla skos:notation ?n2 .
#?urla %s ?urlb .
#<http://vocab.nerc.ac.uk/collection/P01/current/> skos:member ?urlb .
#?urlb owl:deprecated 'false' .
#?urlb skos:notation ?n1 .
#BIND(replace(?n1, "SDN:P01::", "", "i") AS ?P01) .
#BIND(replace(?n2, "SDN:%s::", "", "i") AS ?%s) .
#}
#""" % (vocab, vocab, relation, vocab, vocab)
#print query

# Download semantic component mapping

urlS06 = "http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A%0D%0Aselect+distinct+%3FS06+%3FP01%0D%0Awhere+%7B%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS06%2Fcurrent%2F%3E+skos%3Amember+%3Furla+.%0D%0A%3Furla+owl%3Adeprecated+%27false%27+.%0D%0A%3Furla+skos%3Anotation+%3Fn2+.%0D%0A%3Furla+skos%3Anarrower+%3Furlb+.%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FP01%2Fcurrent%2F%3E+skos%3Amember+%3Furlb+.%0D%0A%3Furlb+owl%3Adeprecated+%27false%27+.%0D%0A%3Furlb+skos%3Anotation+%3Fn1+.%0D%0ABIND%28replace%28%3Fn1%2C+%22SDN%3AP01%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FP01%29+.%0D%0ABIND%28replace%28%3Fn2%2C+%22SDN%3AS06%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FS06%29+.%0D%0A%7D&output=csv&stylesheet="
S06_P01 = pd.read_csv(urlS06)
print("S06 mapping downloaded.")

urlS07 = "http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A%0D%0Aselect+distinct+%3FS07+%3FP01%0D%0Awhere+%7B%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS07%2Fcurrent%2F%3E+skos%3Amember+%3Furla+.%0D%0A%3Furla+owl%3Adeprecated+%27false%27+.%0D%0A%3Furla+skos%3Anotation+%3Fn2+.%0D%0A%3Furla+skos%3Anarrower+%3Furlb+.%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FP01%2Fcurrent%2F%3E+skos%3Amember+%3Furlb+.%0D%0A%3Furlb+owl%3Adeprecated+%27false%27+.%0D%0A%3Furlb+skos%3Anotation+%3Fn1+.%0D%0ABIND%28replace%28%3Fn1%2C+%22SDN%3AP01%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FP01%29+.%0D%0ABIND%28replace%28%3Fn2%2C+%22SDN%3AS07%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FS07%29+.%0D%0A%7D&output=csv&stylesheet="
S07_P01 = pd.read_csv(urlS07)
print("S07 mapping downloaded.")

urlS27 = "http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A%0D%0Aselect+distinct+%3FS27+%3FP01%0D%0Awhere+%7B%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS27%2Fcurrent%2F%3E+skos%3Amember+%3Furla+.%0D%0A%3Furla+owl%3Adeprecated+%27false%27+.%0D%0A%3Furla+skos%3Anotation+%3Fn2+.%0D%0A%3Furla+skos%3Anarrower+%3Furlb+.%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FP01%2Fcurrent%2F%3E+skos%3Amember+%3Furlb+.%0D%0A%3Furlb+owl%3Adeprecated+%27false%27+.%0D%0A%3Furlb+skos%3Anotation+%3Fn1+.%0D%0ABIND%28replace%28%3Fn1%2C+%22SDN%3AP01%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FP01%29+.%0D%0ABIND%28replace%28%3Fn2%2C+%22SDN%3AS27%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FS27%29+.%0D%0A%7D&output=csv&stylesheet="
S27_P01 = pd.read_csv(urlS27)
print("S27 mapping downloaded.")

urlS02 = "http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A%0D%0Aselect+distinct+%3FS02+%3FP01%0D%0Awhere+%7B%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS02%2Fcurrent%2F%3E+skos%3Amember+%3Furla+.%0D%0A%3Furla+owl%3Adeprecated+%27false%27+.%0D%0A%3Furla+skos%3Anotation+%3Fn2+.%0D%0A%3Furla+skos%3Arelated+%3Furlb+.%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FP01%2Fcurrent%2F%3E+skos%3Amember+%3Furlb+.%0D%0A%3Furlb+owl%3Adeprecated+%27false%27+.%0D%0A%3Furlb+skos%3Anotation+%3Fn1+.%0D%0ABIND%28replace%28%3Fn1%2C+%22SDN%3AP01%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FP01%29+.%0D%0ABIND%28replace%28%3Fn2%2C+%22SDN%3AS02%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FS02%29+.%0D%0A%7D&output=csv&stylesheet="
S02_P01 = pd.read_csv(urlS02)
print("S02 mapping downloaded.")

urlS26 = "http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A%0D%0Aselect+distinct+%3FS26+%3FP01%0D%0Awhere+%7B%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS26%2Fcurrent%2F%3E+skos%3Amember+%3Furla+.%0D%0A%3Furla+owl%3Adeprecated+%27false%27+.%0D%0A%3Furla+skos%3Anotation+%3Fn2+.%0D%0A%3Furla+skos%3Anarrower+%3Furlb+.%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FP01%2Fcurrent%2F%3E+skos%3Amember+%3Furlb+.%0D%0A%3Furlb+owl%3Adeprecated+%27false%27+.%0D%0A%3Furlb+skos%3Anotation+%3Fn1+.%0D%0ABIND%28replace%28%3Fn1%2C+%22SDN%3AP01%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FP01%29+.%0D%0ABIND%28replace%28%3Fn2%2C+%22SDN%3AS26%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FS26%29+.%0D%0A%7D&output=csv&stylesheet="
S26_P01 = pd.read_csv(urlS26)
print("S26 mapping downloaded.")

urlS25 = "http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A%0D%0Aselect+distinct+%3FS25+%3FP01%0D%0Awhere+%7B%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS25%2Fcurrent%2F%3E+skos%3Amember+%3Furla+.%0D%0A%3Furla+owl%3Adeprecated+%27false%27+.%0D%0A%3Furla+skos%3Anotation+%3Fn2+.%0D%0A%3Furla+skos%3Anarrower+%3Furlb+.%0D%0A%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FP01%2Fcurrent%2F%3E+skos%3Amember+%3Furlb+.%0D%0A%3Furlb+owl%3Adeprecated+%27false%27+.%0D%0A%3Furlb+skos%3Anotation+%3Fn1+.%0D%0ABIND%28replace%28%3Fn1%2C+%22SDN%3AP01%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FP01%29+.%0D%0ABIND%28replace%28%3Fn2%2C+%22SDN%3AS25%3A%3A%22%2C+%22%22%2C+%22i%22%29+AS+%3FS25%29+.%0D%0A%7D&output=csv&stylesheet="
S25_P01 = pd.read_csv(urlS25)
print("S25 mapping downloaded.")

# Build P01 semantic model dataframe
P01 = pd.merge(P01, S06_P01, how='left', on='P01')
P01 = pd.merge(P01, S07_P01, how='left', on='P01')
P01 = pd.merge(P01, S27_P01, how='left', on='P01')
P01 = pd.merge(P01, S02_P01, how='left', on='P01')
P01 = pd.merge(P01, S26_P01, how='left', on='P01')
P01 = pd.merge(P01, S25_P01, how='left', on='P01')

P01 = P01.fillna(value={'S25': 'BE007736', 'S07': 'S0700006'})

#%% Load ICES semantic model components for mapping to P01 semantic model from file into a Pandas DataFrame
inputs = pd.read_excel(inputfile)

# Insert number of rows in the input file in to the summary information
print("Rows input: %s" % len(inputs))
summary.append(["Rows input:", len(inputs)])

# Make a working copy of the parameter combinations for mapping and remove duplicate combinations
param_combo = inputs.copy(deep=True).drop_duplicates(keep=False)
input_duplicates = len(inputs) - len(param_combo)
print("Rows duplicated: %s" % input_duplicates)
summary.append(["Rows duplicated:", input_duplicates])
summary.append(["" , ""])
print("Rows for mapping: %s" % len(param_combo))
summary.append(["Rows for mapping:", len(param_combo)])

# In the working copy set NaNs to '-9' and add columns for mapped NVS semantic model elements
param_combo = param_combo.fillna('-9')
param_combo = param_combo.assign(S06_label='',              # Measurement Property
                                 S07_label='not specified', # Measurement Property Statistic
                                 S02_label='',              # Measurement - Matrix relationship
                                 )
param_combo['PARAM'] = param_combo['PARAM'].str.upper()

# Remove leading or trailing spaces from the text columns
columns = param_combo.columns.tolist()
columns.remove('AphiaID')
for column in columns:
    param_combo[column] = param_combo[column].str.strip()
    
#%% Check mapping of chemical PARAMs and NVS S27 vocabulary entries and add S27 to the new parameters dataframe

print("Mapping of chemical substances...")
summary.append(["Mapping of chemical substances...", ""])

# First get S27 terms that have a mapping to ICES PARAM vocabulary published from the NVS
q = """PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
                    
    select ?PARAM ?S27 ?S27_label 
    where {
           <http://vocab.nerc.ac.uk/collection/S27/current/> skos:member ?url .
           ?url skos:notation ?a .
           ?url skos:prefLabel ?S27_label .
           ?url owl:deprecated 'false' .
           ?url skos:related ?c .
           FILTER(regex(str(?c), "http://vocab.ices.dk/services/rdf/collection/PARAM/", "i")) .
           BIND(substr(?a,10,8) as ?S27) .
           BIND(replace(str(?c), "http://vocab.ices.dk/services/rdf/collection/PARAM/", "", "i") AS ?PARAM) .
          }"""
# URL for the above query is:
url = """http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A++++PREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A++++++++++++++++++++%0D%0A++++select+%3FPARAM+%3FS27+%3FS27_label+%0D%0A++++where+%7B%0D%0A+++++++++++%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS27%2Fcurrent%2F%3E+skos%3Amember+%3Furl+.%0D%0A+++++++++++%3Furl+skos%3Anotation+%3Fa+.%0D%0A+++++++++++%3Furl+skos%3AprefLabel+%3FS27_label+.%0D%0A+++++++++++%3Furl+owl%3Adeprecated+%27false%27+.%0D%0A+++++++++++%3Furl+skos%3Arelated+%3Fc+.%0D%0A+++++++++++FILTER%28regex%28str%28%3Fc%29%2C+%22http%3A%2F%2Fvocab.ices.dk%2Fservices%2Frdf%2Fcollection%2FPARAM%2F%22%2C+%22i%22%29%29+.%0D%0A+++++++++++BIND%28substr%28%3Fa%2C10%2C8%29+as+%3FS27%29+.%0D%0A+++++++++++BIND%28replace%28str%28%3Fc%29%2C+%22http%3A%2F%2Fvocab.ices.dk%2Fservices%2Frdf%2Fcollection%2FPARAM%2F%22%2C+%22%22%2C+%22i%22%29+AS+%3FPARAM%29+.%0D%0A++++++++++%7D&output=CSV&stylesheet="""

# More efficient to ingest SPARQL response as a CSV directly into a Pandas DataFrame
mapped_chems = pd.read_csv(url)
mapped_chems['SOURCE'] = 'NVS'

# Get local ICES PARAM to NVS S27 substance mapping from mapping file location provided earlier
local_map = pd.read_excel(mapfile)
local_map['SOURCE'] = mapfile

# Combine NVS mappings with those from the local file to generate a complete list of known mappings
full_chem_map = pd.concat([mapped_chems, local_map[['PARAM','S27','S27_label','SOURCE']]]).drop_duplicates(subset='PARAM').reset_index(drop=True)

# Identify any mappings in the local file already published from the NVS
duplicate_map = pd.merge(mapped_chems, local_map, how='inner', on='PARAM')


print("Number of PARAM to S27 chemical substance mappings from NVS: %s" % (len(mapped_chems)))
summary.append(["Number of PARAM to S27 chemical substance mappings from NVS:", (len(mapped_chems))])

print("Number of local chemical substance mappings from file: %s" % (len(local_map)))
summary.append(["Number of local chemical substance mappings from file:" , (len(local_map))])

print("Number of chemical substance mappings in local file and NVS: %s" % (len(duplicate_map)))
summary.append(["Number of chemical substance mappings in local file and NVS:" , (len(duplicate_map))])

# Determine if any new PARAM to S27 mappings are required
# Get subset of chemical PARAMS in the input list
input_PARAMS = param_combo[['PARAM','PRNAM','CAS']][param_combo['CAS']!='-9'].drop_duplicates().reset_index(drop=True)

print("Number of chemical substances for P01 mapping: %s" % (len(input_PARAMS)))
summary.append(["Number of PARAM chemical substances for P01 mapping:" , (len(input_PARAMS))])

# Merge information to determine which mappings are missing from the NVS
compare = pd.merge(input_PARAMS, full_chem_map, how='outer', on='PARAM')
S27_missing = compare[['PARAM','CAS']][compare['S27'].isnull()].copy(deep=True)

print("Number of PARAM chemical substances in file not mapped: %s" % (len(S27_missing)))
summary.append(["Number of PARAM chemical substances in file unable to be mapped:" , (len(S27_missing))])

# For PARAMs with CAS numbers check if the chemcial substance exists within S27 and only requires a mapping to PARAM
# SPARQL query for all NVS substances with CAS numbers from the SPARQL endpoint
q =  """PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
            
    select ?nvs_codval ?nvs_prefLabel ?nvs_casrn
    where {
           <http://vocab.nerc.ac.uk/collection/S27/current/> skos:member ?url .
           ?url skos:notation ?a .
           ?url skos:prefLabel ?nvs_prefLabel .
           ?url owl:deprecated 'false' .
           ?url owl:sameAs ?c .
           FILTER(regex(str(?c), "http://chem.sis.nlm.nih.gov/chemidplus/rn/", "i")) .
           BIND(replace(str(?a),'SDN:S27::','','i') AS ?nvs_codval) .
           BIND(replace(str(?c),'http://chem.sis.nlm.nih.gov/chemidplus/rn/','','i') AS ?nvs_casrn) .
          }"""                

# URL for the above query is:
url = """http://vocab.nerc.ac.uk/sparql/sparql?query=++++PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A++++PREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A++++++++++++%0D%0A++++select+%3Fnvs_codval+%3Fnvs_prefLabel+%3Fnvs_casrn%0D%0A++++where+%7B%0D%0A+++++++++++%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS27%2Fcurrent%2F%3E+skos%3Amember+%3Furl+.%0D%0A+++++++++++%3Furl+skos%3Anotation+%3Fa+.%0D%0A+++++++++++%3Furl+skos%3AprefLabel+%3Fnvs_prefLabel+.%0D%0A+++++++++++%3Furl+owl%3Adeprecated+%27false%27+.%0D%0A+++++++++++%3Furl+owl%3AsameAs+%3Fc+.%0D%0A+++++++++++FILTER%28regex%28str%28%3Fc%29%2C+%22http%3A%2F%2Fchem.sis.nlm.nih.gov%2Fchemidplus%2Frn%2F%22%2C+%22i%22%29%29+.%0D%0A+++++++++++BIND%28replace%28str%28%3Fa%29%2C%27SDN%3AS27%3A%3A%27%2C%27%27%2C%27i%27%29+AS+%3Fnvs_codval%29+.%0D%0A+++++++++++BIND%28replace%28str%28%3Fc%29%2C%27http%3A%2F%2Fchem.sis.nlm.nih.gov%2Fchemidplus%2Frn%2F%27%2C%27%27%2C%27i%27%29+AS+%3Fnvs_casrn%29+.%0D%0A++++++++++%7D&output=csv&stylesheet="""    

# More efficient to ingest SPARQL response as a CSV directly into a Pandas DataFrame
nvs_cas = pd.read_csv(url)

# Merge information between unmapped PARAMs and NVS S27 based on CAS number
S27_casmap_review = pd.merge(S27_missing, nvs_cas, how='inner', left_on='CAS', right_on='nvs_casrn')

print("Number of potential PARAM-S27 mappings for review based on CAS linkage: %s for %s PARAMs" % (len(S27_casmap_review), len(S27_casmap_review['PARAM'].drop_duplicates())))
summary.append(["    Number of potential PARAM-S27 mappings for review based on CAS linkage:", "%s for %s PARAMs" % (len(S27_casmap_review), len(S27_casmap_review['PARAM'].drop_duplicates()))])

print("Number of PARAMs with no S27 term identified using CAS: %s" % (len(pd.concat([S27_missing['PARAM'], S27_casmap_review['PARAM']]).drop_duplicates(keep=False))))
summary.append(["    Number of PARAMs with no S27 term identified using CAS:" , "%s" % len(pd.concat([S27_missing['PARAM'], S27_casmap_review['PARAM']]).drop_duplicates(keep=False))])

# Add holding text for PARAM to S27 mappings from CAS for review
if len(S27_casmap_review)>0:
    warn = pd.DataFrame()
    warn['PARAM'] = S27_casmap_review['PARAM'].drop_duplicates().copy(deep=True).reset_index(drop=True)
    warn['S27'] = 'Potential S27 term exists.'
    warn['S27_label'] = 'Mapping needs to be reviewed and added to NVS or unmapped_substance.xlsx.'
    warn['SOURCE'] = 'CAS linkage exists.'
    
    full_chem_map = pd.concat([full_chem_map, warn])
    
# Add holding text for PARAM where no potential S27 exists through CAS
if len(S27_missing)>0:
    warn = pd.DataFrame()
    warn['PARAM'] = pd.concat([S27_missing['PARAM'], S27_casmap_review['PARAM']]).drop_duplicates(keep=False).copy(deep=True).reset_index(drop=True)
    warn['S27'] = 'No S27 term identified from CAS.'
    warn['S27_label'] = 'Potential new term to be added to S27 and mapped to PARAM.'
    warn['SOURCE'] = 'No CAS linkage found.'
    
    full_chem_map = pd.concat([full_chem_map, warn])

# Add S27 mappings to the main table    
param_combo = pd.merge(param_combo, full_chem_map[['PARAM','S27','S27_label']], how='left', on='PARAM')

# Mark rows that do not require mapping as "Not applicable."    
param_combo = param_combo.fillna(value={'S27': 'not applicable', 'S27_label': 'not applicable'})

print("Total combinations = %s" % (len(param_combo)))

PARAMs2map = param_combo[['PARAM','PRNAM']][param_combo['S27']=='not applicable'].drop_duplicates()

print("PARAMs with no mapping to be reviewed: %s" % len(PARAMs2map))

#%% MATRIX check and mapping to S26
print("...")
print("Mapping of matrix...")
summary.append(["", ""])
summary.append(["Mapping of matrix...", ""])

matrix_check = param_combo[['DTYPE','MATRX','METPT']].drop_duplicates().copy(deep=True).reset_index(drop=True)
# Add S25 biological entity semantic model components and set to default of 'not specified'
matrix_check = matrix_check.assign(S26_label = 'Check MATRX. Not mapped.')

print("Number of MATRX for P01 mapping: %s" % len(matrix_check))
summary.append(["Number of MATRX for P01 mapping:" , len(matrix_check)])

for index, row in matrix_check.iterrows():
    if row['DTYPE'] == 'CF':
        # Set S26 label to 'biota'
        row['S26_label'] = 'biota'

    elif row['DTYPE'] == 'CS':
        if row['MATRX'] == 'SEDTOT':
            row['S26_label'] = 'sediment'
        elif row['MATRX'][3:len(row['MATRX'])] != 'TOT':
            row['S26_label'] = 'sediment <'+row['MATRX'][3:len(row['MATRX'])] +'um'
        else:
            row['S26_label'] = 'Check MATRX'
            
    elif row['DTYPE'] == 'CW':
        if row['MATRX'] == 'WT':
            if row['METPT'] == '-9':
                row['S26_label'] = 'water body [dissolved plus reactive particulate <unknown phase]'
            else:
                metpt_list = row['METPT'].split('~')               
                for metpt in metpt_list:
                    if metpt in ('NF','NONE','NA','CP'):
                        row['S26_label'] = 'water body [dissolved plus reactive particulate phase]'
                        continue
                    elif metpt in('GFF','GF/F','FF-GF-0.7'):
                        row['S26_label'] = 'water body [dissolved plus reactive particulate <GF/F phase]'
                        continue
                    elif metpt in('GFC','GF/C','FF-GF-1.2','FF-PP-1.2'):
                        row['S26_label'] = 'water body [dissolved plus reactive particulate <GF/C phase]'
                        continue
                    elif metpt in('FM-PC-0.4','FM-PC-0.45','FM-PES-0.45','FM-CN-0.45','FM-CA-0.45','PCF40','PCF45','PCF'):
                        row['S26_label'] = 'water body [dissolved plus reactive particulate <0.4/0.45um phase]'
                        continue
                    elif metpt in('F'):
                        row['S26_label'] = 'water body [dissolved plus reactive particulate <unknown phase]'
                        continue
                    elif metpt in('FM-CA-0.2'):
                        row['S26_label'] = 'water body [dissolved plus reactive particulate <0.2um phase]'
                        continue
                    else:
                        if row['S26_label'] == '':
                            row['S26_label'] = 'Check METPT'
    print("Row %s of %s matrix combinations mapped." % (index+1,len(matrix_check)))
    
# Subset potential S26 new entries
S26new = matrix_check[matrix_check['S26_label']=='Check MATRX. Not mapped.']

print("Number of potential new S26 terms: %s" % len(S26new))
summary.append(["Number of potential new S26 terms:" , len(S26new)])
                            
# Add S26 semantic model mapping to the main table based on the combination provided   
param_combo = pd.merge(param_combo, matrix_check, how='left', on=['DTYPE','MATRX','METPT'])

print("Total combinations = %s" % (len(param_combo)))

#%% Taxon, WoRMS AphiaID, ITIS TSN combination check
print("...")
print("Mapping of taxa...")
summary.append(["", ""])
summary.append(["Mapping of taxa...", ""])

# Get all existing TAXONs from S25 and simplify text labels to show distinct TAXON values from the S25 semantic model
url = """http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A++++PREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A++++++++++++++++++++%0D%0A++++select+distinct+%3FAphiaID+%3FTAXON%0D%0A++++where+%7B%0D%0A+++++++++++%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS25%2Fcurrent%2F%3E+skos%3Amember+%3Furl+.%0D%0A+++++++++++%3Furl+skos%3AprefLabel+%3FprefLabel+.%0D%0A+++++++++++%3Furl+owl%3Adeprecated+%27false%27+.%0D%0A+++++++++++FILTER%28regex%28str%28%3FprefLabel%29%2C+%22WoRMS%22%2C+%22i%22%29%29+.%0D%0A+++++++++++BIND%28replace%28str%28%3FprefLabel%29%2C+%22%5C%5C+%5C%5C%5B.*%3F%5C%5C%5D%22%2C%22%22%2C+%22i%22%29+AS+%3FTAXON%29+.%0D%0A+++++++++++BIND%28replace%28replace%28replace%28str%28%3FTAXON%29%2C+%22%5C%5C%29%22%2C%22%22%2C+%22i%22%29%2C+%22.*%28%3F%3DWoRMS+%29%22%2C+%22%22%2C+%22i%22%29%2C+%22WoRMS+%22%2C+%22%22%2C+%22i%22%29+AS+%3FAphiaID%29+.%0D%0A++++++++++%7D%0D%0A++++order+by+%3FAphiaID%0D%0A&output=CSV&stylesheet=CSV"""

S25taxon = pd.read_csv(url)

# Identify multiple TAXONs per AphiaID within S25
S25taxon_duplicates = S25taxon[S25taxon.duplicated(['AphiaID'], keep=False)].copy(deep=True)
S25taxon_duplicates.replace(u'\xc2\xa0',u' ', regex=True, inplace=True)
S25taxon_duplicates.replace(u'\u2019',u"'", regex=True, inplace=True)

# Remove duplicate TAXON records from S25taxon dataframe
S25taxon_clean = pd.concat([S25taxon, S25taxon_duplicates]).drop_duplicates(keep=False).copy(deep=True).reset_index(drop=True)

# Create a Pandas DataFrame and populate with unique combinations of Species and AphiaID from the input file
input_taxa_check = pd.DataFrame()
input_taxa_check = param_combo[['Species','AphiaID']][param_combo['Species']!='-9'].drop_duplicates().reset_index(drop=True)
input_taxa_check = input_taxa_check.astype({"AphiaID": int})

print("Number of Species for P01 mapping: %s" % len(input_taxa_check))
summary.append(["Number of Species for P01 mapping:" , len(input_taxa_check)])
#%%
# Function to call WoRMS web service
def worms_check(url):
    request=Request(url)
    response = urlopen(request)
    if response.code==204:
        e = 'No AphiaID found.'
    elif response.code==206:
        e = 'Multiple AphiaID found.'
    elif response.code==200:
        e = response.read()
    return e

# If AphiaID is absent then lookup using the WoRMS web service
for index, row in input_taxa_check.iterrows():
    if row['AphiaID'] == -9:
        if '&' not in row['Species']:
            url = 'http://marinespecies.org/rest/AphiaIDByName/%s?marine_only=true' % row['Species'].replace(" ","%20")
            input_taxa_check.loc[index, 'AphiaID'] = worms_check(url)
        else:
            input_taxa_check.loc[index, 'AphiaID'] = 'Combination of taxa'

inputs_aphia = pd.merge(inputs, input_taxa_check, on='Species')


#%% 
# Get WoRMS scientific names from AphiaID provided using WoRMS web service
def worms_check(url):
    request=Request(url)
    response = urlopen(request)
    if response.code==204:
        list.append('No response.')
    elif response.code==200:
        e = response.read()
    return json.loads(e)
       
worms = pd.DataFrame()        

aphia_list = input_taxa_check['AphiaID'].tolist()
y = len(aphia_list)
if y<50:
    ids = ''
    for i in range(0,50):
        ids = ids + 'aphiaids%5B%5D=' + str(aphia_list[i]) + '&'
    url = 'http://www.marinespecies.org/rest/AphiaRecordsByAphiaIDs?%s' % ids[0:-1]
    worms = pd.DataFrame(worms_check(url), ignore_index=True)
elif y>50:
    for j in range(0,y/50):
        ids = ''
        for i in range(j*50,min((j+1)*50,y)):
            ids = ids + 'aphiaids%5B%5D=' + str(aphia_list[i]) + '&'
        url = 'http://www.marinespecies.org/rest/AphiaRecordsByAphiaIDs?%s' % ids[0:-1]
        worms = pd.concat([worms, pd.DataFrame(worms_check(url))], ignore_index=True)
    ids = ''
    for i in range((j+1)*50,min((j+2)*50,y)):
        ids = ids + 'aphiaids%5B%5D=' + str(aphia_list[i]) + '&'
    url = 'http://www.marinespecies.org/rest/AphiaRecordsByAphiaIDs?%s' % ids[0:-1]
    worms = pd.concat([worms, pd.DataFrame(worms_check(url))], ignore_index=True)

input_taxa_check = pd.merge(input_taxa_check, worms[['AphiaID','scientificname']], how='left', on='AphiaID')

input_taxa_check = input_taxa_check.rename(index=str, columns={'scientificname': 'name_from_AphiaID'})

# Set column to indicate if a discrepancy to be resolved exists based on Scientific names not matching
a = input_taxa_check.Species == input_taxa_check.name_from_AphiaID
input_taxa_check['proceed'] = np.where(a, 'Yes', 'No')

# Subset those taxa where naming discrepancy exists
taxa_discrepancy = input_taxa_check[input_taxa_check['proceed']=='No'].reset_index(drop=True)

print("Number of Species with name discrepancy: %s" % len(taxa_discrepancy))
summary.append(["Number of Species with name discrepancy:" , len(taxa_discrepancy)])

# Map AphiaID to S25 component TAXON for the non-duplicate AphiaID results in S25
taxa_map = pd.merge(input_taxa_check[['AphiaID','name_from_AphiaID']],
                    S25taxon_clean, 
                    how='left', 
                    on='AphiaID')
taxa_map = taxa_map.fillna(value={'TAXON': 'New TAXON required.'}).drop_duplicates()


# Add TAXON mapping to the main table based on the AphiaID provided    
param_combo = pd.merge(param_combo, taxa_map, how='left', on='AphiaID')

# Mark rows that do not require mapping as "Not applicable."    
param_combo = param_combo.fillna(value={'TAXON': 'not specified', 'name_from_AphiaID': '-9'})

print("Total combinations = %s" % (len(param_combo)))

#%% Get all the S25 biological entities from the NVS
print("...")
print("Mapping of biological entities...")
summary.append(["", ""])
summary.append(["Mapping of biological entities...", ""])

# SPARQL query for all NVS substances with CAS numbers from the SPARQL endpoint
q =  """PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
            
    select ?S25 ?S25_label
    where {
           <http://vocab.nerc.ac.uk/collection/S25/current/> skos:member ?url .
           ?url skos:notation ?a .
           ?url skos:prefLabel ?S25_label .
           ?url owl:deprecated 'false' .
           BIND(replace(str(?a),'SDN:S25::','','i') AS ?S25) .
          }"""                

# URL for the above query is:
url = """http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A++++PREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A++++++++++++%0D%0A++++select+%3FS25+%3FS25_label%0D%0A++++where+%7B%0D%0A+++++++++++%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FS25%2Fcurrent%2F%3E+skos%3Amember+%3Furl+.%0D%0A+++++++++++%3Furl+skos%3Anotation+%3Fa+.%0D%0A+++++++++++%3Furl+skos%3AprefLabel+%3FS25_label+.%0D%0A+++++++++++%3Furl+owl%3Adeprecated+%27false%27+.%0D%0A+++++++++++BIND%28replace%28str%28%3Fa%29%2C%27SDN%3AS25%3A%3A%27%2C%27%27%2C%27i%27%29+AS+%3FS25%29+.%0D%0A++++++++++%7D&output=csv&stylesheet="""    

# More efficient to ingest SPARQL response as a CSV directly into a Pandas DataFrame
nvs_be = pd.read_csv(url)

# Biological entity combination check and mapping
be_check = param_combo[['DTYPE','Note','AphiaID','Species','MATRX','name_from_AphiaID','TAXON']][param_combo['DTYPE']=='CF'].drop_duplicates().copy(deep=True).reset_index(drop=True)
# Add S25 biological entity semantic model components and set to default of 'not specified'
be_check = be_check.assign(S25_label='',
                           STAGE='not specified',
                           GENDER='not specified',
                           SIZE='not specified',
                           SUBCOMPONENT='not specified',
                           MORPHOLOGY='not specified',
                           COLOUR='not specified',
                           SUBGROUP='not specified',
                           )
print("Number of Biological Entities for P01 mapping: %s" % len(be_check))
summary.append(["Number of Biological Entities for P01 mapping:" , len(be_check)])

# Iterate through distinct ICES biota combinations.
for index, row in be_check.iterrows():
# Map matrix of the biota to appropriate NVS S25 SUBCOMPONENT and/or STAGE. Not some constraints based on taxa type applied in the code.
    if row['MATRX'] == 'WO':
        row['SUBCOMPONENT'] = 'not specified'
    elif row['MATRX'] == 'TM':
        row['SUBCOMPONENT'] = 'muscle tissue'
    elif row['MATRX'] == 'SI':
        row['SUBCOMPONENT'] = 'not specified'
    elif row['MATRX'] == 'SH':
        row['SUBCOMPONENT'] = 'shell'
    elif row['MATRX'] == 'SB':
        if row['Species'] not in ('Gobius','Crangon crangon','Mysidacea'):
            row['SUBCOMPONENT'] = 'flesh'
        else:
            row['SUBCOMPONENT'] = 'Checking species-matrx combo validity with ICES.'
    elif row['MATRX'] == 'RO':
        row['STAGE'] = 'eggs'
        row['SUBCOMPONENT'] = 'not specified'
    elif row['MATRX'] == 'MU&EP':
        row['SUBCOMPONENT'] = 'muscle tissues and skin'
    elif row['MATRX'] == 'MU':
        if row['Species'] == 'Loligo vulgaris':
            row['SUBCOMPONENT'] = 'flesh'
        else:
            row['SUBCOMPONENT'] = 'muscle tissue'
    elif row['MATRX'] == 'LI':
        row['SUBCOMPONENT'] = 'liver'
    elif row['MATRX'] == 'KI':
        row['SUBCOMPONENT'] = 'kidney'
    elif row['MATRX'] == 'GO':
        row['SUBCOMPONENT'] = 'gonads'
    elif row['MATRX'] == 'GI':
        row['SUBCOMPONENT'] = 'gill'
    elif row['MATRX'] == 'FE':
        row['SUBCOMPONENT'] = 'feathers'
    elif row['MATRX'] == 'FA':
        row['SUBCOMPONENT'] = 'body fat'
    elif row['MATRX'] == 'EX':
        if row['Species'] == 'Mytilus edulis':
            row['SUBCOMPONENT'] = 'shell'
        else:
            row['SUBCOMPONENT'] = 'Checking species-matrx combo validity with ICES.'
    elif row['MATRX'] == 'EP':
        row['SUBCOMPONENT'] = 'skin'
    elif row['MATRX'] == 'EH':
        row['STAGE'] = 'eggs'
        row['SUBCOMPONENT'] = 'egg yolk and albumen homogenate'
    elif row['MATRX'] == 'EG':
        row['STAGE'] = 'eggs'
    elif row['MATRX'] == 'BS':
        row['SUBCOMPONENT'] = 'blood serum'
    elif row['MATRX'] == 'BR':
        row['SUBCOMPONENT'] = 'brain'
    elif row['MATRX'] == 'BL':
        row['SUBCOMPONENT'] = 'blood'
    elif row['MATRX'] == 'BC':
        row['SUBCOMPONENT'] = 'blood cells'
    elif row['MATRX'] == 'BB':
        if row['Note'] != 'Fish':
            row['SUBCOMPONENT'] = 'blubber'
        else:
            row['SUBCOMPONENT'] = 'Checking species-matrx combo validity with ICES.'
    else:
        row['SUBCOMPONENT'] = 'Unexpected ICES MATRX value. Check mapping.'

    #Build S25 preflabel components for text matching
    label=''
    txn = row['TAXON']
    scp = row['SUBCOMPONENT']
    stg = row['STAGE']
    if np.any([txn == 'New TAXON required.', scp in ('Checking species-matrx combo validity with ICES.','New term needed.'), stg in ('Checking species-matrx combo validity with ICES.','New term needed.')]):
        label = 'No term for at least one S25 model list. Needs adding to NVS'
    elif np.all([txn != 'New TAXON required.', stg == 'not specified', scp == 'not specified']):
        label = ')'
    elif np.all([txn != 'New TAXON required.', stg == 'not specified', scp != 'not specified']):
        label = '[Subcomponent: %s]' % (scp)
    elif np.all([txn != 'New TAXON required.', stg != 'not specified', scp == 'not specified']):
        label = '[Stage: %s]' % (stg)
    else:
        label = '[Stage: %s Subcomponent: %s]' % (stg, scp)
    row['S25_label'] = (txn + " " + label).replace(") )", ")")
    
    print("Row %s of %s biota combinations mapped." % (index+1,len(be_check)))
        
# Add S25 code to the be_check table based on the S25_label   
be_check = pd.merge(be_check, nvs_be, how='left', on='S25_label').fillna(value = {'S25': 'New Biological Entity required.'})   
    
# Determine new combinations
S25_columns = ['DTYPE', 'Note', 'AphiaID', 'Species', 'MATRX','name_from_AphiaID', 
               'S25', 'S25_label', 
               'TAXON', 'STAGE', 'SUBCOMPONENT', 'COLOUR', 'GENDER', 'MORPHOLOGY', 'SIZE', 'SUBGROUP']
S25new = be_check[be_check['S25']=='New Biological Entity required.'][S25_columns].copy(deep=True).reset_index(drop=True)

print("Number of potential new S25 terms: %s" % len(S25new))
summary.append(["Number of potential new S25 terms:" , len(S25new)])

# Add S25 semantic model mapping to the main table based on the combination provided   
param_combo = pd.merge(param_combo, be_check, how='left', on=['DTYPE','Note','AphiaID','Species','MATRX','TAXON', 'name_from_AphiaID'])

# Mark rows that do not require mapping as "not specified"    
param_combo = param_combo.fillna(value={'COLOUR': 'not specified',
                                        'GENDER': 'not specified',
                                        'MORPHOLOGY': 'not specified',
                                        'SIZE': 'not specified',
                                        'STAGE': 'not specified',
                                        'SUBCOMPONENT': 'not specified',
                                        'SUBGROUP': 'not specified',
                                        'S25': 'BE007736',
                                        'S25_label': 'not applicable',
                                        })
        
print("Total combinations = %s" % (len(param_combo)))

#%% Iterate through file for ICES combinations to map S06, S07 and S02.
for index, row in param_combo.iterrows():
    # Logic for contaminants in sediment mappings
    if row['DTYPE'] == 'CS':
        if row['MUNIT'] == '%':
            row['S06_label'] = "Proportion"
        elif row['MUNIT'][-1] == 'g':
            row['S06_label'] = 'Concentration'
        else:
            row['S06_label'] = 'Check MUNIT'
            
        if row['BASIS'] == 'D':
            row['S02_label'] = 'per unit dry weight of'
        elif row['BASIS'] == 'W':
            row['S02_label'] = 'per unit wet weight of'
        else:
            row['S02_label'] = 'Check BASIS'
            
    # Logic for contaminants in water mappings       
    if row['DTYPE'] == 'CW':
        if row['MUNIT'] =='ntu':
            if row['PARAM'] == 'TURB':
                row['S06_label'] = 'Turbidity'
                row['S02_label'] = 'of the'        
        elif row['MUNIT'] == '%':
            row['S06_label'] = "Proportion"
        elif row['MUNIT'] == 'mBq/l':
            row['S06_label'] = "Activity"        
            row['S02_label'] = 'per unit volume of the'
        elif row['MUNIT'][-1] == 'g':
            row['S06_label'] = 'Concentration'
            row['S02_label'] = 'per unit mass of the'
        elif row['MUNIT'][-1] == 'l':
            row['S06_label'] = 'Concentration'
            row['S02_label'] = 'per unit volume of the'
        else:
            row['S06_label'] = 'Check MUNIT'
            row['S02_label'] = 'Check MUNIT'

    # Logic for contaminants in biota mappings
    if row['DTYPE'] == 'CF':
        if row['CAS'] in ('-9','NA'):
            if row['PARAM']!='CS137':
                row['S06_label'] = 'Generate mapping'
            else:
                row['S06_label'] = 'Concentration'
        else:
            row['S06_label'] = 'Concentration'

        # Map BASIS column for dry weight, wet weight and lipid normalised concentrations. Anything else requires checking.  
        if row['BASIS'] == 'D':
            row['S02_label'] = 'per unit dry weight of'
        elif row['BASIS'] == 'W':
            row['S02_label'] = 'per unit wet weight of'
        elif row['BASIS'] == 'L':
            row['S02_label'] = 'in'
            row['S06_label'] = 'Lipid-normalised concentration'
        else:
            row['S02_label'] = 'Check BASIS'
    print("Row %s of %s combinations mapped." % (index+1,len(param_combo)))
    
# Replaces any problematic text characters from the NVS imported into the DataFrame
# that will cause issues when writing the output to file.
param_combo.replace(u'\xa0',u' ', regex=True, inplace=True)
param_combo.replace(u'\u2019',u"'", regex=True, inplace=True)

print("Total combinations = %s" % (len(param_combo)))
         
#%% Map vocabulary labels to codes

param_combo = pd.merge(param_combo, S06, how='left', on='S06_label')
param_combo = pd.merge(param_combo, S07, how='left', on='S07_label')
param_combo = pd.merge(param_combo, S02, how='left', on='S02_label')
param_combo = pd.merge(param_combo, S26, how='left', on='S26_label')

#%% Map semantic model combinations to P01 

param_combo = pd.merge(param_combo, 
                       P01[P01['P01_label'].str.contains(" by ") == False], 
                       how = 'left', 
                       on = ['S06','S07','S27','S02','S26','S25'])

col_order = ['PARGROUP', 'PRNAM', 'CAS', 'DTYPE', 'PARAM', 'MUNIT', 'MATRX', 'BASIS', 'METPT', 'METOA', 'Note', 'AphiaID', 'Species', 
 'name_from_AphiaID', 'P01_Code', 'P01','P01_label',
 'S06_label', 'S07_label', 'S27_label', 'S02_label', 'S26_label', 'S25_label',
 'S06', 'S07', 'S27', 'S02', 'S26', 'S25']

param_combo = param_combo[col_order]

#%% Save the results of the ICES to NVS semantic model mapping to file.

# Split out SPM combinations to file where more worked needed at ICES to allow accurate mapping.
outputSPM = param_combo[param_combo['MATRX']=='SPM'].copy(deep=True).reset_index(drop=True)
SPMcount = len(outputSPM)

# Retain those combinations that are not MATRX=SPM
output = param_combo[param_combo['MATRX']!='SPM'].copy(deep=True).reset_index(drop=True)

# Split out those non-chemical measurements
output_nonchem = output[output['S27']=='not applicable'].copy(deep=True).reset_index(drop=True)
nonchemcount = len(output_nonchem)

# Generate list of paramters with full set of semantic model terms for P01 matching script
# Chemical codes only
output_chem = output[output['S27']!='not applicable'].copy(deep=True).reset_index(drop=True)
duplicate_check = inputs.columns.tolist()
chemcount_all = len(output_chem.drop_duplicates(subset = duplicate_check, keep='first'))

output_chem_mapped = output_chem[output_chem['S25']!='New Biological Entity required.'].copy(deep=True).reset_index(drop=True)
output_chem_mapped = output_chem_mapped[output_chem_mapped['S26_label']!='Check MATRX. Not mapped.'].copy(deep=True).reset_index(drop=True)
output_chem_mapped = output_chem_mapped[output_chem_mapped['S27']!='Potential S27 term exists.'].copy(deep=True).reset_index(drop=True)
output_chem_mapped = output_chem_mapped[output_chem_mapped['S27']!='No S27 term identified from CAS.'].copy(deep=True).reset_index(drop=True)

chem_map_count = len(output_chem_mapped.drop_duplicates(subset = duplicate_check, keep='first'))

P01_count = len(output_chem_mapped[output_chem_mapped['P01'].isnull()==False].drop_duplicates(subset = duplicate_check, keep='first'))
no_P01_count = len(output_chem_mapped[output_chem_mapped['P01'].isnull()==True].drop_duplicates(subset = duplicate_check, keep='first'))

P01_mapped = output_chem_mapped[output_chem_mapped['P01'].isnull()==False]

P01_single = P01_mapped.drop_duplicates(subset=duplicate_check, keep=False)
P01_single_count = len(P01_single)

P01_multi = P01_mapped[P01_mapped.duplicated(duplicate_check, keep='first')]
P01_multi_count = len(P01_multi)

output_S25incomplete = output_chem[output_chem['S25']=='New Biological Entity required.'].copy(deep=True).reset_index(drop=True)
noS25count = len(output_S25incomplete)
output_S26incomplete = output_chem[output_chem['S26_label']=='Check MATRX. Not mapped.'].copy(deep=True).reset_index(drop=True)
noS26count = len(output_S26incomplete)
output_S27incomplete = pd.concat([output_chem[output_chem['S27']=='Potential S27 term exists.'],
                                  output_chem[output_chem['S27']=='No S27 term identified from CAS.']]).copy(deep=True).reset_index(drop=True)
noS27count = len(output_S27incomplete)

print("Rows for SPM: %s" % SPMcount)
print("Rows for non-chemical substances: %s" % nonchemcount)
print("Rows for chemical substances: %s" % chemcount_all)
print("  Chemical rows for new code fully mapped: %s" % chem_map_count)
print("    Chemical rows with one P01 mapping: %s" % P01_single_count)
print("    Chemical rows with multiple P01 mappings: %s" % P01_multi_count)
print("    Chemical rows with no P01 mapping: %s" % no_P01_count)
print("  Chemical rows for new code not fully mapped: %s" % (chemcount_all - chem_map_count))
print("    Rows missing S25: %s" % noS25count)
print("    Rows missing S26: %s" % noS26count)
print("    Rows missing S27: %s" % noS27count)

summary.append(["", ""])
summary.append(["Result split by:", ""])
summary.append(["Rows for SPM:", "%s" % SPMcount])
summary.append(["Rows for non-chemical substances:", "%s" % nonchemcount])
summary.append(["Rows for chemical substances:", "%s" % chemcount_all])
summary.append(["  Chemical rows for new code fully mapped:", "%s" % (P01_count + no_P01_count)])
summary.append(["    Chemical rows with one P01 mapping:", "%s" % P01_single_count])
summary.append(["    Chemical rows with multiple P01 mappings:", "%s" % P01_multi_count])
summary.append(["    Chemical rows with no P01 mapping:", "%s" % no_P01_count])
summary.append(["  Chemical rows for new code not fully mapped:", "%s" % (chemcount_all - chem_map_count)])
summary.append(["    Rows missing S25:", "%s" % noS25count])
summary.append(["    Rows missing S26:", "%s" % noS26count])
summary.append(["    Rows missing S27:", "%s" % noS27count])


summary[1][1] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Create summary information
summary_df = pd.DataFrame(summary)

# Save outputs as worksheets within Excel file
with pd.ExcelWriter(fileout) as writer:
    # Details of processing
    summary_df.to_excel(writer, sheet_name='summary', header=False, index=False)

    # Copy of the input worksheet
    inputs.to_excel(writer, sheet_name='input-original', index=False)
    
    # Full output
    param_combo.to_excel(writer, sheet_name='output-all', index=False)
    
    # Rows completely mapped for next phase
    output_chem_mapped.to_excel(writer, sheet_name='output-mapping_complete', index=False)

    # Requires manual mapping at thsi stage
    outputSPM.to_excel(writer, sheet_name='manual-SPM_codes', index=False)
    output_nonchem.to_excel(writer, sheet_name='manual-nonchemical_codes', index=False)

    # Details of chemical substance mappings
    full_chem_map.to_excel(writer, sheet_name='mapping_PARAM-S27', index=False)
    S27_casmap_review.to_excel(writer, sheet_name='output-S27new', index=False)

    # Details of Matrix mappings
    matrix_check.to_excel(writer, sheet_name='mapping_MATRX-S26', index=False)
    S26new.to_excel(writer, sheet_name='output-S26new', index=False)

    # Details of taxa checks and biological entity mappings
    taxa_discrepancy.to_excel(writer, sheet_name='query-SpeciesNames', index=False)
    S25taxon_duplicates.to_excel(writer, sheet_name='query-S25taxon_duplicates', index=False)
    be_check.to_excel(writer, sheet_name='mapping_SpeciesMATRX-S25', index=False)
    S25new.to_excel(writer, sheet_name='output-S25new', index=False)




    


