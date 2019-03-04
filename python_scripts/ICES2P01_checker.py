# -*- coding: utf-8 -*-
"""
Created on Fri Aug 18 12:55:33 2017

@author: rthomas
"""

# Import toolboxes used by the script and it's functions
import pandas as pd
import os
import datetime

start = datetime.datetime.now()

# Add filepaths for input file here
sourcefile = os.path.normpath('C:/Users/rthomas/Documents/GitHub/ICES2NVS_semantic_map/example/ICES2P01_test_dset_mapped.xlsx')
# Generate output filenames
output = os.path.normpath(sourcefile[:-4]+'_output%s.xlsx' % (start.strftime('%Y-%m-%dT%H%M%S')))

# Add filepaths for mapping files here
p02_file = os.path.normpath('C:/Users/rthomas/Documents/GitHub/ICES2NVS_semantic_map/mappings/ICES2P02_mapping.txt')

#%% Main body of the script follows
# Get the latest semantic model vocabulary contents from the NVS Sparql endpoint

a1 = "http://vocab.nerc.ac.uk/sparql/sparql?query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A++++PREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0A++++%0D%0A++++select+%3F"
a2 = "+%3F"
a3 = "%0D%0A++++where+%7B%0D%0A++++%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2F"
a4 ="%2Fcurrent%2F%3E+skos%3Amember+%3Furl+.%0D%0A++++%3Furl+skos%3AprefLabel+%3F"
a5 = "+.%0D%0A++++%3Furl+skos%3Anotation+%3Fc+.%0D%0A++++%3Furl+owl%3Adeprecated+%27false%27+.%0D%0A++++BIND%28replace%28str%28%3Fc%29%2C%27SDN%3A"
a6 = "%3A%3A%27%2C%27%27%2C%27i%27%29+AS+%3F"
a7 = "%29%0D%0A++++%7D&output=csv&stylesheet="

S06 = pd.read_csv(a1+'S06'+a2+'S06_label'+a3+'S06'+a4+'S06_label'+a5+'S06'+a6+'S06'+a7)
S07 = pd.read_csv(a1+'S07'+a2+'S07_label'+a3+'S07'+a4+'S07_label'+a5+'S07'+a6+'S07'+a7)
S02 = pd.read_csv(a1+'S02'+a2+'S02_label'+a3+'S02'+a4+'S02_label'+a5+'S02'+a6+'S02'+a7)
S26 = pd.read_csv(a1+'S26'+a2+'S26_label'+a3+'S26'+a4+'S26_label'+a5+'S26'+a6+'S26'+a7)
P01 = pd.read_csv(a1+'P01'+a2+'P01_label'+a3+'P01'+a4+'P01_label'+a5+'P01'+a6+'P01'+a7)

#%% Build P01 semantic model
driver = {'S06': 'skos:narrower',
          'S07': 'skos:narrower',
          'S27': 'skos:narrower',
          'S02': 'skos:related',
          'S26': 'skos:narrower',
          'S25': 'skos:narrower'}
vocab='S27'
relation='skos:narrower'

query = """PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
    
select distinct ?%s ?P01
where {
<http://vocab.nerc.ac.uk/collection/%s/current/> skos:member ?urla .
?urla owl:deprecated 'false' .
?urla skos:notation ?n2 .
?urla %s ?urlb .
<http://vocab.nerc.ac.uk/collection/P01/current/> skos:member ?urlb .
?urlb owl:deprecated 'false' .
?urlb skos:notation ?n1 .
BIND(replace(?n1, "SDN:P01::", "", "i") AS ?P01) .
BIND(replace(?n2, "SDN:%s::", "", "i") AS ?%s) .
}
""" % (vocab, vocab, relation, vocab, vocab)
print query

#%% Download semantic component mapping

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

#%% Build P01 semantic model dataframe
P01 = pd.merge(P01, S06_P01, how='left', on='P01')
P01 = pd.merge(P01, S07_P01, how='left', on='P01')
P01 = pd.merge(P01, S27_P01, how='left', on='P01')
P01 = pd.merge(P01, S02_P01, how='left', on='P01')
P01 = pd.merge(P01, S26_P01, how='left', on='P01')
P01 = pd.merge(P01, S25_P01, how='left', on='P01')

P01 = P01.fillna(value={'S25': 'BE007736', 'S07': 'S0700006'})

#%% Open the source file and load to a Pandas DataFrame
inputs = pd.read_excel(os.path.normpath(sourcefile), sheet_name='output-mapping_complete')

# Create a working copy and add empty columns to populate the S06, S07, S02 and S26 vocabulary CODVALs from the labels.
# Also add columns to hold the matched P01 code and labels
d = inputs.copy(deep=True)

d = pd.merge(d, S06, how='left', on='S06_label')
d = pd.merge(d, S07, how='left', on='S07_label')
d = pd.merge(d, S02, how='left', on='S02_label')
d = pd.merge(d, S26, how='left', on='S26_label')

d_cols = ['PARGROUP', 'PRNAM', 'CAS', 'DTYPE', 'PARAM', 'MUNIT', 'MATRX', 'BASIS', 'METPT', 'METOA', 'Note', 'AphiaID', 'Species', 
 'name_from_AphiaID', 'P01_Code',
 'S06_label', 'S07_label', 'S27_label', 'S02_label', 'S26_label', 'S25_label',
 'S06', 'S07', 'S27', 'S02', 'S26', 'S25']

d = d[d_cols]

#%% Map to P01 

mapped = pd.merge(d, P01, how = 'left', on = ['S06','S07','S27','S02','S26','S25'])


#%% Save results

# Set output file column order
out_list = ['PARGROUP','PRNAM','CAS','DTYPE','PARAM','MUNIT','MATRX','BASIS','METPT','METOA','Note','AphiaID','Species','name_from_AphiaID',
            'P01_Code','S06_label','S07_label','S27_label','S02_label','S26_label','S25_label',
            'P01','P01_label','S06','S07','S27','S02','S26','S25']

# Subset all parameters that have been matched
all_matched = d[out_list][d['P01']!=''].reset_index(drop=True)

# Split out 1:1 matches and 1:many that need manual intervention
single_matched = all_matched[all_matched['P01'].str.contains('~')==False].reset_index(drop=True)
multi_matched = all_matched[all_matched['P01'].str.contains('~')==True].reset_index(drop=True)

# No match found
no_match = d[out_list][d['P01']==''].reset_index(drop=True)

# Add P02 code to parameters for creation. Aids BODC in publishing the new vocabulary terms.
# Load P02 mapping based on Data type (water, sediment, biota) and the ICES chemical parameter group - at present stored as a static file
p02_map = pd.read_csv(p02_file, sep='\t')

# Add the P02 code as a column by merging the contents of the unmatched dataframe content with the P02 mapping matching on ICES Parameter group and Data Type
# Requires unpivoting of the P02 mapper file contents
unmatched = pd.merge(no_match, 
                     pd.melt(p02_map.rename(index=str, columns={"WATER": "CW", "SED": "CS", "BIOTA": "CF"}), 
                             id_vars=['PRGROUP','NAME'], 
                             value_vars=['CF', 'CW','CS'], 
                             var_name='DTYPE', 
                             value_name='P02'), 
                     how='inner', 
                     on=None, 
                     left_on=['PARGROUP','DTYPE'], 
                     right_on=['PRGROUP','DTYPE']).drop(columns=['PRGROUP','NAME'])

# Save outputs as worksheets within Excel file
with pd.ExcelWriter(output) as writer:
    inputs.to_excel(writer, sheet_name='input')
    d[out_list].to_excel(writer, sheet_name='full_output')
    all_matched.to_excel(writer, sheet_name='all_matched')
    single_matched.to_excel(writer, sheet_name='single_matched')
    multi_matched.to_excel(writer, sheet_name='multi_matched')
    no_match.to_excel(writer, sheet_name='no_match')
    unmatched.to_excel(writer, sheet_name='for_creation')

# Print summary to the screen
print("Processing started: %s" % (start.strftime('%Y-%m-%d %H:%M:%S')))
print("Processing finished: %s" % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("Source filepath: %s" % sourcefile)
print("Rows input: %s" % len(inputs))
print("Rows matched: %s" % len(all_matched))
print("1:1 matches: %s" % len(single_matched))
print("1:many matches: %s" % len(multi_matched))
print("No matches: %s" % len(no_match))
