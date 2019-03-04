# -*- coding: utf-8 -*-
"""
Created on Fri Aug 18 12:55:33 2017

@author: rthomas
"""

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
