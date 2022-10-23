#
# main.py
#

# establishes the workflow pipeline of the analysis

from photovoltaicperformance import *

# define relevant disk locations
data_input_folder = './data/in'
data_consumed_folder = './data/consumed'
data_functional_folder = './data'

# acquire any new data that may be available at the input folder

#
# consumption
#
consumption_df = get_consumption_data(data_input_folder, data_consumed_folder)
print(consumption_df.describe())

# define the name of the dataframe - this will help with naming the permanent file storage
consumption_df.name = "consumption_df"

# let's save the data to disk now
save_df_to_disk(consumption_df, data_functional_folder)


#
# production
#


