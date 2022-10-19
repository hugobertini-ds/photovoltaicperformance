#
#  photovoltaicperformance.py
#

# contains the functions that will be used throughout the analysis pipeline flow

# import the needed libraries
import vaex          # I tend to prefer using lazy computation strategies, so I picked vaex over Pandas for doing the hard work on the data
import pandas as pd  # Still, Pandas is to be used to assist with the data loading process
import datetime      # We need this to come up with a proper name for disk-stored dataframes



def get_consumption_data(input_folder, consumed_folder):
    """
    the data is expected to follow either a daily (Leituras*.xlsx) or a 15-minute basis (Consumos*.xlsx)
    the dataframe returned will support and contain data from both kinds of information.
    For example, the daily basis data will have no time filled-in;
                 while the 15-minute data will not distinguish consumption periods (vazio aka economy, ponta and cheias)

    input:  input and consumed folder names (with no ending slash '/')
    return: dataframe vaex dataframe with columns:
        date, time,
        daily_origin, daily_status, daily_consumed, daily_vazio, daily_ponta, daily_cheias,
        15min_consumption
    """
    import os

    # initialize the dataframe
    vx_df = vaex.from_pandas(pd.DataFrame(
        columns=["Date", "Time", "RecordedConsumedkW", "Source", "Status", "ConsumedOrProduced", "Vazio", "Ponta",
                 "Cheias"]))

    # read folder contents
    for x in os.listdir(input_folder):
        # read daily data: Leituras
        if x.startswith("Leituras") and x.endswith(".xlsx"):
            # getting daily data
            df = vaex.from_pandas(pd.read_excel(f"{input_folder}/{x}", skiprows=7))
            # rename columns for analysis convenience
            # from: Data da Leitura 	Origem 	Estado 		Vazio 	Ponta 	Cheias
            #   to: Data 	Source 	Status 		Vazio 	Ponta 	Cheias
            df.rename('Data da Leitura', 'Date')
            df.rename('Origem', 'Source')
            df.rename('Estado', 'Status')
            df.rename(' ', 'ConsumedOrProduced')
            vx_df = vaex.concat([vx_df, df])
            # print(f"{vx_df.shape} daily records consumed.")

        # read 15-minute data: Consumos
        if x.startswith("Consumos") and x.endswith(".xlsx"):
            # getting 15-minute data
            df = vaex.from_pandas(pd.read_excel(f"{input_folder}/{x}", skiprows=8))
            # rename columns for analysis convenience
            # from: Data 	Hora 	Consumo registado kW
            #   to: Date 	Time 	RecordedConsumedkW
            df.rename('Data', 'Date')
            df.rename('Hora', 'Time')
            df.rename('Consumo registado kW', 'RecordedConsumedkW')
            vx_df = vaex.concat([vx_df, df])
            # print(f"{vx_df.shape} 15-min records consumed.")

        # move consumed data file to "consumed" folder
        if x.endswith(".xlsx"):
            print(f"{df.shape[0]} records contributed from {x}!")
            # after reading data, move the file to "consumed" folder
            #os.rename(f"{input_folder}/{x}", f"{consumed_folder}/{x}")
            pass
        # if the dataframe is not empty, then print its head
        # if(vx_df.shape[0] > 0):
        #    print(f"current dataframe: \n{vx_df.tail(2)}")

    # print the resulting iidataframe
    # print(vx_df)
    print(f"Total records collected: {vx_df.shape[0]}.")

    # return the resulting dataframe
    return vx_df



def save_df_to_disk(df, destination_folder):
    """
    saves the content of a vaex DataFrame to an existing & accessible storage location
    in:     vaex dataframe presumably containing data
            string with the path to the destination folder
    out:    hdf5 file stored in the specified permanent storage location
            returns the number of records contained in the received DataFrame
    """
    print(f"saving {df.name} to permanent storage")
    if(df.shape[0] == 0):
        print("there is no data for saving to disk")
    else:
        print("there is data for saving to disk. let's go!")
        # get the current date & time for file-naming purposes
        current_time = datetime.datetime.now()
        now = str(current_time).split(".")[0].replace(" ", "_").replace(":", "").replace("-", "")

        # now is a good time to store the data
        print(f"{df.name} was saved to permanent storage as csv.")
        file_extension = "arrow"
        print(f"saving as {file_extension}...")
        #df.export(f'{destination_folder}/{df.name}_{now}.{file_extension}', progress=True)
        filename = f'{destination_folder}/{df.name}_{now}.{file_extension}'
        df.export(filename, progress=True)
        print(f"{df.name} was saved to permanent storage as {filename} file.")
    return df.shape[0]



