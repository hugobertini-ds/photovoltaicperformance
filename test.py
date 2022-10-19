import ntplib
import datetime
from time import ctime
import vaex
import pandas as pd

def main():
    c = ntplib.NTPClient()
    response = c.request('europe.pool.ntp.org', version=3)
    print(f"offset from ntp: {response.offset}")
    print(response.version)

    current_time = datetime.datetime.now()

    print(f"ntp time: {ctime(response.tx_time)}")
    print(f"machine time: {current_time}")




def main_old_2():
    filename = "./data/consumption_df_20221014_213339.arrow"
    print(f"loading {filename}...")
    df = vaex.open(filename)
    print(f"{filename} loaded.\n{df}\ndimensions: {df.shape}. describing...")
    print(df.describe())
    print(f"{filename} file described.")


def main_old():
    df1 = vaex.from_pandas(pd.read_excel("data/Consumos_202201_20220927.xlsx", skiprows=8))
    # rename columns for analysis convenience
    # from: Data 	Hora 	Consumo registado kW
    #   to: Date 	Time 	RecordedConsumedkW
    df1.rename('Data', 'Date')
    df1.rename('Hora', 'Time')
    df1.rename('Consumo registado kW', 'RecordedConsumedkW')
    # print(df1.tail(2))

    print(f"df1:\n{df1.describe()}")

    df2 = vaex.from_pandas(pd.read_excel("data/Leituras_20220930_de20220801a20220930.xlsx", skiprows=7))
    # rename columns for analysis convenience
    # from: Data da Leitura 	Origem 	Estado 		Vazio 	Ponta 	Cheias
    #   to: Data 	Source 	Status 		Vazio 	Ponta 	Cheias
    df2.rename('Data da Leitura', 'Date')
    df2.rename('Origem', 'Source')
    df2.rename('Estado', 'Status')
    df2.rename(' ', 'ConsumedOrProduced')
    # print(df2.tail(2))

    print(f"df2:\n{df2.describe()}")

    df = vaex.concat([df1, df2])
    print(f"concatenated df:\n{df}")

    print("describing concatenated df...:")
    print(f"here it goes...:\n{df.describe()}")
    print("can you see me?")


if __name__ == "__main__":
   try:
      main()
   except Exception as e:
      logger.exception("main crashed. Error: %s", e)





