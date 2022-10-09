import vaex
import pandas as pd

def main():
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





