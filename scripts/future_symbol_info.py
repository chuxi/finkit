import pandas as pd


if __name__ == '__main__':
    df = pd.read_excel("../全部品种.xlsx")
    df = df.rename(columns={'代码': 'variety', '交易所': 'exchange',
                            '证券名称': 'name', '合约乘数': 'unit', '报价单位': 'price_unit'})
    df = df[['variety', 'exchange', 'name', 'unit', 'price_unit']]
    # df.to_csv("../src/finkit/variety_info.csv", index=False)
    # df = df.set_index('variety')
    # df = df.to_dict('index')
    print(df)
