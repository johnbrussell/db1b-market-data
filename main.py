import sys

import pandas as pd


class DB1B:
    def __init__(self, output_path, input_paths):
        assert output_path.endswith('.csv')
        self._output_path = output_path
        assert len(input_paths) > 0
        self._input_paths = input_paths
        self._full_df = None
        self._analysis_length = 0

    def enrich(self):
        self._get_fresh_data()
        df = self._add_fare_per_pax(None)
        df.to_csv(self._output_path)

    def _add_fare_per_pax(self, existing_df):
        df = self._full_df.copy()
        df['Pax/day'] = df['PASSENGERS'] / (0.1 * self._analysis_length)  # Data is a 10% sample
        df['Revenue/day'] = df['MARKET_FARE'] / (0.1 * self._analysis_length)
        df.drop('PASSENGERS', axis=1, inplace=True)
        df.drop('MARKET_FARE', axis=1, inplace=True)
        df = df.groupby(['ORIGIN', 'DEST', 'NONSTOP_MILES', 'TICKET_CARRIER'], as_index=False).sum()
        df['Fare/pax'] = df['Revenue/day'] / df['Pax/day']
        df['Yield'] = df['Fare/pax'] / df['NONSTOP_MILES']
        df_market = df.copy()
        df_market = df_market[['ORIGIN', 'DEST', 'Pax/day', 'Revenue/day']]
        df_market = df_market.groupby(['ORIGIN', 'DEST'], as_index=False).sum()
        df_market['Market fare/pax'] = df_market['Revenue/day'] / df_market['Pax/day']
        df_market.rename(columns={'Pax/day': 'Market pax/day'}, inplace=True)
        df_market.drop('Revenue/day', axis=1, inplace=True)
        df = df.merge(df_market, on=['ORIGIN', 'DEST'])
        df['Market yield'] = df['Market fare/pax'] / df['NONSTOP_MILES']
        df['Market fare premium'] = df['Fare/pax'] / df['Market fare/pax']
        if not existing_df:
            return df
        raise NotImplementedError('Code path not implemented')

    def _add_to_analysis_length(self, year, quarter):
        if quarter == 1 and year % 4 == 0:
            self._analysis_length += 31 + 29 + 31
        elif quarter == 1:
            self._analysis_length += 31 + 28 + 31
        elif quarter == 2:
            self._analysis_length += 30 + 31 + 30
        elif quarter == 3:
            self._analysis_length += 31 + 31 + 30
        else:
            self._analysis_length += 31 + 30 + 31

    def _get_data_file(self, input_path):
        df = pd.read_csv(input_path)
        df = df[['YEAR', 'QUARTER', 'ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'NONSTOP_MILES']]
        self._add_to_analysis_length(df['YEAR'][0], df['QUARTER'][0])
        df = df[['ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'NONSTOP_MILES']]
        return df

    def _get_fresh_data(self):
        self._full_df = pd.concat([self._get_data_file(df) for df in self._input_paths])


def main():
    db1b = DB1B(sys.argv[1], sys.argv[2:])
    db1b.enrich()


if __name__ == '__main__':
    main()
