import sys

import pandas as pd


class DB1B:
    def __init__(self, output_path, input_paths):
        assert output_path.endswith('.csv')
        self._output_path = output_path
        assert len(input_paths) > 0
        self._input_paths = input_paths
        self._full_df = None

    def enrich(self):
        self._get_fresh_data()
        df = self._add_fare_per_pax(None)
        df.to_csv(self._output_path)

    def _add_fare_per_pax(self, existing_df):
        df = self._full_df.groupby(['ORIGIN', 'DEST', 'NONSTOP_MILES', 'TICKET_CARRIER'], as_index=False).sum()
        df['Fare/pax'] = df['MARKET_FARE'] / df['PASSENGERS']
        df['Yield'] = df['Fare/pax'] / df['NONSTOP_MILES']
        df.drop('MARKET_FARE', axis=1, inplace=True)
        if not existing_df:
            return df
        raise NotImplementedError('Code path not implemented')

    @staticmethod
    def _get_data_file(input_path):
        df = pd.read_csv(input_path)
        df = df[['YEAR', 'QUARTER', 'ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'NONSTOP_MILES']]
        # This is intentional; verify files have year and quarter present upon download
        df = df[['ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'NONSTOP_MILES']]
        return df

    def _get_fresh_data(self):
        self._full_df = pd.concat([self._get_data_file(df) for df in self._input_paths])


def main():
    db1b = DB1B(sys.argv[1], sys.argv[2:])
    db1b.enrich()


if __name__ == '__main__':
    main()
