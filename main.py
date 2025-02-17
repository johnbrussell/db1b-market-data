import sys

import pandas as pd


class DB1B:
    def __init__(self, output_path, input_paths):
        assert output_path.endswith('.csv')
        self._output_path = output_path
        assert len(input_paths) > 0
        self._input_paths = input_paths

    def enrich(self):
        df = self._get_fresh_data()
        df.to_csv(self._output_path)

    @staticmethod
    def _get_data_file(input_path):
        df = pd.read_csv(input_path)
        df = df[['YEAR', 'QUARTER', 'ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'NONSTOP_MILES']]
        # This is intentional; verify files have year and quarter present upon download
        df = df[['ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'NONSTOP_MILES']]
        return df

    def _get_fresh_data(self):
        return pd.concat([self._get_data_file(df) for df in self._input_paths])


def main():
    db1b = DB1B(sys.argv[1], sys.argv[2:])
    db1b.enrich()


if __name__ == '__main__':
    main()
