import json
import sys

import pandas as pd


class DB1B:
    def __init__(self, output_path, input_paths):
        self._load_configuration()
        assert output_path.endswith('.csv')
        self._output_path = output_path
        assert len(input_paths) > 0
        self._input_paths = input_paths
        self._full_df = None
        self._analysis_length = 0

    def enrich(self):
        self._get_fresh_data()

        def enrich_df(full_df):
            df = self._add_fare_per_pax(full_df.copy())
            df = self._add_shares(df, full_df.copy())
            df = self._add_distance_premiums(df)
            df = self._filter_at_end(df)
            return df

        unfiltered_df = enrich_df(self._full_df.copy())
        filtered_df = enrich_df(self._filtered_df.copy())

        merge_columns = ['ORIGIN', 'DEST', 'TICKET_CARRIER']
        unfiltered_df = unfiltered_df[[col for col in unfiltered_df.columns if col in merge_columns or ('yield' not in col and 'premium' not in col)]]
        filtered_df = filtered_df[[col for col in filtered_df.columns if col in merge_columns or 'yield' in col or 'premium' in col]]
        merged_df = unfiltered_df.merge(filtered_df, how='left', on=merge_columns)

        merged_df = self._reorder_output_columns(merged_df)
        merged_df.to_csv(self._output_path)

    def _add_distance_premiums(self, existing_df):
        df = existing_df.copy()
        market_distance_df = df[['ORIGIN', 'DEST', 'TICKET_CARRIER', 'NONSTOP_MILES', 'Revenue/day', 'Total revenue/day', 'Pax/day', 'Adj pax/day']].copy()
        market_distance_df['Distance bucket'] = market_distance_df['NONSTOP_MILES'].apply(self._distance_bucket)
        market_distance_bucket_df = market_distance_df[['NONSTOP_MILES', 'Distance bucket']].copy()
        market_distance_bucket_df.drop_duplicates(inplace=True)
        df = df.merge(market_distance_bucket_df, on='NONSTOP_MILES')
        bucket_df = market_distance_df[['Distance bucket', 'NONSTOP_MILES', 'Revenue/day', 'Total revenue/day', 'Pax/day', 'Adj pax/day']].copy()
        bucket_df['Pax miles'] = bucket_df['NONSTOP_MILES'] * bucket_df['Pax/day']
        bucket_df['Adj pax miles'] = bucket_df['NONSTOP_MILES'] * bucket_df['Adj pax/day']
        bucket_df.drop(columns=['NONSTOP_MILES', 'Pax/day', 'Adj pax/day'], axis=1, inplace=True)
        bucket_df = bucket_df.groupby('Distance bucket', as_index=False).sum()
        bucket_df['Distance bucket yield'] = bucket_df['Revenue/day'] / bucket_df['Pax miles']
        bucket_df['Distance bucket total yield'] = bucket_df['Total revenue/day'] / bucket_df['Pax miles']
        bucket_df['Distance bucket density-adjusted total yield'] = bucket_df['Total revenue/day'] / bucket_df['Adj pax miles']
        bucket_df.drop(columns=['Pax miles', 'Adj pax miles', 'Revenue/day', 'Total revenue/day'], axis=1, inplace=True)
        bucket_df.drop_duplicates(inplace=True)
        df = df.merge(bucket_df, on='Distance bucket')
        df['Distance yield premium'] = df['Yield'] / df['Distance bucket yield']
        df['Distance total yield premium'] = df['Total yield'] / df['Distance bucket total yield']
        df['Distance total flight yield premium'] = df['Density-adjusted total yield'] / df['Distance bucket density-adjusted total yield']
        df['Market\'s distance yield premium'] = df['Market yield'] / df['Distance bucket yield']
        df['Market\'s distance total yield premium'] = df['Market total yield'] / df['Distance bucket total yield']
        df['Market\'s distance total flight yield premium'] = df['Market density-adjusted total yield'] / df['Distance bucket density-adjusted total yield']

        metro_distance_df = df[['Origin metro', 'Destination metro', 'TICKET_CARRIER', 'NONSTOP_MILES', 'Revenue/day', 'Total revenue/day', 'Pax/day', 'Adj pax/day']].copy()
        metro_distance_df['Pax miles'] = metro_distance_df['NONSTOP_MILES'] * metro_distance_df['Pax/day']
        metro_distance_df['Adj pax miles'] = metro_distance_df['NONSTOP_MILES'] * metro_distance_df['Adj pax/day']
        metro_distance_distance_df = metro_distance_df[['Origin metro', 'Destination metro', 'Pax miles', 'Adj pax miles', 'Pax/day', 'Adj pax/day']].copy()
        metro_distance_distance_df = metro_distance_distance_df.groupby(['Origin metro', 'Destination metro'], as_index=False).sum()
        metro_distance_distance_df['Metro distance'] = metro_distance_distance_df['Pax miles'] / metro_distance_distance_df['Pax/day']
        metro_distance_distance_df.drop(columns=['Pax miles', 'Adj pax miles', 'Pax/day', 'Adj pax/day'], axis=1, inplace=True)
        metro_distance_distance_df.drop_duplicates(inplace=True)
        metro_distance_df = metro_distance_df.merge(metro_distance_distance_df, on=['Origin metro', 'Destination metro'])
        metro_distance_df['Metro distance bucket'] = metro_distance_df['Metro distance'].apply(self._distance_bucket)
        metro_distance_df.drop('Metro distance', axis=1, inplace=True)
        metro_distance_df.drop_duplicates(inplace=True)
        metro_distance_bucket_df = metro_distance_df[['Metro distance bucket', 'Origin metro', 'Destination metro']].copy()
        metro_distance_bucket_df.drop_duplicates(inplace=True)
        df = df.merge(metro_distance_bucket_df, on=['Origin metro', 'Destination metro'])
        bucket_df = metro_distance_df[['Metro distance bucket', 'Pax miles', 'Adj pax miles', 'Revenue/day', 'Total revenue/day']].copy()
        bucket_df = bucket_df.groupby('Metro distance bucket', as_index=False).sum()
        bucket_df['Metro distance bucket yield'] = bucket_df['Revenue/day'] / bucket_df['Pax miles']
        bucket_df['Metro distance bucket total yield'] = bucket_df['Total revenue/day'] / bucket_df['Pax miles']
        bucket_df['Metro distance bucket density-adjusted total yield'] = bucket_df['Total revenue/day'] / bucket_df['Adj pax miles']
        bucket_df.drop(columns=['Pax miles', 'Adj pax miles', 'Revenue/day', 'Total revenue/day'], axis=1, inplace=True)
        bucket_df.drop_duplicates(inplace=True)
        df = df.merge(bucket_df, on='Metro distance bucket')
        df['Metro distance yield premium'] = df['Yield'] / df['Metro distance bucket yield']
        df['Metro distance total yield premium'] = df['Total yield'] / df['Metro distance bucket total yield']
        df['Metro distance total flight yield premium'] = df['Density-adjusted total yield'] / df['Metro distance bucket density-adjusted total yield']
        df['Metro\'s distance yield premium'] = df['Metro yield'] / df['Metro distance bucket yield']
        df['Metro\'s distance total yield premium'] = df['Metro total yield'] / df['Metro distance bucket total yield']
        df['Metro\'s distance total flight yield premium'] = df['Metro density-adjusted total yield'] / df['Metro distance bucket density-adjusted total yield']

        df['Adj pax miles'] = df['NONSTOP_MILES'] * df['Adj pax/day']
        origin_df = df[['ORIGIN', 'TICKET_CARRIER', 'Distance bucket', 'Total revenue/day', 'Adj pax miles']].copy()
        origin_df = origin_df.groupby(['ORIGIN', 'TICKET_CARRIER', 'Distance bucket'], as_index=False).sum()
        origin_df['Carrier origin distance flight yield'] = origin_df['Total revenue/day'] / origin_df['Adj pax miles']
        origin_df.drop(columns=['Total revenue/day', 'Adj pax miles'], inplace=True)

        bucket_df = df[['Distance bucket', 'Total revenue/day', 'Adj pax miles']].copy()
        bucket_df = bucket_df.groupby(['Distance bucket'], as_index=False).sum()
        bucket_df['Distance flight yield'] = bucket_df['Total revenue/day'] / bucket_df['Adj pax miles']
        bucket_df.drop(columns=['Total revenue/day', 'Adj pax miles'], inplace=True)

        df = df.merge(origin_df, on=['ORIGIN', 'TICKET_CARRIER', 'Distance bucket'])
        df = df.merge(bucket_df, on=['Distance bucket'])
        df['Carrier origin distance total flight yield premium'] = df['Carrier origin distance flight yield'] / df['Distance flight yield']

        weighted_df = df[['ORIGIN', 'TICKET_CARRIER', 'Adj pax miles', 'Carrier origin distance total flight yield premium']].copy()
        weighted_df['Yield premium miles'] = weighted_df['Adj pax miles'] * weighted_df['Carrier origin distance total flight yield premium']
        partial_weighted_df = weighted_df.copy()
        df = df.merge(partial_weighted_df, on=['ORIGIN', 'TICKET_CARRIER', 'Adj pax miles', 'Carrier origin distance total flight yield premium'])
        df['Yield miles (1000)'] = df['Yield premium miles'] / 1000
        weighted_df.drop(columns=['Carrier origin distance total flight yield premium'], inplace=True)
        weighted_df = weighted_df.groupby(['ORIGIN', 'TICKET_CARRIER']).sum()
        weighted_df['Carrier origin average yield premium'] = weighted_df['Yield premium miles'] / weighted_df['Adj pax miles']
        weighted_df.drop(columns=['Yield premium miles', 'Adj pax miles'], inplace=True)
        df = df.merge(weighted_df, on=['ORIGIN', 'TICKET_CARRIER'])
        df.drop(columns=['Adj pax miles'], inplace=True)

        return df

    @staticmethod
    def _add_fare_per_pax(existing_df):
        df = existing_df.copy()
        df_metro_holder = df.copy()[['ORIGIN', 'DEST', 'Origin metro', 'Destination metro']].drop_duplicates()
        df.drop(columns=['Origin metro', 'Destination metro'], axis=1, inplace=True)
        df = df.groupby(['ORIGIN', 'DEST', 'NONSTOP_MILES', 'TICKET_CARRIER'], as_index=False).sum()
        df = df.merge(df_metro_holder, on=['ORIGIN', 'DEST'])
        df['Fare/pax'] = df['Revenue/day'] / df['Pax/day']
        df['Total fare/pax'] = df['Total revenue/day'] / df['Pax/day']
        df['Density-adjusted fare/pax'] = df['Total revenue/day'] / df['Adj pax/day']
        df['Yield'] = df['Fare/pax'] / df['NONSTOP_MILES']
        df['Total yield'] = df['Total fare/pax'] / df['NONSTOP_MILES']
        df['Density-adjusted total yield'] = df['Density-adjusted fare/pax'] / df['NONSTOP_MILES']

        df_market = df.copy()
        df_market = df_market[['ORIGIN', 'DEST', 'Pax/day', 'Adj pax/day', 'Revenue/day', 'Total revenue/day']]
        df_market = df_market.groupby(['ORIGIN', 'DEST'], as_index=False).sum()
        df_market['Market fare/pax'] = df_market['Revenue/day'] / df_market['Pax/day']
        df_market['Market total fare/pax'] = df_market['Total revenue/day'] / df_market['Pax/day']
        df_market['Market density-adjusted fare/pax'] = df_market['Total revenue/day'] / df_market['Adj pax/day']
        df_market.rename(columns={'Pax/day': 'Market pax/day', 'Adj pax/day': 'Market adj pax/day'}, inplace=True)
        df_market.drop(columns=['Revenue/day', 'Total revenue/day'], axis=1, inplace=True)
        df = df.merge(df_market, on=['ORIGIN', 'DEST'])
        df['Market yield'] = df['Market fare/pax'] / df['NONSTOP_MILES']
        df['Market total yield'] = df['Market total fare/pax'] / df['NONSTOP_MILES']
        df['Market density-adjusted total yield'] = df['Market density-adjusted fare/pax'] / df['NONSTOP_MILES']
        df['Market share'] = df['Pax/day'] / df['Market pax/day']
        df['Market fare premium'] = df['Fare/pax'] / df['Market fare/pax']
        df['Market total fare premium'] = df['Total fare/pax'] / df['Market total fare/pax']
        df['Market total flight premium'] = df['Density-adjusted fare/pax'] / df['Market density-adjusted fare/pax']

        df_metro = existing_df.copy()
        df_metro = df_metro[['Origin metro', 'Destination metro', 'NONSTOP_MILES', 'TICKET_CARRIER', 'Pax/day', 'Adj pax/day', 'Revenue/day', 'Total revenue/day']]
        df_metro['Pax miles'] = df_metro['Pax/day'] * df_metro['NONSTOP_MILES']
        df_metro['Adj pax miles'] = df_metro['Adj pax/day'] * df_metro['NONSTOP_MILES']
        df_metro_carrier = df_metro.copy().groupby(['Origin metro', 'Destination metro', 'TICKET_CARRIER'], as_index=False).sum()
        df_metro_carrier['Carrier metro yield'] = df_metro_carrier['Revenue/day'] / df_metro_carrier['Pax miles']
        df_metro_carrier['Carrier metro total yield'] = df_metro_carrier['Total revenue/day'] / df_metro_carrier['Pax miles']
        df_metro_carrier['Carrier metro density-adjusted total yield'] = df_metro_carrier['Total revenue/day'] / df_metro_carrier['Adj pax miles']
        df_metro_carrier.rename(columns={'Pax/day': 'Carrier metro pax/day', 'Adj pax/day': 'Carrier metro adj pax/day'}, inplace=True)
        df_metro_carrier.drop(columns=['NONSTOP_MILES', 'Revenue/day', 'Total revenue/day', 'Pax miles', 'Adj pax miles'], axis=1, inplace=True)
        df = df.merge(df_metro_carrier, on=['Origin metro', 'Destination metro', 'TICKET_CARRIER'])

        df_metro.drop(columns=['TICKET_CARRIER', 'NONSTOP_MILES'], axis=1, inplace=True)
        df_metro = df_metro.groupby(['Origin metro', 'Destination metro'], as_index=False).sum()
        df_metro['Metro fare/pax'] = df_metro['Revenue/day'] / df_metro['Pax/day']
        df_metro['Metro total fare/pax'] = df_metro['Total revenue/day'] / df_metro['Pax/day']
        df_metro['Metro total adj fare/pax'] = df_metro['Total revenue/day'] / df_metro['Adj pax/day']
        df_metro['Distance'] = df_metro['Pax miles'] / df_metro['Pax/day']
        df_metro['Metro yield'] = df_metro['Metro fare/pax'] / df_metro['Distance']
        df_metro['Metro total yield'] = df_metro['Metro total fare/pax'] / df_metro['Distance']
        df_metro['Metro density-adjusted total yield'] = df_metro['Metro total adj fare/pax'] / df_metro['Distance']
        df_metro.drop(columns=['Revenue/day', 'Total revenue/day', 'Distance', 'Pax miles', 'Adj pax miles'], axis=1, inplace=True)
        df_metro.rename(columns={'Pax/day': 'Metro pax/day', 'Adj pax/day': 'Metro adj pax/day'}, inplace=True)
        df = df.merge(df_metro, on=['Origin metro', 'Destination metro'])
        df['Metro share'] = df['Carrier metro pax/day'] / df['Metro pax/day']
        df['Metro fare premium'] = df['Carrier metro yield'] / df['Metro yield']
        df['Metro total fare premium'] = df['Carrier metro total yield'] / df['Metro total yield']
        df['Metro total flight premium'] = df['Carrier metro density-adjusted total yield'] / df['Metro density-adjusted total yield']

        return df

    def _add_share(self, existing_df, airport_df, col):
        airport_df['Airport'] = airport_df[col]
        col = 'Origin' if col == 'ORIGIN' else 'Dest' if col == 'DEST' else 'Dest metro' if col == 'Destination metro' else 'Origin metro'
        airport_df.drop(columns=['ORIGIN', 'DEST', 'Origin metro', 'Destination metro'], axis=1, inplace=True)
        airport_df['NONSTOP_MILES'] = airport_df['NONSTOP_MILES'] * airport_df['Pax/day']
        df = airport_df.groupby(['Airport', 'TICKET_CARRIER'], as_index=False).sum()
        df[f'Carrier {col} pax/day'] = df['Pax/day']
        df[f'Carrier {col} miles/pax'] = df['NONSTOP_MILES'] / df['Pax/day']
        df[f'Carrier {col} fare/pax'] = df['Revenue/day'] / df['Pax/day']
        df[f'Carrier {col} total fare/pax'] = df['Total revenue/day'] / df['Pax/day']
        df[f'Carrier {col} total adj fare/pax'] = df['Total revenue/day'] / df['Adj pax/day']
        df[f'Carrier {col} yield'] = df[f'Carrier {col} fare/pax'] / df[f'Carrier {col} miles/pax']
        df[f'Carrier {col} total yield'] = df[f'Carrier {col} total fare/pax'] / df[f'Carrier {col} miles/pax']
        df[f'Carrier {col} adj total yield'] = df[f'Carrier {col} total adj fare/pax'] / df[f'Carrier {col} miles/pax']
        df.drop(f'Carrier {col} miles/pax', axis=1, inplace=True)
        df = self._add_share_data_subset(df, df.copy(), col, col)
        df = self._add_share_data_subset(df, df[~df['TICKET_CARRIER'].isin(self._configuration['ULCCs'])].copy(), col, f'{col} exc. ULCC')
        df.drop(columns=['NONSTOP_MILES', 'Pax/day', 'Adj pax/day', 'Revenue/day', 'Total revenue/day'], inplace=True)
        col_original_name = 'ORIGIN' if col == 'Origin' else 'DEST' if col == 'Dest' else 'Destination metro' if col == 'Dest metro' else 'Origin metro'
        df.rename(columns={'Airport': col_original_name}, inplace=True)
        df = existing_df.merge(df, on=[col_original_name, 'TICKET_CARRIER'])
        return df

    @staticmethod
    def _add_share_data_subset(df, df_market, col, output_col):
        df_market_original = df_market.copy()
        df_market = df_market[['Airport', 'NONSTOP_MILES', 'Revenue/day', 'Total revenue/day', 'Pax/day', 'Adj pax/day']]
        df_market = df_market.groupby('Airport', as_index=False).sum()
        df_market[f'{output_col} miles/pax'] = df_market['NONSTOP_MILES'] / df_market['Pax/day']
        df_market[f'{output_col} fare/pax'] = df_market['Revenue/day'] / df_market['Pax/day']
        df_market[f'{output_col} total fare/pax'] = df_market['Total revenue/day'] / df_market['Pax/day']
        df_market[f'{output_col} adj total fare/pax'] = df_market['Total revenue/day'] / df_market['Adj pax/day']
        df_market[f'{output_col} yield'] = df_market[f'{output_col} fare/pax'] / df_market[f'{output_col} miles/pax']
        df_market[f'{output_col} total yield'] = df_market[f'{output_col} total fare/pax'] / df_market[f'{output_col} miles/pax']
        df_market[f'{output_col} adj total yield'] = df_market[f'{output_col} adj total fare/pax'] / df_market[f'{output_col} miles/pax']
        df_market.drop(columns=['NONSTOP_MILES', f'{output_col} miles/pax'], axis=1, inplace=True)
        df_market.rename(columns={'Pax/day': f'{output_col} pax/day', 'Adj pax/day': f'{output_col} adj pax/day', 'Revenue/day': f'{output_col} revenue/day', 'Total revenue/day': f'{output_col} total revenue/day'}, inplace=True)
        df_market_original = df_market_original[['Airport', 'TICKET_CARRIER']]
        df_market = df_market_original.merge(df_market, on='Airport', how='left')
        df = df.merge(df_market, on=['Airport', 'TICKET_CARRIER'], how='left')
        df[f'{output_col} market share'] = df['Pax/day'] / df[f'{output_col} pax/day']
        df[f'Carrier {output_col} yield premium'] = df[f'Carrier {col} yield'] / df[f'{output_col} yield']
        df[f'Carrier {output_col} total yield premium'] = df[f'Carrier {col} total yield'] / df[f'{output_col} total yield']
        df[f'Carrier {output_col} total flight yield premium'] = df[f'Carrier {col} adj total yield'] / df[f'{output_col} adj total yield']
        df.drop(columns=[f'{output_col} adj pax/day', f'{output_col} revenue/day', f'{output_col} total revenue/day'], inplace=True)
        df.fillna(0, inplace=True)
        return df

    def _add_shares(self, existing_df, data_df):
        df = self._add_share(existing_df, data_df.copy(), 'ORIGIN')
        df = self._add_share(df, data_df.copy(), 'DEST')
        df = self._add_share(df, data_df.copy(), 'Origin metro')
        df = self._add_share(df, data_df.copy(), 'Destination metro')
        return df

    def _add_to_analysis_length(self, year, quarter):
        self._analysis_length += self._timeframe_length(year, quarter)

    def _ancillary_revenue(self, carrier):
        return self._configuration['Ancillary revenue per passenger'].get(carrier, self._configuration['Ancillary revenue per passenger']['Default'])

    @staticmethod
    def _consolidate_data_file(df):
        return df.groupby(['ORIGIN', 'DEST', 'TICKET_CARRIER', 'NONSTOP_MILES'], as_index=False).sum()

    def _density_bonus(self, carrier):
        return 1 + self._configuration['Extra seats'].get(carrier, self._configuration['Extra seats']['Default'])

    def _distance_bucket(self, distance):
        return self._configuration['Distance bucket size'] * self._divide_and_drop_remainder(distance, self._configuration['Distance bucket size']) + self._configuration['Distance bucket size'] / 2

    @staticmethod
    def _divide_and_drop_remainder(dividend, divisor):
        remainder = dividend % divisor
        return (dividend - remainder) / divisor

    def _filter_at_end(self, df):
        share_filter = self._configuration['Filters at end'].get('Metro market share', 0)
        share_filter = share_filter if share_filter < 1 else share_filter / 100.0
        df = df[df['Metro share'] >= share_filter]
        df = df[df['Pax/day'] >= self._configuration['Filters at end'].get('Market carrier pax/day', 0)]
        df = df[df['Metro pax/day'] >= self._configuration['Filters at end'].get('Metro pax/day', 0)]
        df.drop(columns=['Revenue/day', 'Total revenue/day'], axis=1, inplace=True)
        return df

    def _filter_at_beginning(self, df):
        return df[~df['TICKET_CARRIER'].isin(self._configuration['Filters at beginning']['Invalid carriers'])]

    def _filter_for_share(self, df):
        df = df[df['Pax/day'] >= self._configuration['Filters at beginning'].get('Market carrier pax/day', 0)]

        new_df_len = len(df)
        df_len = new_df_len + 1

        while df_len > new_df_len:
            df_market = df.copy()[['ORIGIN', 'DEST', 'Pax/day']]
            df_market = df_market.groupby(['ORIGIN', 'DEST'], as_index=False).sum()
            df_market.rename(columns={'Pax/day': 'Market pax/day'}, inplace=True)
            df = df.merge(df_market, on=['ORIGIN', 'DEST'])
            df = df[df['Market pax/day'] >= self._configuration['Filters at beginning'].get('Market pax/day', 0)]
            df.drop(columns=['Market pax/day'], axis=1, inplace=True)

            df_metro = df.copy()[['Origin metro', 'Destination metro', 'Pax/day']]
            df_metro_carrier = df_metro.copy().groupby(['Origin metro', 'Destination metro'], as_index=False).sum()
            df_metro_carrier.rename(columns={'Pax/day': 'Metro pax/day'}, inplace=True)
            df = df.merge(df_metro_carrier, on=['Origin metro', 'Destination metro'])
            df = df[df['Metro pax/day'] >= self._configuration['Filters at beginning'].get('Metro pax/day', 0)]
            df.drop(columns=['Metro pax/day'], axis=1, inplace=True)

            df_market = df.copy()[['ORIGIN', 'DEST', 'Pax/day']]
            df_market = df_market.groupby(['ORIGIN', 'DEST'], as_index=False).sum()
            df_market.rename(columns={'Pax/day': 'Market pax/day'}, inplace=True)
            df = df.merge(df_market, on=['ORIGIN', 'DEST'])
            df['Market share'] = df['Pax/day'] / df['Market pax/day']
            df.drop(columns=['Market pax/day'], axis=1, inplace=True)

            df_metro = df.copy()[['Origin metro', 'Destination metro', 'TICKET_CARRIER', 'Pax/day']]
            df_metro_carrier = df_metro.copy().groupby(['Origin metro', 'Destination metro', 'TICKET_CARRIER'], as_index=False).sum()
            df_metro_carrier.rename(columns={'Pax/day': 'Carrier metro pax/day'}, inplace=True)
            df = df.merge(df_metro_carrier, on=['Origin metro', 'Destination metro', 'TICKET_CARRIER'])

            df_metro.drop(columns=['TICKET_CARRIER'], axis=1, inplace=True)
            df_metro = df_metro.groupby(['Origin metro', 'Destination metro'], as_index=False).sum()
            df_metro.rename(columns={'Pax/day': 'Metro pax/day'}, inplace=True)
            df = df.merge(df_metro, on=['Origin metro', 'Destination metro'])
            df['Metro share'] = df['Carrier metro pax/day'] / df['Metro pax/day']
            df.drop(columns=['Metro pax/day', 'Carrier metro pax/day'], axis=1, inplace=True)

            df = df[((df['Market share'] >= self._configuration['Filters at beginning'].get('Market share', 0)) |
                    (df['Pax/day'] > self._configuration['Filters at beginning']['Do not filter if'].get('Market carrier pax/day', 10000)))]
            df = df[((df['Metro share'] >= self._configuration['Filters at beginning'].get('Metro share', 0)) |
                     (df['Pax/day'] > self._configuration['Filters at beginning']['Do not filter if'].get('Market carrier pax/day', 10000)))]
            df.drop(columns=['Metro share', 'Market share'], axis=1, inplace=True)

            df_len = new_df_len
            new_df_len = len(df)

        return df

    def _get_data_file(self, input_path):
        df = pd.read_csv(input_path)
        df = df[['YEAR', 'QUARTER', 'ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'NONSTOP_MILES']]
        self._add_to_analysis_length(df['YEAR'][0], df['QUARTER'][0])
        df = self._filter_at_beginning(df)
        self._validate_data_file(df)
        df = df[['ORIGIN', 'DEST', 'TICKET_CARRIER', 'PASSENGERS', 'MARKET_FARE', 'NONSTOP_MILES']]
        df = self._consolidate_data_file(df)
        return df

    def _get_fresh_data(self):
        self._full_df = pd.concat([self._get_data_file(df) for df in self._input_paths])
        self._full_df = self._consolidate_data_file(self._full_df)
        self._full_df['Pax/day'] = self._full_df['PASSENGERS'] / (0.1 * self._analysis_length)  # Data is a 10% sample
        self._full_df['Adj pax/day'] = self._full_df['Pax/day'] / self._full_df['TICKET_CARRIER'].apply(self._density_bonus)
        self._full_df['Revenue/day'] = self._full_df['MARKET_FARE'] / (0.1 * self._analysis_length)
        self._full_df['Ancillary revenue'] = self._full_df['TICKET_CARRIER'].apply(self._ancillary_revenue)
        self._full_df['Total revenue/day'] = self._full_df['Revenue/day'] + self._full_df['Ancillary revenue'] * self._full_df['Pax/day']
        self._full_df.drop('PASSENGERS', axis=1, inplace=True)
        self._full_df.drop('MARKET_FARE', axis=1, inplace=True)
        self._full_df.drop('Ancillary revenue', axis=1, inplace=True)
        self._full_df['Origin metro'] = self._full_df['ORIGIN'].apply(self._metro)
        self._full_df['Destination metro'] = self._full_df['DEST'].apply(self._metro)
        self._filtered_df = self._filter_for_share(self._full_df.copy())

    def _load_configuration(self):
        try:
            with open('./configuration.json') as f:
                self._configuration = json.load(f)
        except FileNotFoundError:
            with open('./configuration.example.json') as f:
                self._configuration = json.load(f)

        self._configuration['Airport metros'] = dict()
        for metro, airports in self._configuration['Metro areas'].items():
            for airport in airports:
                self._configuration['Airport metros'][airport] = metro

    def _metro(self, airport):
        return self._configuration['Airport metros'].get(airport, airport)

    @staticmethod
    def _reorder_output_columns(df):
        df = df.rename(columns={
            'ORIGIN': 'Origin',
            'DEST': 'Destination',
            'NONSTOP_MILES': 'Nonstop miles',
            'TICKET_CARRIER': 'Carrier',
            'Pax/day': 'Carrier pax/day',
            'Fare/pax': 'Carrier fare/pax',
            'Total fare/pax': 'Carrier total fare/pax',
            'Yield': 'Carrier yield',
            'Total yield': 'Carrier total yield',
            'Market fare premium': 'Carrier market fare premium',
            'Market total fare premium': 'Carrier market total fare premium',
            'Market total flight premium': 'Carrier market total flight premium',
            'Metro fare premium': 'Carrier metro fare premium',
            'Metro total fare premium': 'Carrier metro total fare premium',
            'Metro total flight premium': 'Carrier metro total flight premium',
            'Market share': 'Carrier market share',
            'Metro share': 'Carrier metro share',
            'Origin market share': 'Carrier origin market share',
            'Dest market share': 'Carrier destination market share',
            'Origin metro market share': 'Carrier origin metro share',
            'Dest metro market share': 'Carrier destination metro share',
        })
        df.sort_values(['Origin metro', 'Destination metro', 'Carrier', 'Origin', 'Destination'], inplace=True)
        return df[[
            'Origin metro',
            'Destination metro',
            'Origin',
            'Destination',
            'Carrier',
            'Carrier pax/day',
            'Market pax/day',
            'Carrier market share',
            'Carrier metro pax/day',
            'Metro pax/day',
            'Carrier metro share',
            'Carrier fare/pax',
            'Carrier total fare/pax',
            'Market total fare/pax',
            'Carrier market total fare premium',
            'Carrier market total flight premium',
            'Metro total fare/pax',
            'Carrier metro total fare premium',
            'Carrier metro total flight premium',
            'Carrier total yield',
            'Market total yield',
            'Carrier metro total yield',
            'Metro total yield',
            'Origin pax/day',
            'Origin metro pax/day',
            'Carrier Origin pax/day',
            'Carrier origin market share',
            'Carrier origin metro share',
            'Carrier Origin total yield premium',
            'Carrier Origin total flight yield premium',
            'Carrier Origin exc. ULCC total yield premium',
            'Carrier Origin exc. ULCC total flight yield premium',
            'Carrier Origin metro total yield premium',
            'Carrier Origin metro total flight yield premium',
            'Carrier Origin metro exc. ULCC total yield premium',
            'Carrier Origin metro exc. ULCC total flight yield premium',
            'Origin metro total yield',
            'Dest metro total yield',
            'Nonstop miles',
            'Yield miles (1000)',
            'Distance bucket',
            'Distance bucket total yield',
            'Distance total yield premium',
            'Distance total flight yield premium',
            'Market\'s distance total yield premium',
            'Carrier origin distance total flight yield premium',
            'Carrier origin average yield premium',
            'Metro distance bucket',
            'Metro distance bucket total yield',
            'Metro distance total yield premium',
            'Metro distance total flight yield premium',
            'Metro\'s distance total yield premium',
        ]]

    @staticmethod
    def _timeframe_length(year, quarter):
        if quarter == 1 and year % 4 == 0:
            return 31 + 29 + 31
        elif quarter == 1:
            return 31 + 28 + 31
        elif quarter == 2:
            return 30 + 31 + 30
        elif quarter == 3:
            return 31 + 31 + 30
        #Q4
        return 31 + 30 + 31

    def _validate_data_file(self, original_df):
        df = original_df.copy()
        year = df['YEAR'].iloc[0]
        quarter = df['QUARTER'].iloc[0]
        df['Pax/day'] = df['PASSENGERS'] / (0.1 * self._timeframe_length(year, quarter))
        df = df[['ORIGIN', 'DEST', 'Pax/day']]
        df = df.groupby(['ORIGIN', 'DEST'], as_index=False).sum()
        df_left = df.copy()
        df = df[df['ORIGIN'] < df['DEST']]
        df_left = df_left[df_left['DEST'] < df_left['ORIGIN']]
        df['Original route name'] = df['ORIGIN'] + '-' + df['DEST']
        df_left['Original route name'] = df_left['DEST'] + '-' + df_left['ORIGIN']
        df = df.merge(df_left, on='Original route name', how='outer')
        df.fillna(0, inplace=True)
        df.rename(columns={'Pax/day_x': 'Right', 'Pax/day_y': 'Left'}, inplace=True)
        df = df[['Original route name', 'Right', 'Left']]
        df['Diff'] = df['Right'] - df['Left']
        df['Percent diff'] = df['Right'] / df['Left'] - 1
        max_diff = self._configuration['Passenger flow validation']['Quantity different']
        max_pct_diff = self._configuration['Passenger flow validation']['Percent different']
        max_pct_diff = max_pct_diff if -1 < max_pct_diff < 1 else max_pct_diff / 100.0
        df_concerning = pd.concat([df[df['Diff'] > max_diff], df[df['Diff'] < -max_diff]])
        df_concerning = pd.concat([df_concerning[df_concerning['Percent diff'] > max_pct_diff], df_concerning[df_concerning['Percent diff'] < -max_pct_diff]])
        print(f'Found {len(df_concerning)} routes in Q{quarter} {year} file with concerningly uneven passenger flows '
              f'({round(len(df_concerning)/len(df)*100,2)}%)')


def main():
    db1b = DB1B(sys.argv[1], sys.argv[2:])
    db1b.enrich()


if __name__ == '__main__':
    main()
