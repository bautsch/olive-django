import pandas as pd
from olive.utils import *


class TypeCurves():
    def __init__(self, tc_file, owner):
        print('loading type curves')
        self.version = pd.Timestamp.utcnow()
        self.owner = owner
        self.type_curves = pd.read_csv(tc_file, dtype={'name': str, 'time_on': int,
                                                       'gas': float, 'oil': float, 'water': float},
                                       names=['name', 'time_on', 'gas', 'oil', 'water'], header=0)
        self.type_curves['version'] = self.version
        self.type_curves['owner'] = self.owner
        save_type_curves(self)


class ProducingCurves():
    def __init__(self, pdp_file, owner, scenario):
        print('loading PDP curves')
        self.scenario = scenario
        self.version = pd.Timestamp.utcnow()
        self.owner = owner
        self.pdp_curves = pd.read_csv(pdp_file, header=0)
        daily = False
        if 'Gas (MCF/D)' in self.pdp_curves.columns:
            self.pdp_curves.rename(columns={'Chosen ID': 'idp', 'Date': 'prod_date', 'Gas (MCF/D)': 'gas', 
                                            'Oil (BBL/D)': 'oil', 'Water (BBL/D)': 'water'}, inplace=True)
            daily = True
        if 'Gas Forecast (MCF/D)' in self.pdp_curves.columns:
            self.pdp_curves.rename(columns={'Chosen ID': 'idp', 'Date': 'prod_date', 'Gas Forecast (MCF/D)': 'gas', 
                                            'Oil Forecast (BBL/D)': 'oil', 'Water Forecast (BBL/D)': 'water'}, inplace=True)
            print('production included')
            self.pdp_curves.loc[:, 'prod_date'] = self.pdp_curves.prod_date.astype('datetime64[ns]')
            self.pdp_curves.sort_values(by=['idp', 'prod_date'], inplace=True)
            for p in self.pdp_curves.idp.unique():
                prod_start = self.pdp_curves.loc[self.pdp_curves.idp == p, 'Oil Production (BBL/D)'].notna().idxmax()
                prod_end = self.pdp_curves.loc[self.pdp_curves.idp == p, 'Oil Production (BBL/D)'].notna()[::-1].idxmax()
                self.pdp_curves.loc[prod_start:prod_end, 'oil'] = self.pdp_curves.loc[prod_start:prod_end, 'Oil Production (BBL/D)']

                prod_start = self.pdp_curves.loc[self.pdp_curves.idp == p, 'Gas Production (MCF/D)'].notna().idxmax()
                prod_end = self.pdp_curves.loc[self.pdp_curves.idp == p, 'Gas Production (MCF/D)'].notna()[::-1].idxmax()
                self.pdp_curves.loc[prod_start:prod_end, 'gas'] = self.pdp_curves.loc[prod_start:prod_end, 'Gas Production (MCF/D)']

                prod_start = self.pdp_curves.loc[self.pdp_curves.idp == p, 'Water Production (BBL/D)'].notna().idxmax()
                prod_end = self.pdp_curves.loc[self.pdp_curves.idp == p, 'Water Production (BBL/D)'].notna()[::-1].idxmax()
                self.pdp_curves.loc[prod_start:prod_end, 'water'] = self.pdp_curves.loc[prod_start:prod_end, 'Water Production (BBL/D)']
            daily = True
        if daily:
            print('daily forecasts')
            self.pdp_curves['scenario'] = self.scenario
            self.pdp_curves['version'] = self.version
            self.pdp_curves['owner'] = self.owner
            self.pdp_curves['prod_date'] = self.pdp_curves['prod_date'].astype('datetime64[ns]')
            self.pdp_curves = self.pdp_curves[['scenario', 'idp', 'prod_date', 'gas', 'oil', 'water', 'version', 'owner']]
            self.pdp_curves.fillna(0, inplace=True)
        else:
            self.pdp_curves = convert_to_daily(self, self.pdp_curves)
        save_pdp_curves(self)