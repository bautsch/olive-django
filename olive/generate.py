from olive.utils import *
from olive.models import output_onelines
import numpy as np
import pandas as pd
import calendar
from datetime import datetime
import seaborn as sns
from matplotlib import pyplot as plt
import matplotlib.ticker as mtick
from django.db.models import Max
np.seterr(all='raise')

def cash_flows(args):

    build = args[0]
    property_list = args[1]
    run_type = args[2]
    sim = args[3]
    num_properties = len(property_list)
    num_days = len(build.date_range)
    if num_properties > 50:
        chunks = [property_list[i:i + 50] for i in range(0, num_properties, 50)]
    else:
        chunks = [property_list]
    
    chunk_check = [len(ch) for ch in chunks]
    print(num_properties, sum(chunk_check), chunk_check)

    for ch_num, chunk in enumerate(chunks):
        print(ch_num, 'of', len(chunks), ':', len(chunk))
        property_list = chunk
        num_properties = len(property_list)

        columns=['scenario', 'idp', 'prod_date', 'budget_type',
                'name', 'short_pad', 'pad',
                'rig', 'field', 'time_on',
                'gross_gas_mult', 'gross_oil_mult', 'gross_water_mult',
                'gas_eur', 'oil_eur',
                'gross_gas', 'gross_oil', 'gross_water',
                'ngl_yield', 'btu', 'shrink', 'wi', 'nri', 'royalty',
                'net_gas', 'net_oil', 'net_ngl', 'net_mcfe',
                'royalty_gas', 'royalty_oil', 'royalty_ngl', 'royalty_mcfe',
                'hh', 'wti', 'ngl', 'cig', 'nwr', 'gas_basis',
                'gas_price_adj', 'gas_adj_unit',
                'oil_price_adj', 'oil_adj_unit',
                'ngl_price_adj', 'ngl_adj_unit', 'realized_gas_price',
                'realized_oil_price', 'realized_ngl_price',
                'net_gas_rev', 'net_oil_rev',
                'net_ngl_rev', 'net_total_rev',
                'gross_drill_capex', 'gross_compl_capex',
                'gross_misc_capex', 'gross_aban_capex', 'gross_total_capex',
                'net_drill_capex', 'net_compl_capex', 'net_misc_capex', 
                'net_aban_capex', 'net_total_capex',
                'fixed_cost', 'alloc_fixed_cost', 'var_gas_cost', 'var_oil_cost',
                'var_water_cost', 'doe', 'gtp',
                'taxes', 'loe', 'cf', 'fcf', 'pv1', 'pv1_rate',
                'pv2', 'pv2_rate', 'pv3', 'pv3_rate', 'pv4', 'pv4_rate',
                'pv5', 'pv5_rate', 'pv6', 'pv6_rate', 'pv7', 'pv7_rate',
                'pv8', 'pv8_rate', 'pv9', 'pv9_rate', 'pv10', 'pv10_rate', 'active',
                'owner', 'version', 'type', 'simulation']

        df = {}
        for c in columns:
            if c in ('scenario', 'idp', 'name', 'budget_type',
                        'short_pad', 'pad', 'rig', 'field', 'type'):
                df[c] = np.empty(num_properties*num_days, dtype='object')
            elif c in ('gas_basis', 'gas_adj_unit', 'oil_adj_unit', 'ngl_adj_unit'):
                df[c] = np.empty(num_properties*num_days, dtype='object')
                df[c][:] = 'none'
            elif c == 'prod_date':
                df[c] = np.empty(num_properties*num_days, dtype='datetime64[ns]')
            elif c == 'owner':
                df[c] = [build.owner] * (num_properties*num_days)
            elif c == 'version':
                df[c] = [build.version] * (num_properties*num_days)
            elif c == 'simulation':
                df[c] = [sim] * (num_properties*num_days)
            else:
                df[c] = np.zeros(num_properties*num_days, dtype='float')

        risk_uncertainty = {}

        for n, i in enumerate(property_list):
            idx = n * num_days

            f = build.forecasts[build.forecasts.idp == i]
            e = build.economics[build.economics.idp == i]
            p = build.properties[build.properties.idp == i]
            s = build.schedule_dates[build.schedule_dates.idp == i]

            budget_type = p['budget_type'].values[0]
            forecast_type = f['forecast_type'].values[0]

            risk_uncertainty[i] = {}
            u = build.uncertainty.loc[build.uncertainty.idp == i]
            r = build.risk.loc[build.risk.idp == i]

            if r.performance.values[0] is None:
                tc_mult = u.performance.values[0]
            else:
                tc_mult = u.performance.values[0] * r.performance.values[0]
            if r.profile.values[0] is None:
                ip_mult = None
            else:
                ip_mult = u.profile.values[0] * r.profile.values[0]
            if r.drill_cost.values[0] is None:
                drill_mult = u.drill_cost.values[0]
            else:
                drill_mult = r.drill_cost.values[0]
            if r.complete_cost.values[0] is None:
                complete_mult = u.complete_cost.values[0]
            else:
                complete_mult = r.complete_cost.values[0]

            if r.in_zone.values[0] is not None:
                tc_mult = tc_mult * r.in_zone.values[0]

            if r.wellbore.values[0] is not None:
                tc_mult = tc_mult * r.wellbore.values[0]

            gas_price_mult = u.gas_price.values[0]
            oil_price_mult = u.oil_price.values[0]
            ngl_price_mult = u.ngl_price.values[0]
            btu_mult = u.btu.values[0]
            shrink_mult = u.shrink.values[0]
            ngl_yield_mult = u.ngl_yield.values[0]
            doe_mult = u.doe.values[0]
            gtp_mult = u.gtp.values[0]
            total_capex_mult = u.total_capex.values[0]
            infra_cost = u.infra_cost.values[0]
            curtailment = r.curtailment.values[0]
            frac_hit = r.frac_hit.values[0]
            spacing = r.spacing.values[0]
            abandon = r.abandon.values[0]
            downtime = r.downtime.values[0]
            duration = r.duration.values[0]
            frequency = r.frequency.values[0]
            downtime_mult = r.downtime_mult.values[0]
            downtime_cost = r.downtime_cost.values[0]
            delay = r.delay.values[0]

            if not pd.isna(p['first_prod_date'].values[0]):
                prod_start_date = pd.Timestamp(p['first_prod_date'].values[0])
            elif len(s) > 0:
                prod_start_date = pd.Timestamp(s['prod_start_date'].values[0])
            elif not pd.isna(load_pdp_curve(f)['prod_date'].min()):
                prod_start_date = pd.Timestamp(load_pdp_curve(f)['prod_date'].min())
            else:
                print('no start date')
                raise ValueError

            if not pd.isna(p['drill_start_date'].values[0]):
                drill_start_date = pd.Timestamp(p['drill_start_date'].values[0])
            elif len(s) > 0:
                drill_start_date = pd.Timestamp(s['drill_start_date'].values[0])
            else:
                drill_start_date = None

            if not pd.isna(p['drill_end_date'].values[0]):
                drill_end_date = pd.Timestamp(p['drill_end_date'].values[0])
            elif len(s) > 0:
                drill_end_date = pd.Timestamp(s['drill_end_date'].values[0])
            else:
                drill_end_date = None

            if not pd.isna(p['compl_start_date'].values[0]):
                compl_start_date = pd.Timestamp(p['compl_start_date'].values[0])
            elif len(s) > 0:
                compl_start_date = pd.Timestamp(s['compl_start_date'].values[0])
            else:
                compl_start_date = None

            if not pd.isna(p['compl_end_date'].values[0]):
                compl_end_date = pd.Timestamp(p['compl_end_date'].values[0])
            elif len(s) > 0:
                compl_end_date = pd.Timestamp(s['compl_end_date'].values[0])
            else:
                compl_end_date = None

            if drill_start_date is not None and drill_end_date is not None:
                if drill_start_date > build.end_date or drill_end_date < build.effective_date:
                    drill = False
                else:
                    drill = True
            else:
                drill = False

            if compl_start_date is not None and compl_end_date is not None:
                if (compl_start_date > build.end_date) or (compl_end_date < build.effective_date):
                    complete = False
                else:
                    complete = True
            else:
                complete = False

            if i in build.schedule_dates['idp']:
                pad = p['pad'].values[0]
                rig = build.schedule_file[build.schedule_file['pad'] == pad].rig.values[0]
            else:
                rig = 'base'

            if delay:
                prod_start_date += relativedelta(days=delay)

            if prod_start_date >= build.end_date:
                continue
            if prod_start_date < build.effective_date:
                t_start = (build.effective_date - prod_start_date).days
                prod_start_date = build.effective_date
            elif prod_start_date >= build.effective_date:
                t_start = 0
            t_end = (build.end_date - prod_start_date).days + 5

            if 'type' in forecast_type:
                forecast = load_type_curve(f, t_start, t_end)
                min_date = prod_start_date
                prod = load_production(i, prod_start_date)
                if ':' in forecast_type:
                    if len(prod) > 5 and sum(prod.Gas) > 0:
                        ratios = [float(r) for r in forecast_type.split(':')[1:]]
                        ratio = np.divide(prod.Gas.values,
                                          forecast.loc[:len(prod.Gas.values)-1, 'gas'].values).mean()

                        if ratio > 0:
                            ratio = min(ratios[1], ratio)
                            ratio = max(ratios[0], ratio)

                        forecast.loc[:, 'gas'] = forecast.loc[:, 'gas'].multiply(ratio)
                        forecast.loc[:, 'oil'] = forecast.loc[:, 'oil'].multiply(ratio)
                        forecast.loc[:, 'water'] = forecast.loc[:, 'water'].multiply(ratio)

                if len(prod) > 0 and sum(prod.Gas) > 0:
                    forecast.loc[:len(prod)-1, 'gas'] = prod.Gas.values
                    forecast.loc[:len(prod)-1, 'oil'] = prod.Oil.values
                    forecast.loc[:len(prod)-1, 'water'] = prod.Water.values
            else:
                forecast = load_pdp_curve(f, prod_start_date, build.end_date)
                min_date = pd.Timestamp(forecast.prod_date.min())
            time_on = np.arange(t_start+1, len(forecast)+1)

            if min_date > build.effective_date:
                date_delta = (min_date - build.effective_date).days
                padding = padding_df(forecast, date_delta)
                padding.loc[:, 'prod_date'] = pd.date_range(start=build.effective_date, periods=date_delta)
                forecast = pd.concat([padding, forecast])
                time_on = np.concatenate([np.zeros(len(padding)), time_on])

            if forecast.shape[0] < num_days:
                padding = padding_df(forecast, num_days - forecast.shape[0])
                forecast = pd.concat([forecast, padding])
                time_on = np.concatenate([time_on, np.zeros(len(padding))])
            
            if forecast.shape[0] > num_days:
                forecast = forecast.iloc[:num_days]
                time_on = time_on[:num_days]

            df['scenario'][idx:idx+num_days] = [build.name] * num_days
            df['idp'][idx:idx+num_days] = [i] * num_days
            df['budget_type'][idx:idx+num_days] = [budget_type] * num_days
            df['prod_date'][idx:idx+num_days] = build.date_range.values
            df['name'][idx:idx+num_days] = [p['name'].values[0]] * num_days
            df['short_pad'][idx:idx+num_days] = [p['short_pad'].values[0]] * num_days
            df['pad'][idx:idx+num_days] = [p['pad'].values[0]] * num_days
            df['field'][idx:idx+num_days] = [p['field'].values[0]] * num_days
            df['rig'][idx:idx+num_days] = [rig] * num_days
            df['type'][idx:idx+num_days] = [run_type] * num_days

            df['gross_gas'][idx:idx+num_days] = forecast.gas.values * tc_mult
            if i == 'S9FE2RHSET':
                forecast.to_csv('forecast.csv')
            df['gross_oil'][idx:idx+num_days] = forecast.oil.values * tc_mult
            df['gross_water'][idx:idx+num_days] = forecast.water.values
            df['time_on'][idx:idx+num_days] = time_on

            if ip_mult and ip_mult < 1.0:
                df['gross_gas'][idx:idx+num_days] = apply_ip_adjust(ip_mult, df['gross_gas'][idx:idx+num_days])            
                df['gross_oil'][idx:idx+num_days] = apply_ip_adjust(ip_mult, df['gross_oil'][idx:idx+num_days])
            
            if curtailment and curtailment < 1.0:
                df['gross_gas'][idx:idx+num_days] = apply_curtailment(curtailment, df['gross_gas'][idx:idx+num_days])            
                df['gross_oil'][idx:idx+num_days] = apply_curtailment(curtailment, df['gross_oil'][idx:idx+num_days])

            if spacing and spacing < 1.0:
                df['gross_gas'][idx:idx+num_days] = apply_curtailment(spacing, df['gross_gas'][idx:idx+num_days])            
                df['gross_oil'][idx:idx+num_days] = apply_curtailment(spacing, df['gross_oil'][idx:idx+num_days])

            if frac_hit:
                df['gross_gas'][idx:idx+num_days] = df['gross_gas'][idx:idx+num_days] * frac_hit    
                df['gross_oil'][idx:idx+num_days] = df['gross_oil'][idx:idx+num_days] * frac_hit

            if downtime:
                mask = event_list(frequency, duration, time_on)
                df['gross_gas'][idx:idx+num_days][mask] = df['gross_gas'][idx:idx+num_days][mask] * downtime_mult
                df['gross_oil'][idx:idx+num_days][mask] = df['gross_oil'][idx:idx+num_days][mask] * downtime_mult

            df['hh'][idx:idx+num_days] = build.price_deck.hh.values * gas_price_mult
            df['wti'][idx:idx+num_days] = build.price_deck.wti.values * oil_price_mult
            df['ngl'][idx:idx+num_days] = build.price_deck.ngl.values * ngl_price_mult
            df['cig'][idx:idx+num_days] = build.price_deck.cig.values * ngl_price_mult
            df['nwr'][idx:idx+num_days] = build.price_deck.nwr.values * ngl_price_mult

            df['gas_basis'][idx:idx+num_days] = [e['gas_basis'].values[0]] * num_days

            inputs = {}
            for c in e.columns:
                if c == 'inv_g_misc':
                    inputs[c] = misc_capex_parser(e[c].values[0], build.effective_date, build.end_date)
                elif c in ('inv_g_drill', 'inv_g_compl', 'inv_g_aban', 'minimum_life', 'id',
                        'owner', 'version', 'scenario', 'gas_basis', 'idp', 'type', 'simulation'):
                    continue
                elif c in ('wi_frac', 'nri_frac', 'roy_frac', 'gross_gas_mult', 'gross_oil_mult', 'gross_water_mult'):
                    inputs[c] = econ_parser(c, e[c].values[0], build.effective_date,
                                            build.effective_date, build.end_date)
                else:
                    if c in ('ngl_g_bpmm', 'cost_gtp', 'price_adj_gas', 'price_adj_oil',
                                'price_adj_ngl', 'cost_fixed', 'cost_fixed_alloc', 'shrink', 'btu'):
                        inputs[c] = econ_parser(c, e[c].values[0], build.effective_date,
                                                prod_start_date, build.end_date)
                    else:
                        if drill_start_date is not None:
                            inputs[c] = econ_parser(c, e[c].values[0], build.effective_date,
                                                    drill_start_date,
                                                    build.end_date)
                        else:
                            inputs[c] = econ_parser(c, e[c].values[0], build.effective_date,
                                                    prod_start_date,
                                                    build.end_date)    

            df['gross_gas_mult'][idx:idx+num_days] = inputs['gross_gas_mult'].gross_gas_mult.values
            df['gross_oil_mult'][idx:idx+num_days] = inputs['gross_oil_mult'].gross_oil_mult.values
            df['gross_water_mult'][idx:idx+num_days] = inputs['gross_water_mult'].gross_water_mult.values                

            df['gross_gas'][idx:idx+num_days] = df['gross_gas'][idx:idx+num_days] * df['gross_gas_mult'][idx:idx+num_days]
            df['gross_oil'][idx:idx+num_days] = df['gross_oil'][idx:idx+num_days] * df['gross_oil_mult'][idx:idx+num_days]
            df['gross_water'][idx:idx+num_days] = df['gross_water'][idx:idx+num_days] * df['gross_water_mult'][idx:idx+num_days]

            df['gas_eur'][idx:idx+num_days] = df['gross_gas'][idx:idx+num_days]
            df['oil_eur'][idx:idx+num_days] = df['gross_oil'][idx:idx+num_days]

            df['ngl_yield'][idx:idx+num_days] = inputs['ngl_g_bpmm'].ngl_g_bpmm.values * ngl_yield_mult
            df['btu'][idx:idx+num_days] = inputs['btu_factor'].btu_factor.values * btu_mult
            df['shrink'][idx:idx+num_days] = inputs['shrink_factor'].shrink_factor.values * shrink_mult
            
            df['wi'][idx:idx+num_days] = inputs['wi_frac'].wi_frac.values
            df['nri'][idx:idx+num_days] = inputs['nri_frac'].nri_frac.values
            df['royalty'][idx:idx+num_days] = inputs['roy_frac'].roy_frac.values

            df['net_gas'][idx:idx+num_days] = (df['gross_gas'][idx:idx+num_days] * df['nri'][idx:idx+num_days] *
                                                df['shrink'][idx:idx+num_days])
            df['net_oil'][idx:idx+num_days] = df['gross_oil'][idx:idx+num_days] * df['nri'][idx:idx+num_days]
            df['net_ngl'][idx:idx+num_days] = (df['gross_gas'][idx:idx+num_days] * df['ngl_yield'][idx:idx+num_days] *
                                                df['nri'][idx:idx+num_days] / 1000)
            df['net_mcfe'][idx:idx+num_days] = (df['net_gas'][idx:idx+num_days] + df['net_oil'][idx:idx+num_days] * 6 +
                                                df['net_ngl'][idx:idx+num_days] * 6)
            df['royalty_gas'][idx:idx+num_days] = (df['gross_gas'][idx:idx+num_days] * df['royalty'][idx:idx+num_days] *
                                                    df['shrink'][idx:idx+num_days])
            df['royalty_oil'][idx:idx+num_days] = df['gross_oil'][idx:idx+num_days] * df['royalty'][idx:idx+num_days]
            df['royalty_ngl'][idx:idx+num_days] = (df['gross_gas'][idx:idx+num_days] * df['ngl_yield'][idx:idx+num_days] *
                                                    df['royalty'][idx:idx+num_days] / 1000)

            df['gas_price_adj'][idx:idx+num_days] = inputs['price_adj_gas'].price_adj_gas.values
            df['gas_adj_unit'][idx:idx+num_days] = inputs['price_adj_gas'].unit.values

            df['oil_price_adj'][idx:idx+num_days] = inputs['price_adj_oil'].price_adj_oil.values
            df['oil_adj_unit'][idx:idx+num_days] = inputs['price_adj_oil'].unit.values

            df['ngl_price_adj'][idx:idx+num_days] = inputs['price_adj_ngl'].price_adj_ngl.values
            df['ngl_adj_unit'][idx:idx+num_days] = inputs['price_adj_ngl'].unit.values

            df['fixed_cost'][idx:idx+num_days] = (inputs['cost_fixed'].cost_fixed.values /
                                                    inputs['cost_fixed'].eomonth.dt.day.values) * df['wi'][idx:idx+num_days]
            df['alloc_fixed_cost'][idx:idx+num_days] = (inputs['cost_fixed_alloc'].cost_fixed_alloc.values /
                                                    inputs['cost_fixed_alloc'].eomonth.dt.day.values) * df['wi'][idx:idx+num_days]                                                    
            df['var_gas_cost'][idx:idx+num_days] = (inputs['cost_vargas'].cost_vargas.values * df['shrink'][idx:idx+num_days] *
                                                    df['gross_gas'][idx:idx+num_days]) * df['wi'][idx:idx+num_days]
            df['var_oil_cost'][idx:idx+num_days] = (inputs['cost_varoil'].cost_varoil.values *
                                                    df['gross_oil'][idx:idx+num_days]) * df['wi'][idx:idx+num_days]
            df['var_water_cost'][idx:idx+num_days] = (inputs['cost_varwater'].cost_varwater.values *
                                                        df['gross_water'][idx:idx+num_days]) * df['wi'][idx:idx+num_days]
            df['gtp'][idx:idx+num_days] = (inputs['cost_gtp'].cost_gtp.values * df['shrink'][idx:idx+num_days] *
                                            df['gross_gas'][idx:idx+num_days]) * df['wi'][idx:idx+num_days] * gtp_mult

            df['gross_misc_capex'][idx:idx+num_days] = inputs['inv_g_misc'].inv_g_misc.values * infra_cost

            if downtime:
                spend_day = np.argmax(mask)
                df['gross_misc_capex'][idx:idx+num_days][spend_day] = (df['gross_misc_capex'][idx:idx+num_days][spend_day] 
                                                                    + downtime_cost)

            if drill:
                alloc_drill_capex = (e['inv_g_drill'].values[0] * drill_mult) / ((drill_end_date - drill_start_date).days + 1)
                df['gross_drill_capex'][(df['idp'] == i) &
                                        (df['prod_date'] >= np.datetime64(drill_start_date)) &
                                        (df['prod_date'] <= np.datetime64(drill_end_date))] = alloc_drill_capex

            if complete:
                alloc_start_date = compl_start_date.date()
                alloc_end_date = compl_end_date.date()
                if delay:
                    alloc_start_date += relativedelta(days=delay)
                    alloc_end_date += relativedelta(days=delay)
                if abandon:
                    alloc_compl_capex = abandon / ((alloc_end_date - alloc_start_date).days + 1)
                else:

                    alloc_compl_capex = (e['inv_g_compl'].values[0] * complete_mult) / ((alloc_end_date - alloc_start_date).days + 1)
                    if alloc_compl_capex == np.nan:
                        print('nan', i, sim, e['inv_g_compl'].values[0], complete_mult, alloc_end_date, alloc_start_date, (alloc_end_date - alloc_start_date).days)
                    if not alloc_compl_capex > 0:
                        print('!>0', i, sim, e['inv_g_compl'].values[0], complete_mult, alloc_end_date, alloc_start_date, (alloc_end_date - alloc_start_date).days)

                df['gross_compl_capex'][(df['idp'] == i) &
                                        (df['prod_date'] >= np.datetime64(alloc_start_date)) &
                                        (df['prod_date'] <= np.datetime64(alloc_end_date))] = alloc_compl_capex

            idp_mask = (df['idp'] == i)
            gas_pct_adj = (df['gas_adj_unit'] == 'pct')
            gas_pct_mask = np.logical_and(idp_mask, gas_pct_adj)

            df['realized_gas_price'][gas_pct_mask] = (df['hh'][gas_pct_mask] *
                                                        df['gas_price_adj'][gas_pct_mask])

            gas_per_adj = (df['gas_adj_unit'] == 'per')
            gas_per_mask = np.logical_and(idp_mask, gas_per_adj)
            df['realized_gas_price'][gas_per_mask] = (df['hh'][gas_per_mask] +
                                                        df['gas_price_adj'][gas_per_mask])

            if 'cig' in e['gas_basis'].values[0]:
                df['realized_gas_price'][idx:idx+num_days] = (df['realized_gas_price'][idx:idx+num_days] +
                                                            df['cig'][idx:idx+num_days])

            if 'nwr' in e['gas_basis'].values[0]:
                df['realized_gas_price'][idx:idx+num_days] = (df['realized_gas_price'][idx:idx+num_days] +
                                                            df['nwr'][idx:idx+num_days])

            df['realized_gas_price'][idx:idx+num_days] = (df['realized_gas_price'][idx:idx+num_days] *
                                                            df['btu'][idx:idx+num_days])

            oil_pct_adj = (df['oil_adj_unit'] == 'pct')
            oil_pct_mask = np.logical_and(idp_mask, oil_pct_adj)
            df['realized_oil_price'][oil_pct_mask] = (df['wti'][oil_pct_mask] *
                                                        df['oil_price_adj'][oil_pct_mask])

            oil_per_adj = (df['oil_adj_unit'] == 'per')
            oil_per_mask = np.logical_and(idp_mask, oil_per_adj)
            df['realized_oil_price'][oil_per_mask] = (df['wti'][oil_per_mask] +
                                                        df['oil_price_adj'][oil_per_mask])

            ngl_pct_adj = (df['ngl_adj_unit'] == 'pct')
            ngl_pct_mask = np.logical_and(idp_mask, ngl_pct_adj)
            df['realized_ngl_price'][ngl_pct_mask] = (df['ngl'][ngl_pct_mask] *
                                                        df['ngl_price_adj'][ngl_pct_mask])

            ngl_per_adj = (df['ngl_adj_unit'] == 'per')
            ngl_per_mask = np.logical_and(idp_mask, ngl_per_adj)
            df['realized_ngl_price'][ngl_per_mask] = (df['ngl'][ngl_per_mask] +
                                                        df['ngl_price_adj'][ngl_per_mask])

            df['net_gas_rev'][idx:idx+num_days] = ((df['net_gas'][idx:idx+num_days] + 
                                                    df['royalty_gas'][idx:idx+num_days]) * 
                                                    df['realized_gas_price'][idx:idx+num_days])

            df['net_oil_rev'][idx:idx+num_days] = ((df['net_oil'][idx:idx+num_days] + 
                                                    df['royalty_oil'][idx:idx+num_days]) * 
                                                    df['realized_oil_price'][idx:idx+num_days])

            df['net_ngl_rev'][idx:idx+num_days] = ((df['net_ngl'][idx:idx+num_days] + 
                                                    df['royalty_ngl'][idx:idx+num_days]) *
                                                    df['realized_ngl_price'][idx:idx+num_days])

            df['net_total_rev'][idx:idx+num_days] = (df['net_gas_rev'][idx:idx+num_days] + 
                                                        df['net_oil_rev'][idx:idx+num_days] + 
                                                        df['net_ngl_rev'][idx:idx+num_days])

            df['taxes'][idx:idx+num_days] = ((inputs['tax_sev'].tax_sev.values +
                                                    inputs['tax_adval'].tax_adval.values) * 
                                                df['net_total_rev'][idx:idx+num_days])

            df['doe'][idx:idx+num_days] = (df['fixed_cost'][idx:idx+num_days] +
                                            df['alloc_fixed_cost'][idx:idx+num_days] +
                                            df['var_gas_cost'][idx:idx+num_days] +
                                            df['var_oil_cost'][idx:idx+num_days] +
                                            df['var_water_cost'][idx:idx+num_days])

            df['doe'][idx:idx+num_days] = df['doe'][idx:idx+num_days] * doe_mult

            df['loe'][idx:idx+num_days] = (df['doe'][idx:idx+num_days] +
                                            df['gtp'][idx:idx+num_days] +
                                            df['taxes'][idx:idx+num_days])

            df['gross_total_capex'][idx:idx+num_days] = (df['gross_drill_capex'][idx:idx+num_days] +
                                                            df['gross_compl_capex'][idx:idx+num_days] +
                                                            df['gross_misc_capex'][idx:idx+num_days])
            df['net_drill_capex'][idx:idx+num_days] = (df['gross_drill_capex'][idx:idx+num_days] *
                                                        df['wi'][idx:idx+num_days])
            df['net_compl_capex'][idx:idx+num_days] = (df['gross_compl_capex'][idx:idx+num_days] *
                                                        df['wi'][idx:idx+num_days])
            df['net_misc_capex'][idx:idx+num_days] = (df['gross_misc_capex'][idx:idx+num_days] *
                                                        df['wi'][idx:idx+num_days])
            df['net_total_capex'][idx:idx+num_days] = (df['net_drill_capex'][idx:idx+num_days] +
                                                        df['net_compl_capex'][idx:idx+num_days] +
                                                        df['net_misc_capex'][idx:idx+num_days]) * total_capex_mult

            df['cf'][idx:idx+num_days] = (df['net_total_rev'][idx:idx+num_days] - df['loe'][idx:idx+num_days])
            df['fcf'][idx:idx+num_days] = (df['cf'][idx:idx+num_days] - df['net_total_capex'][idx:idx+num_days])

            if budget_type == 'wedge':
                date_mask = (df['prod_date'][idx:idx+num_days] >= np.datetime64(prod_start_date + relativedelta(years=5)))
            else:
                date_mask = (df['prod_date'][idx:idx+num_days] >= np.datetime64(prod_start_date + relativedelta(years=1)))

            min_life_val = e['minimum_life'].values[0]
            first_active_date = np.where(abs(df['fcf'][idx:idx+num_days]) > 0)[0][0]
            first_active_date = df['prod_date'][idx:idx+num_days][first_active_date]
            pmax = np.argmax(df['fcf'][idx:idx+num_days].cumsum())
            if pmax < len(df['fcf'][idx:idx+num_days]):
                pmax_date = df['prod_date'][idx:idx+num_days][pmax]
                min_life = min_life_parser(min_life_val, first_active_date, pmax_date, build.effective_date, build.end_date)
                if all(min_life.min_life.values != 0):
                    end_of_life = build.effective_date
                else:
                    end_of_life = min_life.loc[(min_life.min_life.values != 0), 'prod_date'].index[-1]
                    end_of_life = df['prod_date'][idx:idx+num_days][end_of_life]
                for k in df.keys():
                    if k in ('scenario', 'idp', 'prod_date', 'budget_type', 'hh', 'wti', 'ng', 'nwr', 'cig',
                            'name', 'short_pad', 'pad', 'rig', 'field', 'time_on', 'wi', 'nri', 'gas_eur', 'oil_eur',
                            'gas_price_adj', 'oil_price_adj', 'ngl_price_adj', 'version', 'owner', 'type', 'simulation'):
                        continue
                    elif k == 'active':
                        df[k][idx:idx+num_days] = min_life.min_life.values
                    else:
                        df[k][idx:idx+num_days] = df[k][idx:idx+num_days] * min_life.min_life.values

                aban_val = e['inv_g_aban'].values[0]
                if aban_val is not None:
                    aban = aban_capex_parser(aban_val, end_of_life, build.effective_date, build.end_date)
                    if len(df['gross_aban_capex'][idx:idx+num_days]) == 0:
                        print(p, 'no output data to apply abandonment')
                    elif sum(aban.inv_g_aban.values) > 0:
                        aban_date = aban.loc[(aban.inv_g_aban.values != 0), 'prod_date'].values[0]
                        aban['active'] = 1
                        aban.loc[aban.prod_date > aban_date, 'active'] = 0
                        aban.loc[aban.prod_date < first_active_date, 'active'] = 0
                        wi = df['wi'][idx:idx+num_days] * aban.active.values
                        nri = df['nri'][idx:idx+num_days] * aban.active.values
                        df['gross_aban_capex'][idx:idx+num_days] = aban.inv_g_aban.values
                        df['gross_total_capex'][idx:idx+num_days] = (df['gross_total_capex'][idx:idx+num_days] + 
                                                                        df['gross_aban_capex'][idx:idx+num_days])
                        df['net_aban_capex'][idx:idx+num_days] = (df['gross_aban_capex'][idx:idx+num_days] *
                                                                    inputs['wi_frac'].wi_frac.values)
                        df['net_total_capex'][idx:idx+num_days] = (df['net_total_capex'][idx:idx+num_days] + 
                                                                    df['net_aban_capex'][idx:idx+num_days])
                        df['fcf'][idx:idx+num_days] = (df['fcf'][idx:idx+num_days] - 
                                                        df['net_aban_capex'][idx:idx+num_days])
                        df['active'][idx:idx+num_days] = aban['active'].values
                        df['wi'][idx:idx+num_days] = wi
                        df['nri'][idx:idx+num_days] = nri
            else:
                if min_life_val not in ('loss ok', 'lossok', 'loss_ok'):
                    date_mask = (df['prod_date'][idx:idx+num_days] >= np.datetime64(prod_start_date))
                    gas_mask = (df['gross_gas'][idx:idx+num_days] == 0)
                    oil_mask = (df['gross_oil'][idx:idx+num_days] == 0)
                    combined_mask = np.logical_and(date_mask, gas_mask)
                    combined_mask = np.logical_and(combined_mask, oil_mask)
                    for k in df.keys():
                        if k in ('scenario', 'idp', 'prod_date', 'budget_type', 'hh', 'wti', 'ng', 'nwr', 'cig',
                                'name', 'short_pad', 'pad', 'rig', 'field', 'time_on', 'wi', 'nri', 'gas_eur', 'oil_eur',
                                'gas_price_adj', 'oil_price_adj', 'ngl_price_adj', 'version', 'owner', 'type', 'simulation'):
                            continue
                        elif k == 'active':
                            df[k][idx:idx+num_days] = (~combined_mask).astype(int)
                        else:
                            df[k][idx:idx+num_days] = df[k][idx:idx+num_days] * (~combined_mask).astype(int)
                else:
                    df['active'][idx:idx+num_days] = np.array([1] * len(df['active'][idx:idx+num_days]))

            for i in range(10):
                if i in range(len(build.pv_spread)):
                    pv = build.pv_spread[i]
                    j = i + 1
                    df['pv'+str(j)][idx:idx+num_days] = npv(df['fcf'][idx:idx+num_days], float(pv)/100)
                    df['pv'+str(j)+'_rate'][idx:idx+num_days] = float(pv)

            if i == 'S9FE2RHSET':
                test = pd.DataFrame(df)
                test.to_csv('test.csv')

        df = pd.DataFrame(df)

        if not build.daily_output:
            eomonth = []
            for d in df['prod_date']:
                day = calendar.monthrange(d.year, d.month)[1]
                eomonth.append(datetime(d.year, d.month, day))
            df.loc[:, 'prod_date'] = pd.to_datetime(pd.Series(eomonth))
            df.loc[:, ['gas_adj_unit', 'oil_adj_unit', 'ngl_adj_unit']] = 0
            df = df.groupby(by=['scenario', 'idp', 'prod_date', 'budget_type',
                                'name', 'short_pad', 'pad', 'rig', 'field', 'owner', 'version', 'type', 'simulation'], as_index=False).sum()
            for i in df['idp'].unique():
                try:
                    t_start = np.where(df.loc[df.idp == i, 'gross_gas'].values > 0)[0][0]
                except IndexError:
                    t_start = 0
                df.loc[df.idp == i, 'time_on'] = np.concatenate([np.zeros(t_start), np.arange(1, len(df.loc[df.idp == i, 'time_on'])+1-t_start)])
            df.loc[:, 'ngl_yield'] = df['ngl_yield'] / df['prod_date'].dt.day
            df.loc[:, 'btu'] = df['btu'] / df['prod_date'].dt.day
            df.loc[:, 'shrink'] = df['shrink'] / df['prod_date'].dt.day
            df.loc[:, 'wi'] = df['wi'] / df['prod_date'].dt.day
            df.loc[:, 'nri'] = df['nri'] / df['prod_date'].dt.day
            df.loc[:, 'gross_gas_mult'] = df['gross_gas_mult'] / df['prod_date'].dt.day
            df.loc[:, 'gross_oil_mult'] = df['gross_oil_mult'] / df['prod_date'].dt.day
            df.loc[:, 'gross_water_mult'] = df['gross_water_mult'] / df['prod_date'].dt.day
            df.loc[:, 'hh'] = df['hh'] / df['prod_date'].dt.day
            df.loc[:, 'wti'] = df['wti'] / df['prod_date'].dt.day
            df.loc[:, 'cig'] = df['cig'] / df['prod_date'].dt.day
            df.loc[:, 'nwr'] = df['nwr'] / df['prod_date'].dt.day
            df.loc[:, 'gas_basis'] = e['gas_basis'].values[0]
            df.loc[:, 'gas_price_adj'] = df['gas_price_adj'] / df['prod_date'].dt.day
            df.loc[:, 'oil_price_adj'] = df['oil_price_adj'] / df['prod_date'].dt.day
            df.loc[:, 'ngl_price_adj'] = df['ngl_price_adj'] / df['prod_date'].dt.day
            df.loc[:, 'realized_gas_price'] = df['realized_gas_price'] / df['prod_date'].dt.day
            df.loc[:, 'realized_oil_price'] = df['realized_oil_price'] / df['prod_date'].dt.day
            df.loc[:, 'realized_ngl_price'] = df['realized_ngl_price'] / df['prod_date'].dt.day
            df.loc[:, 'active'] = df['active'] / df['prod_date'].dt.day

            for i in range(10):
                if i in range(len(build.pv_spread)):
                    pv = build.pv_spread[i]
                    j = i + 1
                    df.loc[:, 'pv'+str(j)+'_rate'] = float(pv)
                else:
                    df.loc[:, 'pv'+str(j)+'_rate'] = ''

        save_output(df)
        onelines(df, build.daily_output)
    return

def onelines(df, daily_output):
    fields = [f.name for f in output_onelines._meta.get_fields()]
    o = pd.DataFrame(columns=fields)
    idp_list = []
    sim_list = []
    num_simulations = len(df['simulation'].unique())
    if num_simulations == 0:
        num_simulations = 1
    for i in df['idp'].unique():
        for s in df['simulation'].unique():
            idp_list.append(i)
            sim_list.append(s)
    o['idp'] = idp_list
    o['simulation'] = sim_list

    for i in df['idp'].unique():
        for s in df['simulation'].unique():

            t = df[(df['idp'] == i) & (df['simulation'] == s)]

            o.loc[(o.idp == i) & (o.simulation == s), 'gas_eur'] = t['gas_eur'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'oil_eur'] = t['oil_eur'].sum()

            o.loc[(o.idp == i) & (o.simulation == s), 'scenario'] = t['scenario'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'type'] = t['type'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'budget_type'] = t['budget_type'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'name'] = t['name'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'short_pad'] = t['short_pad'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pad'] = t['pad'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'rig'] = t['rig'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'field'] = t['field'].values[0]

            if daily_output:
                o.loc[(o.idp == i) & (o.simulation == s), 'ip90'] = t.loc[t.time_on <= 90, 'gross_gas'].sum()
            else:
                o.loc[(o.idp == i) & (o.simulation == s), 'ip90'] = t.loc[t.time_on <= 3, 'gross_gas'].sum()

            o.loc[(o.idp == i) & (o.simulation == s), 'gross_gas_mult'] = t['gross_gas_mult'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_oil_mult'] = t['gross_oil_mult'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_water_mult'] = t['gross_water_mult'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_gas'] = t['gross_gas'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_oil'] = t['gross_oil'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_water'] = t['gross_water'].sum()

            o.loc[(o.idp == i) & (o.simulation == s), 'ngl_yield'] = t['ngl_yield'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'btu'] = t['btu'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'shrink'] = t['shrink'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'wi'] = t['wi'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'nri'] = t['nri'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'royalty'] = t['royalty'].mean()

            o.loc[(o.idp == i) & (o.simulation == s), 'net_gas'] = t['net_gas'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_oil'] = t['net_oil'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_ngl'] = t['net_ngl'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_mcfe'] = t['net_mcfe'].sum()

            o.loc[(o.idp == i) & (o.simulation == s), 'royalty_gas'] = t['royalty_gas'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'royalty_oil'] = t['royalty_oil'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'royalty_ngl'] = t['royalty_ngl'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'royalty_mcfe'] = t['royalty_mcfe'].sum()       

            o.loc[(o.idp == i) & (o.simulation == s), 'hh'] = t['hh'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'wti'] = t['wti'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'ngl'] = t['ngl'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'cig'] = t['cig'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'nwr'] = t['nwr'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'gas_basis'] = t['gas_basis'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'gas_price_adj'] = t['gas_price_adj'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'oil_price_adj'] = t['oil_price_adj'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'ngl_price_adj'] = t['ngl_price_adj'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'realized_gas_price'] = t['realized_gas_price'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'realized_oil_price'] = t['realized_oil_price'].mean()
            o.loc[(o.idp == i) & (o.simulation == s), 'realized_ngl_price'] = t['realized_ngl_price'].mean()

            o.loc[(o.idp == i) & (o.simulation == s), 'net_gas_rev'] = t['net_gas_rev'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_oil_rev'] = t['net_oil_rev'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_ngl_rev'] = t['net_ngl_rev'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_total_rev'] = t['net_total_rev'].sum()

            o.loc[(o.idp == i) & (o.simulation == s), 'gross_drill_capex'] = t['gross_drill_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_compl_capex'] = t['gross_compl_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_misc_capex'] = t['gross_misc_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_aban_capex'] = t['gross_aban_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'gross_total_capex'] = t['gross_total_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_drill_capex'] = t['net_drill_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_compl_capex'] = t['net_compl_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_misc_capex'] = t['net_misc_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_aban_capex'] = t['net_aban_capex'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'net_total_capex'] = t['net_total_capex'].sum()

            o.loc[(o.idp == i) & (o.simulation == s), 'fixed_cost'] = t['fixed_cost'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'alloc_fixed_cost'] = t['alloc_fixed_cost'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'var_gas_cost'] = t['var_gas_cost'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'var_oil_cost'] = t['var_oil_cost'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'var_water_cost'] = t['var_water_cost'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'doe'] = t['doe'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'gtp'] = t['gtp'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'taxes'] = t['taxes'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'loe'] = t['loe'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'cf'] = t['cf'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'fcf'] = t['fcf'].sum()

            o.loc[(o.idp == i) & (o.simulation == s), 'pv1'] = t['pv1'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv1_rate'] = t['pv1_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv2'] = t['pv2'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv2_rate'] = t['pv2_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv3'] = t['pv3'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv3_rate'] = t['pv3_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv4'] = t['pv4'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv4_rate'] = t['pv4_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv5'] = t['pv5'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv5_rate'] = t['pv5_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv6'] = t['pv6'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv6_rate'] = t['pv6_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv7'] = t['pv7'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv7_rate'] = t['pv7_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv8'] = t['pv8'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv8_rate'] = t['pv8_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv9'] = t['pv9'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv9_rate'] = t['pv9_rate'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'pv10'] = t['pv10'].sum()
            o.loc[(o.idp == i) & (o.simulation == s), 'pv10_rate'] = t['pv10_rate'].values[0]

            o.loc[(o.idp == i) & (o.simulation == s), 'owner'] = t['owner'].values[0]
            o.loc[(o.idp == i) & (o.simulation == s), 'version'] = pd.Timestamp(t['version'].values[0])
            o.loc[(o.idp == i) & (o.simulation == s), 'simulation'] = t['simulation'].values[0]

            cf_start = np.where(abs(t['fcf'].values) > 0)[0][0]

            if o.loc[(o.idp == i) & (o.simulation == s), 'net_total_capex'].values[0] > 0:
                irr = xirr(t['fcf'].values[cf_start:], daily_output)
                o.loc[(o.idp == i) & (o.simulation == s), 'irr'] = irr
                if t['fcf'].sum() <0:
                    payout = np.nan
                else:
                    payout = np.where(t['fcf'].values[cf_start:].cumsum() >= 0)[0][0] / (365.25/12)
                    o.loc[(o.idp == i) & (o.simulation == s), 'payout'] = payout
            else:
                o.loc[(o.idp == i) & (o.simulation == s), 'irr'] = np.nan
                o.loc[(o.idp == i) & (o.simulation == s), 'payout'] = np.nan

            o.loc[(o.idp == i) & (o.simulation == s), 'year_1_fcf'] = t['fcf'][cf_start:cf_start+365].sum()
            year_1_capex = t['net_total_capex'][cf_start:cf_start+365].sum()
            if year_1_capex > 0:
                o.loc[(o.idp == i) & (o.simulation == s), 'year_1_roic'] = t['cf'][cf_start:cf_start+365].sum() / year_1_capex
            else:
                o.loc[(o.idp == i) & (o.simulation == s), 'year_1_roic'] = np.nan

            o.loc[(o.idp == i) & (o.simulation == s), 'year_2_fcf'] = t['fcf'][cf_start:cf_start+2*365].sum()
            year_2_capex = t['net_total_capex'][cf_start:cf_start+2*365].sum()
            if year_2_capex > 0:
                o.loc[(o.idp == i) & (o.simulation == s), 'year_2_roic'] = t['cf'][cf_start:cf_start+2*365].sum() / year_2_capex
            else:
                o.loc[(o.idp == i) & (o.simulation == s), 'year_2_roic'] = np.nan
            
            total_capex = t['net_total_capex'].sum()
            if total_capex > 0:
                o.loc[(o.idp == i) & (o.simulation == s), 'pvr'] = (t['pv10'].sum() + total_capex) / total_capex

    save_onelines(o)
    return

def aggregate(data):
    scenario = data['scenario'].values_list('scenario')[0][0]
    agg_field = data['field']
    num_simulations = int(data['num_simulations'])
    num_aggregations = int(data['num_aggregations'])
    owner = data['owner']
    output_version = pd.Timestamp(data['version'])
    low_pct = [float(data['low_pct']) if float(data['low_pct']) < 1 else float(data['low_pct'])/100][0]
    mid_pct = [float(data['mid_pct']) if float(data['mid_pct']) < 1 else float(data['mid_pct'])/100][0]
    high_pct = [float(data['high_pct']) if float(data['high_pct']) < 1 else float(data['high_pct'])/100][0]
    version = pd.Timestamp.utcnow()

    agg_fields = None

    if data['onelines']:
        idp_onelines = output_onelines.objects.filter(scenario=scenario, version=output_version).values_list('idp', flat=True).distinct()
        agg_fields = output_onelines.objects.filter(scenario=scenario, version=output_version).values(agg_field).distinct()
        oneline_fields = [f.name for f in output_onelines._meta.get_fields()]
        num_prop_onelines = len(idp_onelines)

        oneline_results = {}
        for c in oneline_fields:
            if c in ('scenario', 'idp', 'name', 'budget_type', 'prod_date', 'owner', 'version', 'simulation', 'id',
                        'short_pad', 'pad', 'rig', 'field', 'type', 'gas_basis', 'gas_adj_unit', 'oil_adj_unit', 'ngl_adj_unit'):
                continue
            elif c == 'owner':
                oneline_results[c] = [owner] * (num_prop_onelines*num_simulations)
            elif c == 'version':
                oneline_results[c] = [version] * (num_prop_onelines*num_simulations)
            elif c == 'simulation':
                oneline_results[c] = np.arange(num_prop_onelines*num_simulations)
            else:
                oneline_results[c] = np.zeros(num_prop_onelines*num_simulations, dtype='float')
        oneline_results['aggregation_field'] = [agg_field] * (num_prop_onelines*num_simulations)
        oneline_results['aggregation_id'] = np.empty(num_prop_onelines*num_simulations, dtype='object')

    if data['monthly']:
        idp_monthly = output.objects.filter(scenario=scenario, version=output_version).values_list('idp', flat=True).distinct()
        agg_fields = output.objects.filter(scenario=scenario, version=output_version).values(agg_field).distinct()
        monthly_fields = [f.name for f in output._meta.get_fields()]
        num_prop_monthly = len(idp_monthly)

        monthly_results = {}
        for c in monthly_fields:
            if c in ('scenario', 'idp', 'name', 'budget_type', 'prod_date', 'owner', 'version', 'simulation', 'id',
                        'short_pad', 'pad', 'rig', 'field', 'type', 'gas_basis', 'gas_adj_unit', 'oil_adj_unit', 'ngl_adj_unit'):
                continue
            elif c == 'owner':
                monthly_results[c] = [owner] * (num_prop_monthly*num_simulations)
            elif c == 'version':
                monthly_results[c] = [version] * (num_prop_monthly*num_simulations)
            elif c == 'simulation':
                monthly_results[c] = np.arange(num_prop_monthly*num_simulations)
            else:
                monthly_results[c] = np.zeros(num_prop_monthly*num_simulations, dtype='float')
        monthly_results['aggregation_field'] = [agg_field] * (num_prop_monthly*num_simulations)
        monthly_results['aggregation_id'] = np.empty(num_prop_monthly*num_simulations, dtype='object')

    count = 0
    for f in agg_fields:
        print(f)
        for s in range(num_simulations):
            field_name = list(f.keys())[0]
            value = list(f.values())[0]
            filters = {'scenario': scenario, 'version': output_version, field_name: value}
            if data['onelines']:
                id_choices = output_onelines.objects.filter(**filters).values_list('id', flat=True)
            else:
                id_choices = output.objects.filter(**filters).values_list('id', flat=True)
            ids_selected = np.random.choice(id_choices, num_aggregations, replace=False)
            onelines_selected = pd.DataFrame(output_onelines.objects.filter(id__in=ids_selected, **filters).values())
            monthly_selected = pd.DataFrame(output.objects.filter(id__in=ids_selected, **filters).values())
            if data['onelines']:
                for c in oneline_results.keys():
                    if c == 'aggregation_field':
                        continue
                    elif c == 'aggregation_id':
                        oneline_results[c][count] = f
                    elif c == 'simulation':
                        oneline_results[c][count] = s
                    else:
                        oneline_results[c][count] = onelines_selected[c].mean()
            if data['monthly']:
                for c in monthly_results.keys():
                    if c == 'aggregation_field':
                        continue
                    elif c == 'aggregation_id':
                        monthly_results[c][count] = f
                    elif c == 'simulation':
                        monthly_results[c][count] = s
                    else:
                        monthly_results[c][count] = monthly_selected[c].mean()
            count += 1

    if data['onelines']:
        oneline_results = pd.DataFrame(oneline_results)
        oneline_results.to_csv('oneline_test.csv')
        oneline_results.to_pickle('oneline_test.pkl')
    if data['monthly']:
        monthly_results = pd.DataFrame(monthly_results)
        monthly_results.to_pickle('monthly_test.pkl')
    


def box_plot(df, metric):
    f = df.copy()
    f.loc[:, 'irr'] = df.loc[:, 'irr']
    plt.figure(figsize=(15,10))
    sorted_index = f.groupby(by=['aggregation_id'], as_index=False)[['aggregation_id', metric]].quantile(q=0.5)
    sorted_index.sort_values(by=[metric], inplace=True, ascending=False)
    sorted_index = sorted_index['aggregation_id'].values
    ax = sns.boxplot(x='aggregation_id', y=metric, data=f, order=sorted_index)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('aggregation.png')


def calculate_percentiles(build):
    print('calculating percentiles')
    for idp in build.properties_list:
        print(idp)
        print('output')
        pct_rank_output(p_list=[build.low_pct, build.mid_pct, build.high_pct], idp=idp, scenario=build.name, version=build.version)
        print('onelines')
        pct_rank_onelines(p_list=[build.low_pct, build.mid_pct, build.high_pct], idp=idp, scenario=build.name, version=build.version)