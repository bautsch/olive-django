import pandas as pd
import numpy as np
from datetime import timedelta
import math
import random
import calendar
import datetime
import operator
from scipy.stats import truncnorm
from scipy.stats import beta
from scipy import optimize
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd
from turbodbc import connect as con
from turbodbc import make_options
from django.db.models import Count
from sqlalchemy.pool import NullPool
from olive.models import *
from django.db.models import Max

def calc_drill_dates(schedule):
    schedule.schedule_dates.drill_start_date = pd.to_datetime(schedule.schedule_dates.drill_start_date)
    schedule.schedule_dates.drill_end_date = pd.to_datetime(schedule.schedule_dates.drill_end_date)
    for k, rig in schedule.rig_dict.items():
        for idxp, pad in enumerate(rig.pad_list):
            if pad.rig.rig_name == 'DUC':
                for idxw, well in enumerate(pad.well_list):
                    schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                       'drill_start_date'] = pd.NaT
                    schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                       'drill_end_date'] = pd.NaT
                continue
            if pad.drill_start is None:
                prior_pad = rig.pad_list[idxp - 1]
                pad.drill_start = prior_pad.drill_finish + timedelta(prior_pad.mob_out) + timedelta(pad.mob_in)
            if pad.drill_finish is None:
                for idxw, well in enumerate(pad.well_list):
                    idp = well.idp
                    if schedule.uncertainty is not None:
                        u = schedule.uncertainty.loc[schedule.uncertainty.idp == idp].drill_time.values[0]
                    else:
                        u = 1
                    if schedule.risk is not None:
                        r = schedule.risk.loc[schedule.risk.idp == idp].drill_time.values[0]
                    else:
                        r = 1
                    if idxw == 0:
                        well.drill_start_date = pad.drill_start
                        well.drill_end_date = well.drill_start_date + timedelta(well.drill_time * u * r)
                        well.drill_time = well.drill_time * u * r
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_start_date'] = well.drill_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_end_date'] = well.drill_end_date
                    elif idxw == len(pad.well_list) - 1:
                        prior_well = pad.well_list[idxw-1]
                        well.drill_start_date = prior_well.drill_end_date
                        well.drill_end_date = well.drill_start_date + timedelta(well.drill_time * u * r)
                        well.drill_time = well.drill_time * u * r
                        pad.drill_finish = well.drill_end_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_start_date'] = well.drill_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_end_date'] = well.drill_end_date
                    else:
                        prior_well = pad.well_list[idxw - 1]
                        well.drill_start_date = prior_well.drill_end_date
                        well.drill_end_date = well.drill_start_date + timedelta(well.drill_time * u * r)
                        well.drill_time = well.drill_time * u * r
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_start_date'] = well.drill_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_end_date'] = well.drill_end_date
            else:
                drill_time = (pad.drill_finish - pad.drill_start).days / pad.num_wells
                for idxw, well in enumerate(pad.well_list):
                    idp = well.idp
                    if schedule.uncertainty is not None:
                        u = schedule.uncertainty.loc[schedule.uncertainty.idp == idp].drill_time.values[0]
                    else:
                        u = 1
                    if schedule.risk is not None:
                        r = schedule.risk.loc[schedule.risk.idp == idp].drill_time.values[0]
                    else:
                        r = 1
                    if idxw == 0:
                        if idxp == 0:
                            next_pad = rig.pad_list[idxp + 1]
                            if next_pad.drill_start is not None:
                                pad_timing_delta = (next_pad.drill_start - pad.drill_finish).days
                        if idxp > 0:
                            prior_pad = rig.pad_list[idxp - 1]
                            pad.drill_start = prior_pad.drill_finish + timedelta(pad_timing_delta)
                            if idxp < len(rig.pad_list) - 1:
                                next_pad = rig.pad_list[idxp + 1]
                                if next_pad.drill_start is not None:
                                    pad_timing_delta = (next_pad.drill_start - pad.drill_finish).days
                        well.drill_start_date = pad.drill_start
                        well.drill_end_date = well.drill_start_date + timedelta(drill_time * u * r)
                        well.drill_time = drill_time * u * r
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_start_date'] = well.drill_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_end_date'] = well.drill_end_date
                    else:
                        prior_well = pad.well_list[idxw - 1]
                        well.drill_start_date = prior_well.drill_end_date
                        well.drill_end_date = well.drill_start_date + timedelta(drill_time * u * r)
                        well.drill_time = drill_time * u * r
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_start_date'] = well.drill_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'drill_end_date'] = well.drill_end_date
                    if idxw == len(pad.well_list) - 1:
                        time_shift = (well.drill_end_date - pad.drill_finish).days
                        pad.time_shift = time_shift
                        pad.drill_finish = well.drill_end_date

def calc_compl_dates(schedule):
    schedule.schedule_dates.compl_start_date = pd.to_datetime(schedule.schedule_dates.compl_start_date)
    schedule.schedule_dates.compl_end_date = pd.to_datetime(schedule.schedule_dates.compl_end_date)
    for k, rig in schedule.rig_dict.items():
        for idxp, pad in enumerate(rig.pad_list):
            if pad.compl_start is None:
                pad.compl_start = (pad.drill_finish + timedelta(pad.mob_out)
                                   + timedelta(pad.log_pad) + timedelta(pad.build_facilities) + timedelta(pad.time_shift))
                prior_pad = rig.pad_list[idxp - 1]
                last_compl_date = prior_pad.compl_finish
                if last_compl_date:
                    if pad.compl_start < last_compl_date:
                        pad.compl_start = last_compl_date + timedelta(3)
            if pad.compl_finish is None:
                for idxw, well in enumerate(pad.well_list):
                    if idxw == 0:
                        well.compl_start_date = pad.compl_start + timedelta(pad.time_shift)
                        well.compl_end_date = well.compl_start_date + timedelta(well.compl_time)
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_start_date'] = well.compl_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_end_date'] = well.compl_end_date
                    elif idxw == len(pad.well_list) - 1:
                        prior_well = pad.well_list[idxw-1]
                        well.compl_start_date = prior_well.compl_end_date
                        well.compl_end_date = well.compl_start_date + timedelta(well.compl_time)
                        pad.compl_finish = well.compl_end_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_start_date'] = well.compl_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_end_date'] = well.compl_end_date
                    else:
                        prior_well = pad.well_list[idxw - 1]
                        well.compl_start_date = prior_well.compl_end_date
                        well.compl_end_date = well.compl_start_date + timedelta(well.compl_time)
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_start_date'] = well.compl_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_end_date'] = well.compl_end_date
            else:
                pad.compl_start = pad.compl_start + timedelta(pad.time_shift)
                pad.compl_finish = pad.compl_finish + timedelta(pad.time_shift)
                compl_time = (pad.compl_finish - pad.compl_start).days / pad.num_wells
                for idxw, well in enumerate(pad.well_list):
                    if idxw == 0:
                        well.compl_start_date = pad.compl_start
                        well.compl_end_date = well.compl_start_date + timedelta(compl_time)
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_start_date'] = well.compl_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_end_date'] = well.compl_end_date
                    else:
                        prior_well = pad.well_list[idxw - 1]
                        well.compl_start_date = prior_well.compl_end_date
                        well.compl_end_date = well.compl_start_date + timedelta(compl_time)
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_start_date'] = well.compl_start_date
                        schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                           'compl_end_date'] = well.compl_end_date

def calc_start_dates(schedule):
    schedule.schedule_dates.prod_start_date = pd.to_datetime(schedule.schedule_dates.prod_start_date)
    for k, rig in schedule.rig_dict.items():
        for idxp, pad in enumerate(rig.pad_list):
            if pad.prod_start is None:
                wells = np.arange(0, len(pad.well_list))
                if pad.pod_size == len(wells):
                    pods = [len(wells) - 1]
                elif pad.pod_size == 1:
                    pods = wells
                else:
                    if pad.pod_size > len(wells):
                        pods = [len(wells) - 1]
                    else:
                        pods = [w - 1 for w in wells if (w % pad.pod_size == 0) and (w > 0)]
                        if max(wells) > max(pods):
                            pods.append(max(wells))
                pod_start_dates = [pad.well_list[w].compl_end_date +
                                   timedelta(pad.well_list[w].flowback_time) for w in pods]
                pod_idx = 0
                pod = pods[pod_idx]
                pad.prod_start = pod_start_dates[0]
                pad.prod_finish = pod_start_dates[-1]
                if pad.prod_finish == pad.prod_start:
                    pad.prod_finish = pad.prod_finish + timedelta(3)
                for idxw, well in enumerate(pad.well_list):
                    if idxw > pod:
                        pod_idx += 1
                    pod = pods[pod_idx]
                    well.prod_start_date = pod_start_dates[pod_idx]
                    schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                                'prod_start_date'] = well.prod_start_date
                    if idxw == 0:
                       pad.prod_start = pd.Timestamp(well.prod_start_date)
            else:
                if pad.prod_finish is None:
                    pad.prod_finish = pad.prod_start + timedelta(3)
                for idxw, well in enumerate(pad.well_list):
                    well.prod_start_date = pad.prod_start
                    schedule.schedule_dates.loc[schedule.schedule_dates.idp == well.idp,
                                                'prod_start_date'] = well.prod_start_date
                    if idxw == 0:
                        pad.prod_start = pd.Timestamp(well.prod_start_date)

def load_properties(scenario):
    if scenario['project'] is not None:
        idp_list = projects.objects.values_list('idp', flat=True).filter(scenario=scenario['project'], version=scenario['project_version'])
        props = properties.objects.filter(scenario=scenario['properties'], idp__in=idp_list, version=scenario['properties_version']).values()
    else:
        props = properties.objects.filter(scenario=scenario['properties'], version=scenario['properties_version']).values()
    return props

def load_probabilities(properties, scenario, probability_version, owner, version, sim=0):
    num_prop = len(properties['idp'].values)
    uncertainty = {
        'idp': properties['idp'].values,
        'drill_time': [1.0] * num_prop,
        'performance': [1.0] * num_prop,
        'profile': [1.0] * num_prop,
        'drill_cost': [1.0] * num_prop,
        'complete_cost': [1.0] * num_prop,
        'gas_price': [1.0] * num_prop,
        'oil_price': [1.0] * num_prop,
        'ngl_price': [1.0] * num_prop,
        'ngl_yield': [1.0] * num_prop,
        'btu': [1.0] * num_prop,
        'shrink': [1.0] * num_prop,
        'doe': [1.0] * num_prop,
        'gtp': [1.0] * num_prop,
        'total_capex': [1.0] * num_prop,
        'infra_cost': [1.0] * num_prop
    }
    uncertainty = pd.DataFrame(uncertainty)

    risk = {
        'idp': properties['idp'].values,
        'drill_time': [1.0] * num_prop,
        'performance': [None] * num_prop,
        'in_zone': [None] * num_prop,
        'wellbore': [None] * num_prop,
        'profile': [1.0] * num_prop,
        'curtailment': [None] * num_prop,
        'frac_hit': [None] * num_prop,
        'spacing': [None] * num_prop,
        'drill_cost': [None] * num_prop,
        'complete_cost': [None] * num_prop,
        'abandon': [None] * num_prop,
        'downtime': [None] * num_prop,
        'frequency': [None] * num_prop,
        'duration': [None] * num_prop,
        'downtime_mult': [None] * num_prop,
        'downtime_cost': [None] * num_prop,
        'gas_downtime': [None] * num_prop,
        'oil_downtime': [None] * num_prop,
        'delay': [None] * num_prop,
    }
    risk = pd.DataFrame(risk)

    if scenario is not None:
        df = pd.DataFrame(probabilities.objects.filter(scenario=scenario, version=probability_version).values())
        u = df[df.category == 'uncertainty']
        if len(u) > 0:
            for p in u.property_value.unique():
                col = u[u.property_value == p].property_type.values[0]
                prop_subset = properties[properties[col] == p]['idp']
                u_p = u[u.property_value == p]
                for t in u_p.type.unique():
                    d = prob_dict(u_p[u_p.type == t].value.values[0])
                    a = apply_uncertainty(d)
                    uncertainty.loc[uncertainty.idp.isin(prop_subset), t] = a

        r = df[df.category == 'risk']
        abandon = None
        if len(r) > 0:
            for p in r.property_value.unique():
                col = r[r.property_value == p].property_type.values[0]
                prop_subset = properties[properties[col] == p]['idp']
                r_p = r[r.property_value == p]
                if len(prop_subset) > 0:
                    if 'abandon' in r_p.type.values:
                        d = prob_dict(r_p[r_p.type == 'abandon'].value.values[0])
                        abandon = apply_risk(d)
                        if abandon is not None:
                            prop = np.random.choice(prop_subset, 1)
                            rem_prop = [p for p in prop_subset if p not in prop]
                            risk.loc[risk.idp.isin(prop), 'performance'] = 0.0
                            risk.loc[risk.idp.isin(prop), 'profile'] = 0.0
                            risk.loc[risk.idp.isin(prop), 'abandon'] = abandon
                            risk.loc[risk.idp.isin(rem_prop), 'abandon'] = None
                        else:
                            risk.loc[risk.idp.isin(prop_subset), 'abandon'] = abandon

                    for t in r_p.type.values:
                        if t == 'abandon':
                            continue
                        if abandon is None:
                            if t == 'downtime':
                                d = prob_dict(r_p[r_p.type == t].value.values[0])
                                risk.loc[risk.idp.isin(prop_subset), 'downtime'] = True
                                risk.loc[risk.idp.isin(prop_subset), 'frequency'] = d['frequency']
                                risk.loc[risk.idp.isin(prop_subset), 'duration'] = d['duration']
                                risk.loc[risk.idp.isin(prop_subset), 'downtime_mult'] = d['mult']
                                risk.loc[risk.idp.isin(prop_subset), 'downtime_cost'] = d['cost']
                            else:
                                d = prob_dict(r_p[r_p.type == t].value.values[0])
                                r_val = apply_risk(d)
                                if t == 'delay':
                                    if r_val is not None:
                                        r_val = int(r_val)
                                risk.loc[risk.idp.isin(prop_subset), t] = r_val
                        else:
                            if t == 'downtime':
                                d = prob_dict(r_p[r_p.type == t].value.values[0])
                                risk.loc[risk.idp.isin(prop_subset), 'downtime'] = True
                                risk.loc[risk.idp.isin(prop_subset), 'frequency'] = d['frequency']
                                risk.loc[risk.idp.isin(prop_subset), 'duration'] = d['duration']
                                risk.loc[risk.idp.isin(prop_subset), 'downtime_mult'] = d['mult']
                                risk.loc[risk.idp.isin(prop_subset), 'downtime_cost'] = d['cost']
                            else:
                                d = prob_dict(r_p[r_p.type == t].value.values[0])
                                r_val = apply_risk(d)
                                if t == 'delay':
                                    if r_val is not None:
                                        r_val = int(r_val)
                                risk.loc[risk.idp.isin(rem_prop), t] = r_val
        save_probability_log(uncertainty, risk, sim, scenario, version, owner)
    return {'risk': risk, 'uncertainty': uncertainty}

def prob_dict(u_val):
    d = {}
    u_val = u_val.split(',')
    for i in u_val:
        if i[0] == ' ':
            i = i[1:]
        i = i.split(':')
        try:
            val = float(i[1])
        except:
            val = i[1]
        d[i[0]] = val
    return d

def apply_uncertainty(d):
    if d['distribution'] == 'normal':
        a = (d['min'] - d['mean']) / d['stdev']
        b = (d['max'] - d['mean']) / d['stdev']
        return truncnorm.rvs(a, b, loc=d['mean'], scale=d['stdev'])
    if d['distribution'] == 'uniform':
        return np.random.uniform(d['min'], d['max'])
    if d['distribution'] == 'constant':
        return d['value']
    if d['distribution'] == 'coinflip':
        if np.random.random() <= d['probability']:
            return d['heads']
        else:
            return d['tails']

def apply_risk(d):
    p = d['probability']
    if np.random.random() <= p:
        if 'distribution' in d.keys():
            if d['distribution'] == 'normal':
                a = (d['min'] - d['mean']) / d['stdev']
                b = (d['max'] - d['mean']) / d['stdev']
                return truncnorm.rvs(a, b, loc=d['mean'], scale=d['stdev'])
            if d['distribution'] == 'uniform':
                return np.random.uniform(d['min'], d['max'])
            if d['distribution'] == 'constant':
                return d['value']
        elif 'cost' in d.keys():
            return d['cost']
        elif 'ip_mult' in d.keys():
            return d['ip_mult']
        elif 'tc_mult' in d.keys():
            return d['tc_mult']
    else:
        return None

def apply_curtailment(m, f):
    profile = f * m
    idx = np.nonzero(f)[0]
    if len(idx) == 0:
        return profile
    else:
        p = idx[0]
        t = int(5514.8*np.exp(-3.978*m))
        if p+45 > len(profile) or p+t > len(f):
            return profile
        else:
            v1 = profile[p+45]
            v2 = f[p+t]
            delta = (v2 - v1) / (t - 45)
            fill = np.arange(1, t-44) * delta + v1
            profile[p+45:p+t] = fill
            profile[p+t:] = f[p+t:]
            q1 = sum(f)
            q2 = sum(profile)
            delta_q = (q1 - q2) / 18160
            profile[p+90:] = profile[p+90:] + delta_q
            return profile

def apply_ip_adjust(m, f):
    profile = f * m
    idx = np.nonzero(f)[0]
    if len(idx) == 0:
        return profile
    else:
        p = idx[0]
        x = np.linspace(0.1, 1, 19)
        y = [13904, 7966, 5285, 3832, 2919,
            2299, 1853, 1519, 1260, 1055,
            887, 752, 635, 533, 445,
            365, 293, 212, 0]
        t = int(np.interp(m, x, y))
        if t == 90:
            t = 91
        if p+90 > len(profile) or p+t > len(f):
            return profile
        else:
            if m <= 1.0:
                v1 = profile[p+90]
                v2 = f[p+t]
                delta = (v2 - v1) / (t - 90)
                fill = np.arange(1, t-89) * delta + v1
                profile[p+90:p+t] = fill
                profile[p+t:] = f[p+t:]
                q1 = sum(f)
                q2 = sum(profile)
                delta_q = (q1 - q2) / 18160
                profile[p+90:] = profile[p+90:] + delta_q
                return profile
            else:
                v1 = profile[p+90]
                v2 = profile[p+120]
                delta = (v2 - v1) / 30
                fill = np.arange(1, 31) * delta + v1
                q1 = sum(f[p:p+120])
                q2 = sum(profile[p:p+120])
                delta_q = (q1 - q2) / 18160
                profile[p+90:p+120] = fill
                profile[p+120:] = profile[p+120:] + delta_q
                return profile

def save_probability_log(uncertainty, risk, sim, scenario, version, owner):
    u = uncertainty.melt(id_vars=['idp'], var_name='type', value_name='value')
    u['category'] = 'uncertainty'
    u['scenario'] = scenario
    u['version'] = version
    u['owner'] = owner
    u['simulation'] = sim
    u.to_sql(name='olive_probability_log', con=engine(),
             if_exists='append', method='multi',
             index=False, chunksize=500)
    r = risk.melt(id_vars=['idp'], var_name='type', value_name='value')
    r['category'] = 'risk'
    r['scenario'] = scenario
    r['version'] = version
    r['owner'] = owner
    r['simulation'] = sim
    r.to_sql(name='olive_probability_log', con=engine(),
             if_exists='append', method='multi',
             index=False, chunksize=500)
    return

def save_type_curves(type_curves):
    conn = connect()
    query = str('insert into olive_type_curves (name, time_on, gas, oil, water, version, owner)  values (?, ?, ?, ?, ?, ?, ?)')
    cursor = conn.cursor()
    cursor.executemany(query, type_curves.type_curves.itertuples(index=False, name=None))
    conn.commit()
    cursor.close()

def save_pdp_curves(pdp_curves):
    conn = connect()
    query = str('insert into olive_prod_forecasts (scenario, idp, prod_date, gas, oil, water, version, owner)  values (?, ?, ?, ?, ?, ?, ?, ?)')
    cursor = conn.cursor()
    cursor.executemany(query, pdp_curves.pdp_curves.itertuples(index=False, name=None))
    conn.commit()
    cursor.close()

def save_schedule(schedule):
    schedule.schedule_dates.to_sql(name='olive_schedules', con=engine(),
                                  if_exists='append', method='multi',
                                  index=False, chunksize=500)

def save_schedule_log(schedule):
    schedule_log = pd.DataFrame(columns={'schedule_scenario', 'schedule_owner', 'schedule_version',
                                         'schedule_file_scenario', 'schedule_file_owner', 'schedule_file_version',
                                         'schedule_inputs_owner', 'schedule_inputs_version'})
    schedule_log['schedule_scenario'] = [schedule.name]
    schedule_log['schedule_owner'] = [schedule.owner]
    schedule_log['schedule_version'] = [schedule.version]
    schedule_log['schedule_file_scenario'] = [schedule.name]
    schedule_log['schedule_file_owner'] = [schedule.owner]
    schedule_log['schedule_file_version'] = [schedule.version]
    schedule_inputs_owner = schedule_inputs.objects.values_list('owner', flat=True).filter(version=schedule.schedule_inputs_version).distinct()
    schedule_log['schedule_inputs_owner'] = schedule_inputs_owner
    schedule_log['schedule_inputs_version'] = [schedule.schedule_inputs_version]
    schedule.schedule_log = schedule_log
    schedule_log.to_sql(name='olive_schedule_log', con=engine(),
                                  if_exists='append', method='multi',
                                  index=False, chunksize=500)

def save_output_log(build, run_type):
    output_log = pd.DataFrame(columns={'output_scenario', 'output_owner', 'output_version', 'output_type',
                                         'scenario', 'scenario_owner', 'scenario_version'})
    output_log['output_scenario'] = [build.name]
    output_log['output_owner'] = [build.owner]
    output_log['output_version'] = [build.version]
    output_log['output_type'] = [run_type]
    output_log['scenario'] = [build.name]
    output_log['scenario_owner'] = [build.scenario_owner]
    output_log['scenario_version'] = [build.scenario_version]
    output_log.to_sql(name='olive_output_log', con=engine(),
                                  if_exists='append', method='multi',
                                  index=False, chunksize=500)

def engine():
    return create_engine(str('mssql+turbodbc://:@:1433/?driver=ODBC+Driver+17+for+SQL+Server'), poolclass=NullPool)

def end_date(framework):
    return (framework.effective_date + relativedelta(years=+framework.life) - relativedelta(days=+1))

def connect():
    options = make_options(prefer_unicode=True)
    return con(driver='ODBC Driver 17 for SQL Server',
                server='',
                port='1433',
                database='',
                uid='',
                pwd='',
                turbodbc_options=options)

def load_type_curves():
    eng = engine()
    query = str('select * from olive_type_curves')
    return pd.read_sql(query, eng)

def load_type_curve(f, start, end):
    eng = engine()
    versions = type_curves.objects.values_list('version', flat=True).filter(name=f.forecast_name.values[0]).order_by('version').distinct()
    version = versions[int(f.forecast_version.values[0])-1]
    query = str('select * from olive_type_curves where name = \'' + f.forecast_name.values[0] + '\' '
                'and version = \'' + str(version) + '\' and time_on >= ' + str(start) + ' and time_on <= ' + str(end))
    return pd.read_sql(query, eng)

def gen_cc_type_curve(tc_name, segments, dmin, min_rate):
    segments = int(segments)
    dmin = float(dmin)
    min_rate = int(min_rate)
    if dmin > 1:
        dmin = dmin/100
    eng = engine()
    current_ver = type_curves.objects.filter(name=tc_name).aggregate(Max('version'))['version__max']
    query = str('select * from olive_type_curves where name = \'' + tc_name + '\' '
                'and version = \'' + str(current_ver) + '\'')
    olive_tc = pd.read_sql(query, eng)
    olive_tc.sort_values(by=['time_on'], inplace=True)
    segx, segy = segments_fit(olive_tc.time_on[:150], olive_tc.gas[:150], segments)
    guess = [1.01, 0.75, 5000]
    bounds = ((0.5, 0.2, 50), (1.4, 0.99, 15000))
    r = optimize.least_squares(residuals, guess, bounds=bounds, args=(olive_tc.gas[150:], dmin, min_rate))
    params = [r.x[0], r.x[1], r.x[2]]
    switch = switch_date(params, dmin)
    eol = end_of_life(params, dmin, min_rate)
    cc_arps_import = pd.DataFrame(columns=['Segment', 'Segment Type', 'Start Date', 'End Date', 'q Start', 'q End', 'Di Eff-Sec', 'Di Nominal', 'b', 'Realized D', 'Sw-Date'])
    segment = np.arange(1, segments + 2)
    seg_type = ['linear']*segments
    segx = segx - 1
    start_date = []
    end_date = []
    q_start = []
    q_end = []
    di_eff = [0]*(segments)
    di_nom = [0]*(segments+1)
    b = [0]*(segments)
    realized_d = [0]*(segments)
    sw_date = [0]*(segments)

    for i, x in enumerate(segx):
        x = int(x)
        if i == 0:
            start_date.append(x)
            q_start.append(segy[i])
        else:
            start_date.append(x+1)
            end_date.append(x)
            q_start.append(segy[i])
            q_end.append(segy[i])

    seg_type.append('arps_modified')
    end_date.append(eol)
    q_end.append(min_rate)
    di_eff.append(params[1]*100)
    b.append(params[0])
    realized_d.append(int(dmin*100))
    sw_date.append(switch)

    cc_arps_import['Segment'] = segment
    cc_arps_import['Segment Type'] = seg_type
    cc_arps_import['Start Date'] = start_date
    cc_arps_import['End Date'] = end_date
    cc_arps_import['q Start'] = q_start
    cc_arps_import['q End'] = q_end
    cc_arps_import['Di Eff-Sec'] = di_eff
    cc_arps_import['Di Nominal'] = di_nom
    cc_arps_import['b'] = b
    cc_arps_import['Realized D'] = realized_d
    cc_arps_import['Sw-Date'] = sw_date

    return cc_arps_import

def segments_fit(X, Y, count):
    xmin = X.min()
    xmax = X.max()
    print('xmax', xmax)
    print('xmin', xmin)

    seg = np.full(count - 1, (xmax - xmin) / count)
    print(seg)

    px_init = np.r_[np.r_[xmin, seg].cumsum(), xmax]
    print(px_init)
    py_init = np.array([Y[np.abs(X - x) < (xmax - xmin) * 0.01].mean() for x in px_init])
    print(py_init)

    def func(p):
        seg = p[:count - 1]
        py = p[count - 1:]
        px = np.r_[np.r_[xmin, seg].cumsum(), xmax]
        return px, py

    def err(p):
        px, py = func(p)
        Y2 = np.interp(X, px, py)
        return np.mean((Y - Y2)**2)

    r = optimize.minimize(err, x0=np.r_[seg, py_init])
    return func(r.x)

def switch_date(params, dmin):
    if abs(params[0] - 1) < 0.0001:
        params[0] = 0.999
    ai = ((1/params[0])*(np.power((1-params[1]), -params[0])-1))/365
    df = 1-np.power((1+params[0]*-np.log(1-dmin)), -1/params[0])
    af = (1/params[0])*(np.power((1-df), -params[0])-1)/365
    t = int((ai-af)/(params[0]*ai*af))
    return t

def end_of_life(params, dmin, min_rate):
    if abs(params[0] - 1) < 0.0001:
        params[0] = 0.999
    ai = ((1/params[0])*(np.power((1-params[1]), -params[0])-1))/365
    df = 1-np.power((1+params[0]*-np.log(1-dmin)), -1/params[0])
    af = (1/params[0])*(np.power((1-df), -params[0])-1)/365
    t = int((ai-af)/(params[0]*ai*af))
    m = np.arange(1, t+1)
    m_exp = np.arange(1, 45625-t)
    q = params[2]/np.power((1+params[0]*ai*m), 1/params[0])
    qf = np.insert(q, 0, params[2])
    n = (np.power(params[2], params[0])*(np.power(params[2],
         (1-params[0]))-np.power(qf, (1-params[0])))/((1-params[0])*ai))
    forecast_arps = np.diff(n, axis=0)
    q0_exp = params[2]/np.power((1+params[0]*ai*t), 1/params[0])
    qf_exp = q0_exp*np.exp(-af*m_exp)
    n_exp = (q0_exp-qf_exp)/af
    forecast_exp = np.diff(n_exp, axis=0)
    forecast = np.concatenate([forecast_arps, forecast_exp])
    if any(forecast < min_rate):
        end_of_life = np.argmax(forecast < min_rate)
    else:
        end_of_life = len(forecast)
    return end_of_life

def arps_fit(params, dmin, min_rate):
    if abs(params[0] - 1) < 0.0001:
        params[0] = 0.999
    ai = ((1/params[0])*(np.power((1-params[1]), -params[0])-1))/365
    df = 1-np.power((1+params[0]*-np.log(1-dmin)), -1/params[0])
    af = (1/params[0])*(np.power((1-df), -params[0])-1)/365
    t = int((ai-af)/(params[0]*ai*af))
    m = np.arange(1, t+1)
    m_exp = np.arange(1, 45625-t)
    q = params[2]/np.power((1+params[0]*ai*m), 1/params[0])
    qf = np.insert(q, 0, params[2])
    n = (np.power(params[2], params[0])*(np.power(params[2],
         (1-params[0]))-np.power(qf, (1-params[0])))/((1-params[0])*ai))
    forecast_arps = np.diff(n, axis=0)
    q0_exp = params[2]/np.power((1+params[0]*ai*t), 1/params[0])
    qf_exp = q0_exp*np.exp(-af*m_exp)
    n_exp = (q0_exp-qf_exp)/af
    forecast_exp = np.diff(n_exp, axis=0)
    forecast = np.concatenate([forecast_arps, forecast_exp])
    if any(forecast < min_rate):
        end_of_life = np.argmax(forecast < min_rate)
    else:
        end_of_life = len(forecast)
    forecast = forecast[:end_of_life]
    if len(forecast) < 18250:
        np.concatenate([forecast, np.zeros(18250-len(forecast))])
    return forecast

def residuals(params, y, dmin, min_rate, method='diff'):
    fcst = arps_fit(params, dmin, min_rate)[:len(y)]
    if len(fcst) < len(y):
        fcst = np.concatenate([fcst, np.zeros(len(y) - len(fcst))])
    if method == 'beta':
        beta_x = np.linspace(0.01, 0.99, len(y))
        cost = np.multiply(y - fcst, beta.pdf(beta_x, .98, 0.8))
        cost = np.divide(cost[y > 0], y[y > 0])
    if method == 'diff':
        cost = y - fcst
    if method == 'frac':
        cost = y / fcst
    return cost

def load_pdp_curve(f, start=None, end=None):
    eng = engine()
    versions = prod_forecasts.objects.values_list('version', flat=True).filter(idp=f.idp.values[0]).order_by('version').distinct()
    version = versions[int(f.forecast_version.values[0])-1]
    if start is not None and end is not None:
        query = str('select * from olive_prod_forecasts where idp = \'' + f.idp.values[0] + '\' '
                    'and version = \'' + str(version) + '\' and prod_date >= \'' + str(start) + '\' and prod_date <= \'' + str(end) + '\'')
    elif start is not None and end is None:
        query = str('select * from olive_prod_forecasts where idp = \'' + f.idp.values[0] + '\' '
                    'and version = \'' + str(version) + '\' and prod_date >= \'' + str(start) + '\'')
    elif start is None and end is not None:
        query = str('select * from olive_prod_forecasts where idp = \'' + f.idp.values[0] + '\' '
                    'and version = \'' + str(version) + '\' and prod_date <= \'' + str(end) + '\'')
    else:
        query = str('select * from olive_prod_forecasts where idp = \'' + f.idp.values[0] + '\' and version = \'' + str(version) + '\'')
    pdp_curve = pd.read_sql(query, eng)
    pdp_curve.sort_values(by=['prod_date'], inplace=True)
    return pdp_curve

def load_production(idp, prod_start_date):
    eng = create_engine(str('mssql+turbodbc://:@.:1433/CC?driver=ODBC+Driver+17+for+SQL+Server'), poolclass=NullPool)
    query = str('select * from cc_dailyprodop where ariesid = \'' + idp + '\' '
                'and Date >= \'' + str(prod_start_date) + '\'')
    prod = pd.read_sql(query, eng)
    prod.sort_values(by=['Date'], inplace=True)
    return prod

def end_date(framework):
    return (framework.effective_date + relativedelta(years=+framework.life) - relativedelta(days=+1))

def connect():
    options = make_options(prefer_unicode=True)
    return con(driver='ODBC Driver 17 for SQL Server',
                server='',
                port='1433',
                database='',
                uid='',
                pwd='',
                turbodbc_options=options)

def load_csv(file, owner, version, model):
    dtypes = {'CharField': str, 'IntegerField': float, 'FloatField': float, 'DateField': 'datetime64[ns]', 'DateTimeField': 'datetime64[ns]', 'BooleanField': bool}
    fields = [f.name for f in model._meta.get_fields()]
    if 'id' in fields:
        fields.remove('id')
    dtypes = {f: dtypes[model._meta.get_field(f).get_internal_type()] for f in fields}
    f = pd.read_csv(file, header=0)
    f['owner'] = owner
    f['version'] = pd.Timestamp(version).tz_convert('UTC')
    f['version'] = f['version'].dt.tz_localize(None)
    f.drop(columns=['id'], inplace=True)
    f.fillna(value=np.nan, inplace=True)
    for col in f.columns:
        f.loc[:, col] = f[col].astype(dtypes[col])
    if 'forecast_version' in f.columns:
        scenario = f['scenario'].unique()[0]
        tc_list = f.loc[f.forecast_type == 'type', 'forecast_name'].unique()
        pdp_list = f.loc[f.forecast_type != 'type', 'idp'].values
        tc_versions = type_curves.objects.filter(name__in=tc_list).values('name').distinct().annotate(forecast_version=Count('version', distinct=True))
        tc_versions_df = pd.DataFrame(tc_versions)
        pdp_versions = prod_forecasts.objects.filter(idp__in=pdp_list, scenario=scenario).annotate(forecast_version=Count('version', distinct=True)).values('idp', 'forecast_version').distinct()
        pdp_versions_df = pd.DataFrame(pdp_versions)
        for tc in tc_list:
            f.loc[(f.forecast_name == tc) & (pd.isna(f.forecast_version)), 'forecast_version'] = tc_versions_df.loc[tc_versions_df.name == tc, 'forecast_version'].values[0]
        for pdp in pdp_list:
            if pdp_versions_df.empty:
                f.loc[(f.idp == pdp)  & (pd.isna(f.forecast_version)), 'forecast_version'] = 1
            else:
                if len(f.loc[(f.idp == pdp)  & (pd.isna(f.forecast_version)), 'forecast_version']) > 0:
                    f.loc[(f.idp == pdp)  & (pd.isna(f.forecast_version)), 'forecast_version'] = pdp_versions_df.loc[pdp_versions_df.idp == pdp, 'forecast_version'].values[0] + 1
    
    placeholders = placeholders = ', '.join('?' * len(f.columns))
    query = 'insert into ' + model.objects.model._meta.db_table + ' ('  + ', '.join(f for f in fields) + ') values (' + placeholders + ')'
    conn = connect()
    cursor = conn.cursor()
    cursor.executemany(query, csv_iter(f))
    conn.commit()
    cursor.close()

def csv_iter(df):
    for i in range(len(df)):
        r = df.loc[i].values
        for idx, j in enumerate(r):
            if j == 'nan':
                r[idx] = None
            if pd.isnull(j):
                r[idx] = None
        yield r

def load_price_deck(build):
    print('loading pricing')
    gas_check = True
    while gas_check:
        load_check = True
        while load_check:
            price_deck = pd.DataFrame(pricing.objects.filter(scenario=build.scenario['price_deck'], version=build.scenario['price_deck_version']).values())
            check = pd.DataFrame(pricing.objects.filter(scenario=build.scenario['price_deck'], version=build.scenario['price_deck_version']).values())
            if not all(price_deck.hh == check.hh):
                print('gas load error')
            elif not all(price_deck.wti == check.wti):
                print('oil load error')
            elif not all(price_deck.ngl == check.ngl):
                print('ngl load error')
            elif not all(price_deck.cig == check.cig):
                print('cig load error')
            elif not all(price_deck.nwr == check.nwr):
                print('nwr load error')  
            else:
                price_deck['prod_date'] = price_deck['prod_date'].astype('datetime64[ns]')
                print('successful price load')
                load_check = False

        min_prices = price_deck[price_deck.prod_date == price_deck.prod_date.min()]
        max_prices = price_deck[price_deck.prod_date == price_deck.prod_date.max()]

        if max_prices.hh.values[0] > 7:
            print('max gas price failed verification, reloading')
            gas_check = True
            continue

        p = price_deck[(price_deck.prod_date >= build.effective_date) &
                            (price_deck.prod_date <= build.end_date)]

        max_date = price_deck.prod_date.max()
        if build.end_date > max_date:
            date_range = pd.date_range(start=max_date, end=build.end_date, closed='right')

            temp_price_df = pd.DataFrame({'prod_date': date_range,
                                        'hh': np.empty(len(date_range)),
                                        'wti': np.empty(len(date_range)),
                                        'ngl': np.empty(len(date_range)),
                                        'cig': np.empty(len(date_range)),
                                        'nwr': np.empty(len(date_range))
                                        })
            temp_price_df['hh'] = max_prices.hh.values[0]
            temp_price_df['wti'] = max_prices.wti.values[0]
            temp_price_df['ngl'] = max_prices.ngl.values[0]
            temp_price_df['cig'] = max_prices.cig.values[0]
            temp_price_df['nwr'] = max_prices.nwr.values[0]
            price_deck = pd.concat([p, temp_price_df])
        else:
            price_deck = p

        if any(price_deck.hh > 7):
            print('gas price failed verification')
        elif any(price_deck.wti > 85):
            print('oil price verification failed')
        elif any(price_deck.ngl > 85):
            print('ngl price verification failed')
        elif any(price_deck.cig > 0.40):
            print('cig price verification failed')
        elif any(price_deck.nwr > 1.10):
            print('nwr price verification failed')
        else:
            print('price deck verification succeeded')
            gas_check = False

    return price_deck

def padding_df(df, padding):
    pad_df = pd.DataFrame({c: np.zeros(padding) for c in df.columns})
    pad_df.prod_date = pd.NaT
    return pad_df

def event(l):
    return int(-math.log(1.0 - random.random()) / l)

def event_list(l, d, n):
    events = np.zeros(len(n), dtype=bool)
    e = event(l)
    d = int(d)
    for i in n:
        if i == e:
            if e + d > len(n):
                events[e:] = 1
            else:
                events[e:e+d] = 1
            e = event(l)
            if e < d:
                e = int(i) + d + 1
            else:
                e = int(i) + e
    return events

def econ_parser(param_name, param, effective_date, prod_start_date, end_date):
    ops = {'>': operator.gt,
           '<': operator.lt,
           '>=': operator.ge,
           '<=': operator.le}
    tmp_econ_df = pd.DataFrame(columns=['prod_date', 'eomonth', param_name, 'unit'])
    date_range = pd.date_range(effective_date, end_date, freq='D')
    prod_start_date = pd.Timestamp(prod_start_date.date())
    tmp_econ_df.prod_date = date_range
    eomonth = []
    for d in date_range:
        day = calendar.monthrange(d.year, d.month)[1]
        eomonth.append(datetime.datetime(d.year, d.month, day))
    tmp_econ_df.eomonth = pd.to_datetime(pd.Series(eomonth))
    try:
        param = float(param)
        tmp_econ_df[param_name] = param
        tmp_econ_df['unit'] = 'per'
        tmp_econ_df.loc[tmp_econ_df.prod_date < prod_start_date, param_name] = 0.0
        return tmp_econ_df
    except:
        try:
            param = float(param.strip('%'))/100.
            tmp_econ_df[param_name] = param
            tmp_econ_df['unit'] = 'pct'
            tmp_econ_df.loc[tmp_econ_df.prod_date < prod_start_date, param_name] = 0.0
            return tmp_econ_df
        except:
            params = param.split(' ')
            _iter = enumerate(params)
            prior_date = None
            for i, p in _iter:
                if i == 0:

                    if '%' in p:
                        try:
                            start_val = float(p.strip('%'))/100.
                            start_unit = 'pct'
                            continue
                        except:
                            print('first value must be float, provided', p.strip('%'))
            
                        return
                    else:
                        try:
                            start_val = float(p)
                            start_unit = 'per'
                            continue
                        except:
                            print('first value must be float, provided', p)
            
                        return

                else:
                    if p == 'if':
                        try:
                            op_val = params[i+1]
                            date_val= params[i+2]
                            else_op = params[i+3]
                            other_val = params[i+4]
                        except:
                            print('missing values after if')
            
                            return
                        if op_val not in ops.keys():
                            print('invalid operator')
            
                            return
                        try:
                            date_val = pd.Timestamp(date_val)
                        except:
                            print('invalid date value', date_val)
            
                            return
                        if else_op != 'else':
                            print('invalid syntax', else_op)
            
                            return
                        if '%' in other_val:
                            try:
                                other_val = float(other_val.strip('%'))/100.
                                other_unit = 'pct'
                            except:
                                print('invalid alternate value', other_val.strip('%'))
                
                                return
                        else:
                            try:
                                other_val = float(other_val)
                                other_unit = 'per'
                            except:
                                print('invalid alternate value', other_val)
                
                                return
                        if not ops[op_val](prod_start_date, date_val):
                            start_val = other_val
                            start_unit = other_unit
                        tmp_econ_df[param_name] = start_val
                        tmp_econ_df.unit = start_unit
                        _ = next(_iter)
                        _ = next(_iter)
                        _ = next(_iter)
                        _ = next(_iter)

                    if p == 'until':
                        try:
                            date_val = params[i+1]
                            then_op = params[i+2]
                            next_val = params[i+3]
                        except:
                            print('missing values after until')
            
                            return
                        try:
                            date_val = pd.Timestamp(date_val)
            
                        except:
                            print('invalid date value', date_val)
            
                            return
                        if then_op != 'then':
                            print('invalid syntax', then_op)
            
                            return
                        if '%'  in next_val:
                            try:
                                next_val = float(next_val.strip('%'))/100.
                                next_unit = 'pct'
                            except:
                                print('invalid next value', next_val.strip('%'))
                
                        else:
                            try:
                                next_val = float(next_val)
                                next_unit = 'per'
                            except:
                                print('invalid next value', next_val)
                
                                return
                        mask = operator.le(date_range, date_val)
                        antimask = ~mask
                        if prior_date is not None:
                            mask = (operator.gt(date_range, prior_date) & operator.le(date_range, date_val))
                            antimask = operator.gt(date_range, date_val)
                        tmp_econ_df.loc[mask, param_name] = start_val
                        tmp_econ_df.loc[mask, 'unit'] = start_unit
                        tmp_econ_df.loc[antimask, param_name] = next_val
                        tmp_econ_df.loc[antimask, 'unit'] = next_unit
                        start_val = next_val
                        start_unit = next_unit
                        prior_date = date_val
                        _ = next(_iter)
                        _ = next(_iter)
                        _ = next(_iter)

                    if p == 'for':
                        try:
                            time_val = params[i+1]
                            unit_val = params[i+2]
                            then_op = params[i+3]
                            next_val = params[i+4]
                        except:
                            print('missing values after for')
            
                        try:
                            time_val = int(time_val)
                        except:
                            print('invalid time value', time_val)
            
                            return
                        if unit_val not in ('d', 'day', 'days',
                                            'mo', 'mos', 'month', 'months',
                                            'y', 'yr', 'yrs', 'year', 'years'):
                            print('unknown date unit', unit_val)
            
                            return
                        if '%'  in next_val:
                            try:
                                next_val = float(next_val.strip('%'))/100.
                                next_unit = 'pct'
                            except:
                                print('invalid next value', next_val.strip('%'))
                
                        else:
                            try:
                                next_val = float(next_val)
                                next_unit = 'per'
                            except:
                                print('invalid next value', next_val)
                
                                return
                        if unit_val in ('d', 'day', 'days'):
                            delta = relativedelta(days=time_val)
                        if unit_val in ('m', 'mo', 'mos', 'month', 'months'):
                            delta = relativedelta(months=time_val)
                        if unit_val in ('y', 'yr', 'yrs', 'year', 'years'):
                            delta = relativedelta(years=time_val)
                        end_date = prod_start_date + delta
                        mask = date_range < end_date
                        tmp_econ_df.loc[mask, param_name] = start_val
                        tmp_econ_df.loc[mask, 'unit'] = start_unit
                        tmp_econ_df.loc[~mask, param_name] = next_val
                        tmp_econ_df.loc[~mask, 'unit'] = next_unit
                        start_val = next_val
                        start_unit = next_unit
                        _ = next(_iter)
                        _ = next(_iter)
                        _ = next(_iter)
                        _ = next(_iter)

    tmp_econ_df.loc[tmp_econ_df.prod_date < prod_start_date, param_name] = 0.0
    return tmp_econ_df

def misc_capex_parser(param, effective_date, end_date):
    tmp_df = pd.DataFrame(columns=['prod_date', 'eomonth', 'inv_g_misc'])
    date_range = pd.date_range(effective_date, end_date, freq='D')
    tmp_df.prod_date = date_range
    eomonth = []
    for d in date_range:
        day = calendar.monthrange(d.year, d.month)[1]
        eomonth.append(datetime.datetime(d.year, d.month, day))
    tmp_df.eomonth = pd.to_datetime(pd.Series(eomonth))
    tmp_df.inv_g_misc = 0.0
    params = param.split(',')
    for p in params:
        if p[0] == ' ':
            p = p[1:]
        p_split = p.split(' ')
        if len(p_split) == 1:
            try:
                p_split = float(p_split[0])
                if p_split > 0.001:
                    print('misc capex with no date provided', p_split)
    
                    return tmp_df
                else:
                    return tmp_df
            except:
                print('bad misc capex', p_split)

                return tmp_df
        if len(p_split) > 1:
            try:
                p_cap = float(p_split[0])
            except:
                print('capex not float')

                return tmp_df
            if p_split[1] != 'on':
                print('invalid syntax, missing \'on\'')

                return tmp_df
            try:
                p_date = pd.Timestamp(p_split[2])
            except:
                print('invalid date')
                return tmp_df
            tmp_df.loc[tmp_df.prod_date == p_date, 'inv_g_misc'] = p_cap
    return tmp_df

def aban_capex_parser(param, end_of_life, effective_date, end_date):
    tmp_df = pd.DataFrame(columns=['prod_date', 'eomonth', 'inv_g_aban'])
    date_range = pd.date_range(effective_date, end_date, freq='D')
    tmp_df.prod_date = date_range
    eomonth = []
    for d in date_range:
        day = calendar.monthrange(d.year, d.month)[1]
        eomonth.append(datetime.datetime(d.year, d.month, day))
    tmp_df.eomonth = pd.to_datetime(pd.Series(eomonth))
    tmp_df.inv_g_aban = 0.0
    try:
        p_cap = float(param)
        tmp_df.loc[tmp_df.prod_date == end_of_life, 'inv_g_aban'] = p_cap
    except:
        params = param.split(' ')
        try:
            p_cap = float(params[0])
        except:
            print('capex not float')
        if params[1] != 'after':
            print('invalid syntax, missing \'after\'')
            return
        try:
            time_delta = int(params[2])
        except:
            print('bad time delta', params[2])
            return
        unit_val = params[3]
        if unit_val in ('d', 'day', 'days'):
            delta = relativedelta(days=time_delta)
        elif unit_val in ('m', 'mo', 'mos', 'month', 'months'):
            delta = relativedelta(months=time_delta)
        elif unit_val in ('y', 'yr', 'yrs', 'year', 'years'):
            delta = relativedelta(years=time_delta)
        else:
            print('invalid timestep', unit_val)
            return
        aban_date = pd.Timestamp(end_of_life) + delta
        tmp_df.loc[tmp_df.prod_date == aban_date, 'inv_g_aban'] = p_cap
    return tmp_df

def min_life_parser(param, first_active_date, pmax_date, effective_date, end_date):
    tmp_df = pd.DataFrame(columns=['prod_date', 'eomonth', 'min_life'])
    date_range = pd.date_range(effective_date, end_date, freq='D')
    tmp_df.prod_date = date_range
    eomonth = []
    for d in date_range:
        day = calendar.monthrange(d.year, d.month)[1]
        eomonth.append(datetime.datetime(d.year, d.month, day))
    tmp_df.eomonth = pd.to_datetime(pd.Series(eomonth))
    tmp_df.min_life = 1
    if param in ('loss ok', 'lossok', 'loss_ok'):
        return tmp_df
    try:
        params = param.split(' ')
    except:
        print('invalid syntax, can\'t split', param)
        return
    try:
        time_delta = int(params[0])
    except:
        print('time value not int', params[0])
        return
    unit_val = params[1]
    if unit_val in ('d', 'day', 'days'):
        delta = relativedelta(days=time_delta)
    elif unit_val in ('m', 'mo', 'mos', 'month', 'months'):
        delta = relativedelta(months=time_delta)
    elif unit_val in ('y', 'yr', 'yrs', 'year', 'years'):
        delta = relativedelta(years=time_delta)
    else:
        print('invalid timestep', unit_val)
        return
    if len(params) > 2:
        if params[2] != 'after':
            print('invalid syntax, missing \'after\'')
            return                
        if len(params) == 5:
            p = params[3] + ' ' + params[4]
        else:
            p = params[3]
        if p in ('effective date', 'eff', 'eff date',
                    'effective_date', 'eff', 'eff_date'):
            min_date = effective_date + delta
            if pmax_date < min_date:
                end_of_life = min_date
            else:
                end_of_life = pmax_date
        if p == 'life':
            end_of_life = pmax_date + delta
    else:
        min_date = effective_date + delta
        if pmax_date < min_date:
            end_of_life = min_date
        else:
            end_of_life = pmax_date
    tmp_df.loc[tmp_df.prod_date < first_active_date, 'min_life'] = 0
    tmp_df.loc[tmp_df.prod_date > end_of_life, 'min_life'] = 0
    return tmp_df

def xirr(cf, daily_output):
    tol = 10
    disc_cf = cf.sum()
    max_iter = 1000

    if cf.sum() > 0:
        step = 0.01
        iter = 0
        guess = 0.0
        while abs(disc_cf) > tol:
            disc_cf = xnpv(cf, guess, daily_output)
            if abs(disc_cf) > tol:
                if disc_cf > 0:
                    guess += step
                else:
                    guess -= step
                    step /= 2.0
                iter += 1
            if iter == max_iter:
                break
        return guess
    else:
        step = -0.01
        iter = 0
        guess = 0.0
        while abs(disc_cf) > tol:
            disc_cf = xnpv(cf, guess, daily_output)
            if abs(disc_cf) > tol:
                if disc_cf > 0:
                    guess -= step
                    step /= 2.0
                else:
                    guess += step
                iter += 1
            if iter == max_iter:
                break
            if guess <= -1.0:
                return -1
        return guess

def npv(cf, d=0.1):
    return cf/(1+d)**(np.arange(cf.shape[0])/365.25)

def xnpv(cf, d=0.1, daily_output=False):
    if daily_output:
        return sum(cf/(1+d)**(np.arange(cf.shape[0])/365.25))
    else:
        return sum(cf/(1+d)**(np.arange(cf.shape[0])/12))

def save_output(df):
    placeholders = placeholders = ', '.join('?' * len(df.columns))
    query = 'insert into ' + output.objects.model._meta.db_table + ' ('  + ', '.join(f for f in df.columns) + ') values (' + placeholders + ')'
    conn = connect()
    cursor = conn.cursor()
    cursor.executemany(query, df.itertuples(index=False, name=None))
    conn.commit()
    cursor.close()

def save_onelines(df):
    if 'id' in df.columns:
        df.drop(columns=['id'], inplace=True)
    placeholders = placeholders = ', '.join('?' * len(df.columns))
    query = 'insert into ' + output_onelines.objects.model._meta.db_table + ' ('  + ', '.join(f for f in df.columns) + ') values (' + placeholders + ')'
    conn = connect()
    cursor = conn.cursor()
    cursor.executemany(query, oneline_iter(df))
    conn.commit()
    cursor.close()

def pct_rank_output(p_list, idp, scenario, version):
    eng = engine()
    query = str('select top (1) * from olive_output '
                'where idp = \'' + idp + '\' and scenario = \'' + scenario + '\' and version = \'' + str(version) + '\' ')
    d = pd.read_sql(query, eng)
    query = str('select prod_date, time_on, gas_eur, oil_eur, gross_gas_mult, gross_oil_mult, gross_water_mult, gross_gas, '
                'gross_oil, gross_water, ngl_yield, btu, shrink, wi, nri, royalty, net_gas, net_oil, net_ngl, '
                'net_mcfe, royalty_gas, royalty_oil, royalty_ngl, royalty_mcfe, hh, wti, ngl, cig, nwr, '
                'gas_price_adj, oil_price_adj, ngl_price_adj, realized_gas_price, realized_oil_price, '
                'realized_ngl_price, net_gas_rev, net_oil_rev, net_ngl_rev, net_total_rev, gross_drill_capex, '
                'gross_compl_capex, gross_misc_capex, gross_aban_capex, gross_total_capex, net_drill_capex, '
                'net_compl_capex, net_misc_capex, net_aban_capex, net_total_capex, fixed_cost, alloc_fixed_cost, '
                'var_gas_cost, var_oil_cost, var_water_cost, doe, gtp, taxes, loe, cf, fcf, pv1, pv2, pv3, '
                'pv4, pv5, pv6, pv7, pv8, pv9, pv10, active '
                'from olive_output '
                'where idp = \'' + idp + '\' and scenario = \'' + scenario + '\' and version = \'' + str(version) + '\' '
                'order by prod_date')
    e = pd.read_sql(query, eng)
    prod_date = e.prod_date.unique()
    df_list = []
    for p in p_list:
        t = pd.DataFrame(columns=d.columns)
        t.drop(columns=['id'], inplace=True)
        t['prod_date'] = prod_date
        for f in d.columns:
            if f in ('scenario', 'idp', 'name', 'budget_type', 'short_pad', 'pad', 'rig', 'field', 'gas_basis', 'gas_adj_unit',
                    'oil_adj_unit', 'ngl_adj_unit', 'owner', 'version',  'pv1_rate', 'pv2_rate', 'pv3_rate', 'pv4_rate',
                    'pv5_rate', 'pv6_rate', 'pv7_rate', 'pv8_rate', 'pv9_rate', 'pv10_rate'):
                t[f] = d[f].unique()[0]
            elif f == 'type':
                t[f] = str('p' + str(int(p*100)))
            elif f == 'simulation':
                t[f] = 0
            elif f in ('prod_date', 'id'):
                continue
            else:
                t[f] = e.groupby(by='prod_date')[f].quantile(q=p).values
        df_list.append(t)
    df = pd.concat(df_list)
    save_output(df)

# def pct_rank_output(p, idp, scenario, version):
#     eng = engine()
#     print(1)
#     query = str('select * from olive_output '
#                 'where idp = \'' + idp + '\' and scenario = \'' + scenario + '\' and version = \'' + str(version) + '\' '
#                 'order by prod_date')
#     d = pd.read_sql(query, eng)
#     print(2)
#     prod_date = d.prod_date.unique()
#     t = pd.DataFrame(columns=d.columns)
#     t.drop(columns=['id'], inplace=True)
#     t['prod_date'] = prod_date
#     print(3)
#     for f in d.columns:
#         if f in ('scenario', 'idp', 'name', 'budget_type', 'short_pad', 'pad', 'rig', 'field', 'gas_basis', 'gas_adj_unit',
#                  'oil_adj_unit', 'ngl_adj_unit', 'owner', 'version',  'pv1_rate', 'pv2_rate', 'pv3_rate', 'pv4_rate',
#                  'pv5_rate', 'pv6_rate', 'pv7_rate', 'pv8_rate', 'pv9_rate', 'pv10_rate'):
#             t[f] = d[f].unique()[0]
#         elif f == 'type':
#             t[f] = str('p' + str(int(p*100)))
#         elif f == 'simulation':
#             t[f] = 0
#         elif f in ('prod_date', 'id'):
#             continue
#         else:
#             t[f] = d.groupby(by='prod_date')[f].quantile(q=p).values
#     print(4)
#     save_output(t)
#     print(5)

def pct_rank_onelines(p_list, idp, scenario, version):
    eng = engine()
    query = str('select * from olive_output_onelines '
                'where idp = \'' + idp + '\' and scenario = \'' + scenario + '\' and version = \'' + str(version) + '\'')
    d = pd.read_sql(query, eng)
    df_list = []
    for p in p_list:
        t = pd.DataFrame(columns=d.columns)
        t.drop(columns=['id'], inplace=True)
        t['idp'] = [idp]
        for f in d.columns:
            if f in ('scenario', 'name', 'budget_type', 'short_pad', 'pad', 'rig', 'field', 'gas_basis', 'gas_adj_unit',
                    'oil_adj_unit', 'ngl_adj_unit', 'owner', 'version',  'pv1_rate', 'pv2_rate', 'pv3_rate', 'pv4_rate',
                    'pv5_rate', 'pv6_rate', 'pv7_rate', 'pv8_rate', 'pv9_rate', 'pv10_rate'):
                t[f] = d[f].unique()[0]
            elif f == 'type':
                t[f] = str('p' + str(int(p*100)))
            elif f == 'simulation':
                t[f] = 0
            elif f in ('prod_date', 'id', 'idp'):
                continue
            else:
                t[f] = d[f].quantile(q=p)
        df_list.append(t)
    df = pd.concat(df_list)
    df.reset_index(inplace=True, drop=True)
    save_onelines(df)

def oneline_iter(df):
    for i in range(len(df)):
        r = df.loc[i].values
        for idx, j in enumerate(r):
            if j == 'nan':
                r[idx] = None
            if pd.isnull(j):
                r[idx] = None
        yield r

def timer(start, stop):
    runtime = (stop - start)
    if runtime < 60:
        print('execution time:',
              round(runtime, 2), 'seconds')
    if runtime >= 60 and runtime < 3600:
        print('execution time:',
              round(runtime/60, 2), 'minutes')
    if runtime >= 3600:
        print('execution time:',
              round(runtime/3600, 2), 'hours')

def is_monthly(d):
    if len(d.dt.day.unique()) == 1:
        return True
    else:
        return False

def convert_to_daily(pdp, df):
    df_list = []
    if 'Gas (MCF/M)' in df.columns:
        df.rename(columns={'Chosen ID': 'idp', 'Date': 'prod_date', 'Gas (MCF/M)': 'gas', 
                            'Oil (BBL/M)': 'oil', 'Water (BBL/M)': 'water'}, inplace=True)
        df.loc[:, 'prod_date'] = df.prod_date.astype('datetime64[ns]')
        df.sort_values(by=['idp', 'prod_date'], inplace=True)
    if 'Gas Forecast (MCF/M)' in df.columns:
        df.rename(columns={'Chosen ID': 'idp', 'Date': 'prod_date', 'Gas Forecast (MCF/M)': 'gas', 
                            'Oil Forecast (BBL/M)': 'oil', 'Water Forecast (BBL/M)': 'water'}, inplace=True)
        print('production included')
        df.loc[:, 'prod_date'] = df.prod_date.astype('datetime64[ns]')
        df.sort_values(by=['idp', 'prod_date'], inplace=True)
        for p in df.idp.unique():
            prod_start = df.loc[df.idp == p, 'Oil Production (BBL/M)'].notna().idxmax()
            prod_end = df.loc[df.idp == p, 'Oil Production (BBL/M)'].notna()[::-1].idxmax()
            df.loc[prod_start:prod_end, 'oil'] = df.loc[prod_start:prod_end, 'Oil Production (BBL/M)']

            prod_start = df.loc[df.idp == p, 'Gas Production (MCF/M)'].notna().idxmax()
            prod_end = df.loc[df.idp == p, 'Gas Production (MCF/M)'].notna()[::-1].idxmax()
            df.loc[prod_start:prod_end, 'gas'] = df.loc[prod_start:prod_end, 'Gas Production (MCF/M)']

            prod_start = df.loc[df.idp == p, 'Water Production (BBL/M)'].notna().idxmax()
            prod_end = df.loc[df.idp == p, 'Water Production (BBL/M)'].notna()[::-1].idxmax()
            df.loc[prod_start:prod_end, 'water'] = df.loc[prod_start:prod_end, 'Water Production (BBL/M)']
    df.fillna(0, inplace=True)

    for idp in df.idp.unique():
        df_idp = df.loc[df.idp == idp, :]
        temp_df = pd.DataFrame(columns=df.columns)
        datehelper = pd.DataFrame(columns=['prod_date', 'month', 'year', 'days'])
        datehelper['prod_date'] = df_idp.prod_date
        datehelper['month'] = df_idp.prod_date.dt.month
        datehelper['year'] = df_idp.prod_date.dt.year
        datehelper['days'] = (df_idp.prod_date + MonthEnd(1)).dt.day
        min_date = df_idp.prod_date.min()
        min_date = datetime.date(min_date.year, min_date.month, 1)
        max_date = df_idp.prod_date.max()
        max_date = datetime.date(max_date.year, max_date.month, 1)
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        df_idp['month'] = df_idp.prod_date.dt.month
        df_idp['year'] = df_idp.prod_date.dt.year
        temp_df['prod_date'] = date_range
        temp_df['month'] = temp_df.prod_date.dt.month
        temp_df['year'] = temp_df.prod_date.dt.year
        temp_df['idp'] = idp
        temp_df['scenario'] = pdp.scenario
        temp_df['version'] = pdp.version
        temp_df['owner'] = pdp.owner
        temp_df = pd.merge(temp_df, datehelper, on=['month', 'year'])
        temp_df = pd.merge(temp_df, df_idp, on=['month', 'year'])
        temp_df['gas_x'] = temp_df['gas_y'] / temp_df['days']
        temp_df['oil_x'] = temp_df['oil_y'] / temp_df['days']
        temp_df['water_x'] = temp_df['water_y'] / temp_df['days']
        temp_df = temp_df[['scenario', 'idp_x', 'prod_date_x', 'gas_x', 'oil_x', 'water_x', 'version', 'owner']]
        temp_df.rename(columns={'idp_x': 'idp', 'prod_date_x': 'prod_date',
                                'gas_x': 'gas', 'oil_x': 'oil', 'water_x': 'water'}, inplace=True)
        df_list.append(temp_df)

    df = pd.concat(df_list)
    return df
