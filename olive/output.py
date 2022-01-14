from olive.utils import *
from olive.generate import cash_flows, calculate_percentiles
import pandas as pd
from olive.models import properties, projects, frameworks
import multiprocessing as mp
from django import db
from olive.schedule import Schedule
import time


class Build():
    def __init__(self, scenario, owner, low_pct=None, mid_pct=None, high_pct=None):

        self.scenario = scenario
        self.name = scenario['scenario']
        self.scenario_version = scenario['version']
        self.scenario_owner = scenario['owner']
        self.owner = owner
        self.version = pd.Timestamp.utcnow()
        print('output version:', self.version)
        self.schedule = None
        self.probability = None
        if low_pct is not None:
            self.low_pct = [float(low_pct) if float(low_pct) < 1 else float(low_pct)/100][0]
        else:
            self.low_pct = None
        if mid_pct is not None:
            self.mid_pct = [float(mid_pct) if float(mid_pct) < 1 else float(mid_pct)/100][0]
        else:
            self.mid_pct = None
        if high_pct is not None:
            self.high_pct = [float(high_pct) if float(high_pct) < 1 else float(high_pct)/100][0]
        else:
            self.high_pct = None
        self.load_properties()

    def load_properties(self):
        print('loading properties')
        if self.scenario['project'] is None:
            self.properties = pd.DataFrame(properties.objects.filter(scenario=self.scenario['properties'],
                                                                    version=self.scenario['properties_version']).values())
        else:
            idp_list = projects.objects.values_list('idp', flat=True).filter(scenario=self.scenario['project'], version=self.scenario['project_version'])
            self.properties = pd.DataFrame(properties.objects.filter(scenario=self.scenario['properties'],
                                                                     version=self.scenario['properties_version'], idp__in=idp_list).values())
        self.properties_list = self.properties.idp.values

    def load_probability(self, sim=None):
        print('loading probability')
        if self.scenario['probability'] is not None and sim is not None:
            self.probability = load_probabilities(properties=self.properties, scenario=self.scenario['probability'], probability_version=self.scenario['probability_version'],
                                                  owner=self.owner, version=self.version, sim=sim)
            self.risk = self.probability['risk']
            self.uncertainty = self.probability['uncertainty']
        else:
            self.probability = load_probabilities(properties=self.properties, scenario=None, probability_version=None, owner=self.owner, version=self.version, sim=0)
            self.risk = self.probability['risk']
            self.uncertainty = self.probability['uncertainty']

    def create_schedule(self, file):
        self.load_probability()
        if self.scenario['schedule'] is not None:
            print('creating schedule')
            self.schedule = Schedule(file=file, scenarios=self.scenario,
                                     owner=self.owner, probability=self.probability, version=self.version, type='deterministic')

    def load_schedule(self, sim=None):
        print('loading schedule')
        if not sim:
            if self.scenario['schedule'] is not None:
                self.schedule_dates = pd.DataFrame(schedules.objects.filter(scenario=self.scenario['schedule'],
                                                                            version=self.scenario['schedule_version']).values())
                self.schedule_file = pd.DataFrame(schedule_file.objects.filter(scenario=self.scenario['schedule'],
                                                                            version=self.scenario['schedule_version']).values())
        else:
            self.schedule = Schedule(file=None, scenarios=self.scenario, owner=self.owner, probability=self.probability,
                                     version=self.version, type='probabilistic', simulation=sim)
            self.schedule_dates = self.schedule.schedule_dates
            self.schedule_file = pd.DataFrame(schedule_file.objects.filter(scenario=self.scenario['schedule'],
                                                                        version=self.scenario['schedule_version']).values())

    def load_framework(self):
        print('loading framework')
        sql_load = frameworks.objects.filter(scenario=self.scenario['framework'], version=self.scenario['framework_version']).values()[0]

        self.effective_date = pd.Timestamp(sql_load['effective_date'])
        self.life = sql_load['life']
        if self.life > 50:
            print('max life is 50 years, setting life to 50')
            self.life = 50
        self.end_date = end_date(self)
        self.date_range = pd.date_range(self.effective_date,
                                        self.end_date, freq='d')
        self.daily_output = sql_load['daily_output']
        self.pv_spread = sql_load['pv_spread']
        if self.pv_spread is None:
            self.pv_spread = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
            print('using default pv spread', ', '.join(self.pv_spread))
        else:
            self.pv_spread = self.pv_spread.split(',')
            if len(self.pv_spread) > 10:
                print('only 10 pv spread values allowed, dropping', ', '.join(self.pv_spread[10:]))
                self.pv_spread = self.pv_spread[:10]

    def load_forecasts(self):
        print('loading forecasts')
        self.forecasts = pd.DataFrame(forecasts.objects.filter(scenario=self.scenario['forecast'], version=self.scenario['forecast_version']).values())

    def load_economics(self):
        print('loading economics')
        self.economics = pd.DataFrame(economics.objects.filter(scenario=self.scenario['economics'], version=self.scenario['economics_version']).values())

    def load_pricing(self):
        self.price_deck = load_price_deck(self)

    def deterministic(self):
        self.load_probability()
        self.load_schedule()
        self.load_framework()
        self.load_forecasts()
        self.load_economics()
        self.load_pricing()

        num_processes = mp.cpu_count() - 1
        chunk_size = int(len(self.properties_list) / num_processes)
        if chunk_size <= 1:
            chunk_size = int(len(self.properties_list) / (num_processes / 2))
        if chunk_size <= 1:
            chunk_size = int(len(self.properties_list) / (num_processes / 4))
        chunks = [self.properties_list[i:i + chunk_size] for i in range(0, len(self.properties_list), chunk_size)]
        chunk_check = [len(c) for c in chunks]
        tmp_args = []
        for c in chunks:
            tmp_args.append([self, c, 'deterministic', 0])
        args = tmp_args

        print('processes:', num_processes, len(self.properties_list), sum(chunk_check), chunk_check)

        db.connections.close_all()

        start = time.time()
        pool = mp.Pool(processes=len(chunks))
        pool.map(cash_flows, args)
        pool.close()
        
        save_output_log(self, 'deterministic')
        stop = time.time()
        timer(start, stop)

    def probabilistic(self, num_simulations):
        self.num_simulations = range(num_simulations)
        self.load_framework()
        self.load_forecasts()
        self.load_economics()
        self.load_pricing()
    
        num_processes = mp.cpu_count() - 1
        chunk_size = int(len(self.num_simulations) / num_processes)
        if chunk_size <= 1:
            chunk_size = int(len(self.num_simulations) / (num_processes / 2))
        chunks = [self.num_simulations[i:i + chunk_size] for i in range(0, len(self.num_simulations), chunk_size)]
        chunk_check = [len(c) for c in chunks]

        print('processes:', num_processes, len(self.num_simulations), sum(chunk_check), chunk_check)

        db.connections.close_all()
        start = time.time()
        pool = mp.Pool(processes=len(chunks))
        pool.map(self.mp_probabilistic, chunks)
        pool.close()

        calculate_percentiles(self)

        save_output_log(self, 'probabilistic')
        stop = time.time()
        timer(start, stop)

    def mp_probabilistic(self, chunks):
        for i, s in enumerate(chunks):
            print(self.owner, i+1, 'of', len(chunks), ':', s)
            np.random.seed()
            self.load_probability(sim=s)
            self.load_schedule(sim=s)
            args = [self, self.properties_list, 'probabilistic', s]
            cash_flows(args)
        return