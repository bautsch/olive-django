import re
from typing import Type
from django.shortcuts import render
from olive.models import *
from olive.forms import *
from django.http import HttpResponse
import csv
from pathlib import Path
import os
from django.db.models import Max
from django.contrib import messages
from django.utils import timezone
import pytz
from dateutil import parser
from olive.projections import TypeCurves, ProducingCurves
from olive.utils import *
from olive.output import Build
from olive.generate import aggregate
tz = pytz.timezone('America/Denver')

def user_picked(selection):
    return selection.cleaned_data['user'] in users.objects.values_list('name', flat=True)


def current_scenario(selected_scenario):
    current_ver = scenarios.objects.filter(scenario=selected_scenario).aggregate(Max('version'))['version__max']
    return scenarios.objects.filter(scenario=selected_scenario, version=current_ver)


def get_version(scenario, field):
    current_ver = scenarios.objects.filter(scenario=scenario).aggregate(Max('version'))['version__max']
    return scenarios.objects.filter(scenario=scenario, version=current_ver).values_list(field, flat=True)[0]


def index(request):
    scenario_dropdown = ScenarioSelect()
    scenario_dropdown.set_scenario_dropdown()
    scenario_dropdown.set_user_dropdown()
    sub_scenarios = {'project': [ProjectSelect, projects],
                     'properties': [PropertiesSelect, properties],
                     'framework': [FrameworkSelect, frameworks],
                     'schedule': [ScheduleSelect, schedules],
                     'schedule_inputs': [ScheduleInputsSelect, schedule_inputs],
                     'schedule_file': [ScheduleFileSelect, schedule_file],
                     'schedule_log': [ScheduleLogSelect, schedule_log],
                     'economics': [EconomicsSelect, economics],
                     'forecast': [ForecastSelect, forecasts],
                     'price_deck': [PricingSelect, pricing],
                     'probability': [ProbabilitySelect, probabilities],
                     'probability_log': [ProbabilityLogSelect, probability_log],
                     'output_log': [OutputLogSelect, output_log]
                     }

    forms = {'scenarios': None,
             'scenario_dropdown': scenario_dropdown,
             'edit_scenario': None,
             'copy_scenario': None,
             'fields': None,
             'values': None,
             'load': None,
             'user': None,
             'scenario_versions': None,
             'output_list': OutputList(),
             'tc_list': TypeCurveList()
             }

    if request.method == 'POST':
        print(request.POST)
        sub_check = [i in request.POST.keys() for i in sub_scenarios.keys()]

        if '_scenario' in request.POST.keys():
            forms['output_list'].set_output_choices(selected_scenario=request.POST['_scenario'])
        elif 'scenario' in request.POST.keys():
            forms['output_list'].set_output_choices(selected_scenario=request.POST['scenario'])

        forms['tc_list'].set_tc_choices()

        if 'select_scenario' in request.POST:
            print('selecting scenario')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)

        elif 'copy_scenario' in request.POST:
            print('copying scenario')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                    if len(forms['scenarios']) > 0:
                        forms['copy_scenario'] = CopyScenario()
                        for f in forms['copy_scenario'].fields:
                            if 'copy' in f or '_scenario' in f:
                                continue
                            elif 'category' in f:
                                forms['copy_scenario'].fields[f].initial = forms['scenarios'].values(f)[0][f]
                            elif 'user' in f:
                                forms['copy_scenario'].fields['owner'].initial = forms['user']
                            elif f == 'scenario':
                                forms['copy_scenario'].fields[f].widget.attrs['placeholder'] = forms['scenarios'].values(f)[0][f]
                            else:
                                forms['copy_scenario'].fields[f].initial = forms['scenarios'].values(f)[0][f]
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)

        elif 'save' in request.POST:
            print('saving new scenario')
            fixed = request.POST.copy()
            for k, v in fixed.items():
                if '_version' in k:
                    fixed[k] = fixed['version']
            new_scenario = CopyScenario(data=request.POST)
            if new_scenario.is_valid():
                instance = new_scenario.save(commit=False)
                forms['user'] = new_scenario.cleaned_data['user']
                version = new_scenario.cleaned_data['version']
                copy_scenario = current_scenario(new_scenario.cleaned_data['_scenario']).values()[0]
                for new_field, new_value in new_scenario.cleaned_data.items():

                    if new_field in ('user', 'owner', 'scenario', 'category', '_scenario' ,'version', 'active'):
                        print('skip', new_field)

                    elif new_value is None:
                        setattr(instance, new_field + '_version', None)
                        print('set', new_field, 'and version equal to None')

                    elif '_version' in new_field:
                        print('skip', new_field)
                        continue

                    elif new_value is not None and copy_scenario[new_field] is None:
                        print('update main scenario table only for ', new_field, 'from', copy_scenario[new_field], 'to', new_value)
                        setattr(instance, new_field + '_version', version)

                    elif new_value == copy_scenario[new_field]:
                        print('same data', new_field)
                        setattr(instance, new_field + '_version', copy_scenario['version'])
                        continue

                    else:
                        print('copy', new_field, 'from', copy_scenario[new_field], 'to', new_value)
                        print('copying', copy_scenario[new_field], copy_scenario[new_field+'_version'])
                        print('to', new_value, version)
                        copy_data = sub_scenarios[new_field][1].objects.filter(scenario=copy_scenario[new_field], version=copy_scenario[new_field+'_version'])
                        for c in copy_data:
                            c.id = None
                            setattr(c, 'version', version)
                            setattr(c, 'scenario', new_value)
                            c.save()
                instance.save()
                selected_scenario = new_scenario.cleaned_data['scenario']
                forms['scenarios'] = current_scenario(selected_scenario)
                forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                forms['scenario_dropdown'].set_user_dropdown(forms['user'])
            else:      
                print(new_scenario.errors)

        elif 'edit_scenario' in request.POST:
            print('editing scenario')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                    if len(forms['scenarios']) > 0:
                        forms['edit_scenario'] = EditScenario()
                        for f in forms['edit_scenario'].fields:
                            if 'user' in f or '_scenario' in f:
                                continue
                            elif 'version' in f:
                                choices = []
                                current_id = scenarios.objects.filter(scenario=selected_scenario).aggregate(Max('id'))['id__max']
                                current_version = scenarios.objects.values_list(f, flat=True).filter(scenario=selected_scenario, id=current_id)
                                scen = f[:-8]
                                if 'owner' not in f:
                                    versions = sub_scenarios[scen][1].objects.values_list('version', flat=True).filter(scenario=forms['scenarios'].values()[0][scen]).order_by('-version').distinct()
                                    for v in versions:
                                        if v is None:
                                            choices.append((0, 'None'))
                                        else:
                                            choices.append((v, v))                         
                                else:
                                    for v in scenarios.objects.values_list(f, flat=True).filter(scenario=selected_scenario).order_by('-'+f).distinct():
                                        if v is None:
                                            choices.append((0, 'None'))
                                        else:
                                            choices.append((v, v))
                                forms['edit_scenario'].fields[f].widget.choices = choices
                                # if current_version is None:
                                #     forms['edit_scenario'].fields[f].initial = current_version[0]
                                forms['edit_scenario'].fields[f].initial = current_version[0]
                            elif 'category' in f:
                                forms['edit_scenario'].fields[f].initial = forms['scenarios'].values(f)[0][f]
                            elif forms['scenarios'].values(f)[0][f] == 'None' or forms['scenarios'].values(f)[0][f] is None:
                                continue
                            else:
                                forms['edit_scenario'].fields[f].widget.attrs['value'] = forms['scenarios'].values(f)[0][f]
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)

        elif 'archive_scenario' in request.POST:
            print('archiving scenario')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    print(selection.cleaned_data)
                    selected_scenario = selection.cleaned_data['scenario']
                    archive_scenario = scenarios.objects.filter(scenario=selected_scenario)
                    archive_scenario.update(active=0)
                    for archive in archive_scenario:
                        archive.save()
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = None
                    forms['scenario_dropdown'].set_scenario_dropdown('pick scenario')
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)

        elif 'update' in request.POST:
            print('updating scenario')
            fixed = request.POST.copy()
            current_version = scenarios.objects.filter(scenario=fixed['scenario']).aggregate(Max('version'))['version__max']
            old_scenario = scenarios.objects.values().filter(scenario=fixed['scenario'], version=current_version)[0]
            for k, v in old_scenario.items():
                if k not in fixed.keys():
                    fixed[k] = None
            for k, v in fixed.items():
                if 'version' in k:
                    if v not in (None, '', 0, '0') and type(v) == str:
                        fixed[k] = parser.parse(v)
                    else:
                        fixed[k] = None
            for k, v in fixed.items():
                if k in old_scenario.keys() and 'version' not in k:
                    if v != old_scenario[k] and v != '' and k not in ('active', 'id'):
                        print(k, v, old_scenario[k])
                        current_version = sub_scenarios[k][1].objects.filter(scenario=v).aggregate(Max('version'))['version__max']
                        print(k+'_version', current_version)
                        fixed[k+'_version'] = current_version
            print(fixed)
            request.POST = fixed
            update_scenario = UpdateScenario(request.POST)
            if update_scenario.is_valid():
                selected_scenario = update_scenario.cleaned_data['scenario']
                forms['user'] = update_scenario.cleaned_data['user']
                print(update_scenario.cleaned_data)
                update_scenario.save(commit=True)
                forms['scenarios'] = current_scenario(selected_scenario)
                forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                forms['scenario_dropdown'].set_user_dropdown(forms['user'])
            else:
                print(update_scenario.errors)

        elif 'load_tc_confirmed' in request.POST.keys():
            print('load type curves confirmed')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                    TypeCurves(tc_file=request.FILES['file'], owner=forms['user'])
                    messages.info(request, 'Load completed')
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)

        elif 'load_pdp_confirmed' in request.POST.keys():
            print('load pdp forecasts confirmed')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                    ProducingCurves(pdp_file=request.FILES['file'], owner=forms['user'], scenario=selected_scenario)
                    messages.info(request, 'Load completed')
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)

        elif 'deterministic' in request.POST.keys():
            print('generate deterministic cash flows')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                    scenario = current_scenario(selected_scenario).values()[0]
                    build = Build(scenario=scenario, owner=forms['user'])
                    build.deterministic()
                    messages.info(request, 'Build completed')
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)   

        elif 'run_probabilistic' in request.POST.keys():
            print('generate probabilistic analysis')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                    scenario = current_scenario(selected_scenario).values()[0]
                    low_pct = request.POST['low_pct']
                    mid_pct = request.POST['mid_pct']
                    high_pct = request.POST['high_pct']
                    build = Build(scenario=scenario, owner=forms['user'],
                                  low_pct=low_pct, mid_pct=mid_pct, high_pct=high_pct)
                    build.probabilistic(int(request.POST['num_simulations']))
                    messages.info(request, 'Build completed')
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)   

        elif 'run_aggregations' in request.POST.keys():
            print('aggregating results')
            selection = ScenarioSelect(request.POST)
            if selection.is_valid():
                if user_picked(selection):
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                    scenario = current_scenario(selected_scenario).values()[0]
                    args = {'owner': forms['user'], 'scenario': forms['scenarios'],
                            'num_aggregations': request.POST['num_aggregations'],
                            'num_simulations': request.POST['num_simulations'],
                            'field': request.POST['agg_field'], 'version': request.POST['output_list'],
                            'low_pct': request.POST['low_pct'], 'mid_pct': request.POST['mid_pct'],
                            'high_pct': request.POST['high_pct']}
                    if 'onelines' in request.POST.keys():
                        args['onelines'] = True
                    else:
                        args['onelines'] = False
                    if 'monthly' in request.POST.keys():
                        args['monthly'] = True
                    else:
                        args['monthly'] = False
                    aggregate(args)
                    messages.info(request, 'Build completed')
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)   

        elif 'gen_cc_tc' in request.POST.keys():
            print('generating ComboCurve type curve')
            selection = ScenarioSelect(request.POST)
            tc_name = request.POST['tc_list']
            segments = request.POST['segments']
            dmin = request.POST['dmin']
            min_rate = request.POST['min_rate']
            if selection.is_valid():
                if user_picked(selection):
                    print(selection.cleaned_data)
                    selected_scenario = selection.cleaned_data['scenario']
                    forms['user'] = selection.cleaned_data['user']
                    forms['scenarios'] = current_scenario(selected_scenario)
                    forms['scenario_dropdown'].set_scenario_dropdown(selected_scenario)
                    forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                    scenario = current_scenario(selected_scenario).values()[0]
                    cc_tc = gen_cc_type_curve(tc_name, segments, dmin, min_rate)
                    response = HttpResponse(content_type='text/csv')
                    file_name = tc_name + '.csv'
                    response['Content-Disposition'] = 'attachment; filename="' + file_name + '"'
                    writer = csv.writer(response)
                    downloads_path = Path(Path.home() / 'Downloads/' / file_name)
                    if downloads_path.exists():
                        os.remove(downloads_path)
                    frame_cols = cc_tc.columns
                    writer.writerow(frame_cols)
                    for row in cc_tc.iterrows():
                        writer.writerow(row[1])
                    return response
                else:
                    print('no user')
                    forms['user'] = None
            else:
                print(selection.errors)

        elif any(sub_check):
            sub_scenario = [scen for idx, scen in enumerate(sub_scenarios.keys()) if sub_check[idx]][0]
            print(sub_scenario)
            selection = sub_scenarios[sub_scenario][0](request.POST)
            if selection.is_valid():
                cleaned = selection.cleaned_data
                forms['scenarios'] = current_scenario(cleaned['_scenario'])
                forms['fields'] = [field.name for field in sub_scenarios[sub_scenario][1]._meta.get_fields()]
                forms['user'] = selection.cleaned_data['user']
                if sub_scenario in ('schedule_inputs' ,'probability_log', 'schedule_file', 'schedule_log', 'output_log'):
                    forms['load'] == None
                else:
                    forms['load'] = sub_scenario
                if forms['scenarios'][0].project is not None and sub_scenario in ('properties', 'economics', 'forecast', 'schedule'):
                    print(1)
                    idp_list = projects.objects.values_list('idp').filter(scenario=forms['scenarios'][0].project, version=forms['scenarios'][0].project_version)
                    current_version = forms['scenarios'].values()[0][sub_scenario+'_version']
                    forms['values'] = sub_scenarios[sub_scenario][1].objects.filter(scenario=cleaned[sub_scenario], idp__in=idp_list, version=current_version)
                else:
                    print(4)
                    print(cleaned[sub_scenario])
                    if sub_scenario in ('schedule_inputs'):
                        current_version = forms['scenarios'].values()[0][sub_scenario+'_version']
                        print(current_version)
                        forms['values'] = sub_scenarios[sub_scenario][1].objects.filter(version=current_version)
                    elif sub_scenario == 'schedule_log':
                        forms['values'] = sub_scenarios[sub_scenario][1].objects.filter(schedule_scenario=cleaned[sub_scenario]).order_by('-schedule_version')
                    elif sub_scenario == 'output_log':
                        forms['values'] = sub_scenarios[sub_scenario][1].objects.filter(output_scenario=cleaned['_scenario']).order_by('-output_version')
                    elif sub_scenario == 'schedule_file':
                        current_version = forms['scenarios'].values()[0]['schedule_version']
                        print(current_version)
                        forms['values'] = sub_scenarios[sub_scenario][1].objects.filter(scenario=cleaned[sub_scenario], version=current_version)
                    elif sub_scenario == 'probability_log':
                        current_version = probability_log.objects.values_list('version', flat=True).filter(scenario=forms['scenarios'][0].probability).order_by('-version').distinct()[0]
                        print(current_version)
                        forms['values'] = sub_scenarios[sub_scenario][1].objects.filter(scenario=forms['scenarios'][0].probability, version=current_version)                        
                    else:
                        current_version = forms['scenarios'].values()[0][sub_scenario+'_version']
                        print(current_version)
                        forms['values'] = sub_scenarios[sub_scenario][1].objects.filter(scenario=cleaned[sub_scenario], version=current_version)
                scenario_dropdown.fields['scenario'].initial = cleaned['_scenario']
                forms['scenario_dropdown'].set_user_dropdown(forms['user'])
                if 'edit' in request.POST.keys():
                    print('edit')
                    response = HttpResponse(content_type='text/csv')
                    file_name = sub_scenario + '.csv'
                    response['Content-Disposition'] = 'attachment; filename="' + file_name + '"'
                    writer = csv.writer(response)
                    downloads_path = Path(Path.home() / 'Downloads/' / file_name)
                    if downloads_path.exists():
                        os.remove(downloads_path)
                    frame_cols = [field.name for field in sub_scenarios[sub_scenario][1]._meta.get_fields()]
                    writer.writerow(frame_cols)
                    qs = forms['values']
                    for row in qs.values_list():
                        writer.writerow(row)
                    return response
                if 'load_confirmed' in request.POST.keys():
                    print('load confirmed')
                    version = timezone.now()
                    file_path=request.FILES['file']
                    if sub_scenario in ('properties', 'economics', 'project', 'schedule_inputs', 'forecast', 'price_deck', 'framework', 'probability'):
                        load_csv(file_path, forms['user'], version, sub_scenarios[sub_scenario][1])
                    new_scenario = current_scenario(cleaned['_scenario'])
                    for scen in new_scenario:
                        scen.id = None
                        setattr(scen, 'version', version)
                        setattr(scen, sub_scenario + '_version', version)
                        setattr(scen, 'owner_version', version)
                        setattr(scen, 'owner', forms['user'])
                        scen.save()
                    messages.info(request, 'Load completed')
                if 'create_confirmed' in request.POST.keys():
                    print('create confirmed')
                    update_scenario = current_scenario(selection.cleaned_data['_scenario'])
                    build = Build(scenario=forms['scenarios'].values()[0], owner=forms['user'])
                    build.create_schedule(request.FILES['file'])
                    for scen in update_scenario:
                        setattr(scen, 'schedule_version', build.schedule.version)
                        setattr(scen, 'schedule_inputs_version', build.schedule.schedule_inputs_version)
                        setattr(scen, 'id', None)
                        setattr(scen, 'version', build.schedule.version)
                        setattr(scen, 'owner', forms['user'])
                        scen.save()
                    messages.info(request, 'Load completed')
            else:
                print(selection)

    return render(request, 'olive/index.html', context=forms)