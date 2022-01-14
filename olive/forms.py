from django import forms
from olive.models import *

scenario_cols = [field.name for field in scenarios._meta.get_fields()]
scenario_choices = []
scenario_choices.append(('pick scenario', 'pick scenario'))
scenario_query = scenarios.objects.values('scenario').filter(active=1).distinct()
try:
    for s in scenario_query:
        scenario_choices.append((s['scenario'], s['scenario']))
except:
    pass

user_choices = []
user_choices.append(('pick user', 'pick user'))
user_query = users.objects.values('name')
try:
    for u in user_query:
        user_choices.append((u['name'], u['name']))
except:
    pass

output_choices = []
output_choices.append(('pick output', 'pick output'))

tc_choices = []
tc_choices.append(('pick type curve', 'pick type curve'))

class ScenarioSelect(forms.Form):

    scenario = forms.ChoiceField(choices=scenario_choices, initial={'pick scenario', 'pick scenario'})
    user = forms.ChoiceField(choices=user_choices, initial={'pick user', 'pick user'})

    def __init__(self, *args, **kwargs):
        super(ScenarioSelect, self).__init__(*args, **kwargs)
        scenario_choices = []
        scenario_choices.append(('pick scenario', 'pick scenario'))
        try:
            for s in scenarios.objects.values('scenario').filter(active=1).distinct():
                scenario_choices.append((s['scenario'], s['scenario']))
        except:
            pass
        self.fields['scenario'].choices = scenario_choices

        user_choices = []
        user_choices.append(('pick user', 'pick user'))
        user_query = users.objects.values('name')
        try:
            for u in user_query:
                user_choices.append((u['name'], u['name']))
        except:
            pass

    def set_scenario_dropdown(self, selected_scenario=None):
        scenario_choices = []
        scenario_choices.append(('pick scenario', 'pick scenario'))
        try:
            for s in scenarios.objects.values('scenario').filter(active=1).distinct():
                scenario_choices.append((s['scenario'], s['scenario']))
        except:
            pass
        self.fields['scenario'].choices = scenario_choices
        if selected_scenario:
            self.fields['scenario'].initial = selected_scenario

    def set_user_dropdown(self, selected_user=None):
        user_choices = []
        user_choices.append(('pick user', 'pick user'))
        try:
            for u in users.objects.values('name'):
                user_choices.append((u['name'], u['name']))
        except:
            pass
        self.fields['user'].choices = user_choices
        if selected_user:
            self.fields['user'].initial = selected_user


class EditScenario(forms.ModelForm):
    user = forms.CharField()
    _scenario = forms.CharField()
    project_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])
    properties_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])
    framework_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])
    schedule_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])
    economics_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])
    forecast_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])
    price_deck_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])
    probability_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])
    owner_version = forms.DateField(widget=forms.widgets.Select(), input_formats=['%Y-%m-%d %H:%M:%S.%f'])

    class Meta():
        model = scenarios
        fields = ('scenario', 'category', 'owner', 'project', 'properties', 'framework', 'schedule', 'economics', 'forecast', 'price_deck', 'probability')


class CopyScenario(forms.ModelForm):
    user = forms.CharField()
    _scenario = forms.CharField()
    owner = forms.ChoiceField(choices=user_choices)

    class Meta():
        model = scenarios
        fields = '__all__'


class UpdateScenario(forms.ModelForm):
    user = forms.CharField()
    _scenario = forms.CharField()

    class Meta():
        model = scenarios
        fields = '__all__'


class NewScenario(forms.ModelForm):
    user = forms.CharField()
    _scenario = forms.CharField()

    class Meta():
        model = scenarios
        fields = '__all__'


class PropertiesSelect(forms.Form):
    properties = forms.CharField()
    _scenario = forms.CharField()
    user = forms.CharField()

    class Meta():
        model = properties
        fields = '__all__'

class FrameworkSelect(forms.Form):
    framework = forms.CharField()
    _scenario = forms.CharField()
    user = forms.CharField()

    class Meta():
        model = frameworks
        fields = '__all__'

class EconomicsSelect(forms.Form):
    economics = forms.CharField()
    _scenario = forms.CharField()
    user = forms.CharField()

    class Meta():
        model = economics
        fields = '__all__'

class ProjectSelect(forms.Form):
    project = forms.CharField()
    _scenario = forms.CharField()
    user = forms.CharField()

    class Meta():
        model = projects
        fields = '__all__'

class ForecastSelect(forms.Form):
    forecast = forms.CharField()
    _scenario = forms.CharField()
    user = forms.CharField()

    class Meta():
        model = forecasts
        fields = '__all__'

class ScheduleSelect(forms.Form):
    schedule = forms.CharField()
    _scenario = forms.CharField()
    file = forms.FileField(required=False)
    user = forms.CharField()

    class Meta():
        model = schedules
        fields = '__all__'

class PricingSelect(forms.Form):
    price_deck = forms.CharField()
    _scenario = forms.CharField()
    user = forms.CharField()

    class Meta():
        model = pricing
        fields = '__all__'

class ProbabilitySelect(forms.Form):
    probability = forms.CharField()
    _scenario = forms.CharField()
    user = forms.CharField()

    class Meta():
        model = probabilities
        fields = '__all__'

class ScheduleInputsSelect(forms.Form):
    _scenario = forms.CharField()
    user = forms.CharField()
    schedule_inputs = forms.CharField()

    class Meta():
        model = schedule_inputs
        fields = '__all__'

class ProbabilityLogSelect(forms.Form):
    _scenario = forms.CharField()
    user = forms.CharField()
    probability_log = forms.CharField()

    class Meta():
        model = probability_log
        fields = '__all__'

class ScheduleFileSelect(forms.Form):
    _scenario = forms.CharField()
    user = forms.CharField()
    schedule_file = forms.CharField()

    class Meta():
        model = schedule_file
        fields = '__all__'

class ScheduleLogSelect(forms.Form):
    _scenario = forms.CharField()
    user = forms.CharField()
    schedule_log = forms.CharField()

    class Meta():
        model = schedule_log
        fields = '__all__'

class OutputLogSelect(forms.Form):
    _scenario = forms.CharField()
    user = forms.CharField()
    output_log = forms.CharField()

    class Meta():
        model = output_log
        fields = '__all__'

class OutputList(forms.Form):
    output_list = forms.ChoiceField(choices=output_choices)

    def __init__(self, *args, **kwargs):
        super(OutputList, self).__init__(*args, **kwargs)

    def set_output_choices(self, selected_scenario=None):
        output_choices = []
        output_choices.append(('pick output', 'pick output'))
        output_query = output_log.objects.values_list('output_version', flat=True).filter(scenario=selected_scenario).order_by('-output_version')
        try:
            for o in output_query:
                choice = o.strftime('%m/%d/%Y %I:%M:%S %p')
                output_choices.append((o, choice))
        except:
            pass
        self.fields['output_list'].choices = output_choices

class TypeCurveList(forms.Form):
    tc_list = forms.ChoiceField(choices=tc_choices, required=True)

    def __init__(self, *args, **kwargs):
        super(TypeCurveList, self).__init__(*args, **kwargs)

    def set_tc_choices(self):
        tc_choices = []
        tc_choices.append(('pick type curve', 'pick type curve'))
        tc_query = type_curves.objects.values_list('name', flat=True).order_by('name').distinct()
        for tc in tc_query:
            tc_choices.append((tc, tc))
        self.fields['tc_list'].choices = tc_choices