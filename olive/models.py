from django.db import models


class users(models.Model):
    name = models.CharField(max_length=100, choices=[(1, 'james'), (2, 'kara')])


class properties(models.Model):
    scenario = models.CharField(max_length=100)
    budget_type = models.CharField(max_length=25, choices=[('base', 'base'), ('wedge', 'wedge')], null=True, blank=True)
    idp = models.CharField(max_length=100)
    cc_id = models.CharField(max_length=255, null=True, blank=True)
    api14 = models.CharField(max_length=14, null=True, blank=True)
    cost_center = models.CharField(max_length=50, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(max_length=25, null=True, blank=True)
    reserve_cat = models.CharField(max_length=5, null=True, blank=True)
    asset = models.CharField(max_length=50, null=True, blank=True)
    field = models.CharField(max_length=50, null=True, blank=True)
    short_pad = models.CharField(max_length=100, null=True, blank=True)
    pad = models.CharField(max_length=200, null=True, blank=True)
    schedule_order = models.IntegerField(null=True, blank=True)
    depth = models.IntegerField(null=True, blank=True)
    drill_start_date = models.DateField(null=True, blank=True)
    drill_end_date = models.DateField(null=True, blank=True)
    compl_start_date = models.DateField(null=True, blank=True)
    compl_end_date = models.DateField(null=True, blank=True)
    first_prod_date = models.DateField(null=True, blank=True)
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'idp', 'owner', 'version'),)


class economics(models.Model):
    scenario = models.CharField(max_length=100)
    idp = models.CharField(max_length=100)
    gross_gas_mult = models.CharField(max_length=255)
    gross_oil_mult = models.CharField(max_length=255)
    gross_water_mult = models.CharField(max_length=255)
    btu_factor = models.FloatField()
    shrink_factor = models.FloatField()
    ngl_g_bpmm = models.FloatField()
    wi_frac = models.FloatField()
    nri_frac = models.FloatField()
    roy_frac = models.FloatField()
    cost_fixed = models.CharField(max_length=255)
    cost_fixed_alloc = models.CharField(max_length=255)
    cost_vargas = models.CharField(max_length=255)
    cost_varoil = models.CharField(max_length=255)
    cost_varwater = models.CharField(max_length=255)
    cost_gtp = models.CharField(max_length=255)
    tax_sev = models.CharField(max_length=255)
    tax_adval = models.CharField(max_length=255)
    inv_g_drill = models.FloatField()
    inv_g_compl = models.FloatField()
    inv_g_misc = models.CharField(max_length=255)
    inv_g_aban = models.CharField(max_length=255)
    gas_basis = models.CharField(max_length=15)
    price_adj_gas = models.CharField(max_length=255)
    price_adj_oil = models.CharField(max_length=255)
    price_adj_ngl = models.CharField(max_length=255)
    minimum_life = models.CharField(max_length=255)
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'idp', 'owner', 'version'),)


class projects(models.Model):
    scenario = models.CharField(max_length=255)
    idp = models.CharField(max_length=255)
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'idp', 'owner', 'version'),)


class schedules(models.Model):
    scenario = models.CharField(max_length=100)
    type = models.CharField(choices=[('deterministic', 'deterministic'), ('probabilistic', 'probabilistic')], max_length=25)
    idp = models.CharField(max_length=255)
    drill_start_date = models.DateField()
    drill_end_date = models.DateField()
    compl_start_date = models.DateField()
    compl_end_date = models.DateField()
    prod_start_date = models.DateField()
    simulation = models.IntegerField()
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'idp', 'simulation', 'owner', 'version'),)


class schedule_file(models.Model):
    scenario = models.CharField(max_length=100)
    rig = models.CharField(max_length=100)
    pad = models.CharField(max_length=100)
    input_group = models.CharField(max_length=100)
    pad_start_date = models.DateField(blank=True, null=True)
    pad_end_date = models.DateField(blank=True, null=True)
    conductor_days = models.FloatField(blank=True, null=True)
    mob_in_days = models.FloatField(blank=True, null=True)
    drill_days = models.FloatField(blank=True, null=True)
    mob_out_days = models.FloatField(blank=True, null=True)
    logging_days = models.FloatField(blank=True, null=True)
    build_fac_days = models.FloatField(blank=True, null=True)
    frac_days = models.FloatField(blank=True, null=True)
    flowback_days = models.FloatField(blank=True, null=True)
    pod_size = models.IntegerField(blank=True, null=True)
    drill_start_date = models.DateField(blank=True, null=True)
    drill_end_date = models.DateField(blank=True, null=True)
    compl_start_date = models.DateField(blank=True, null=True)
    compl_end_date = models.DateField(blank=True, null=True)
    prod_start_date = models.DateField(blank=True, null=True)
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'rig', 'pad', 'owner', 'version'),)


class schedule_inputs(models.Model):
    input_group = models.CharField(max_length=50)
    parameter = models.CharField(max_length=50)
    unit = models.CharField(max_length=50)
    value = models.CharField(max_length=50)
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('input_group', 'parameter', 'owner', 'version'),)


class schedule_log(models.Model):
    schedule_scenario = models.CharField(max_length=100)
    schedule_owner = models.CharField(max_length=255)
    schedule_version = models.DateTimeField()
    schedule_file_scenario = models.CharField(max_length=100)
    schedule_file_owner = models.CharField(max_length=255)
    schedule_file_version = models.DateTimeField()
    schedule_inputs_owner = models.CharField(max_length=255)
    schedule_inputs_version = models.DateTimeField()

    class Meta:
        unique_together = (('schedule_scenario', 'schedule_owner', 'schedule_version'),)


class forecasts(models.Model):
    scenario = models.CharField(max_length=100)
    idp = models.CharField(max_length=100)
    forecast_name = models.CharField(max_length=100)
    forecast_type = models.CharField(max_length=100)
    forecast_version = models.IntegerField()
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'idp', 'owner', 'version'),)


class pricing(models.Model):
    scenario = models.CharField(max_length=100)
    prod_date = models.DateField()
    end_of_month = models.DateField()
    hh = models.FloatField()
    wti = models.FloatField()
    cig = models.FloatField()
    nwr = models.FloatField()
    ngl = models.FloatField()
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'prod_date', 'owner', 'version'),)


class probabilities(models.Model):
    scenario = models.CharField(max_length=100)
    property_type = models.CharField(max_length=50)
    property_value = models.CharField(max_length=255)
    category = models.CharField(max_length=100)
    type = models.CharField(max_length=100)
    value = models.CharField(max_length=100)
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'property_type', 'property_value', 'category', 'type', 'owner', 'version'),)


class probability_log(models.Model):
    scenario = models.CharField(max_length=100)
    idp = models.CharField(max_length=100)
    category = models.CharField(max_length=100)
    type = models.CharField(max_length=100)
    value = models.CharField(max_length=100, null=True)
    simulation = models.IntegerField()
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'idp', 'category', 'type', 'simulation', 'owner', 'version'),)


class frameworks(models.Model):
    scenario = models.CharField(max_length=100)
    effective_date = models.DateField()
    life = models.IntegerField()
    daily_output = models.BooleanField()
    pv_spread = models.CharField(max_length=100)
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'owner', 'version'),)


class type_curves(models.Model):
    name = models.CharField(max_length=255)
    time_on = models.IntegerField()
    gas = models.FloatField()
    oil = models.FloatField()
    water = models.FloatField()
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('name', 'time_on', 'owner', 'version'),)


class prod_forecasts(models.Model):
    scenario = models.CharField(max_length=100)
    idp = models.CharField(max_length=100)
    prod_date = models.DateField()
    gas = models.FloatField()
    oil = models.FloatField()
    water = models.FloatField()
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()

    class Meta:
        unique_together = (('scenario', 'idp', 'prod_date', 'owner', 'version'),)


class scenarios(models.Model):
    scenario = models.CharField(max_length=100)
    category = models.CharField(max_length=100, choices=[
                                                         ('budget', 'budget'), 
                                                         ('field_assessment', 'field_assessment'),
                                                         ('lookback', 'lookback'), 
                                                         ('miscellaneous', 'miscellaneous'), 
                                                         ('monthly_outlook_update', 'monthly_outlook_update'),
                                                         ('rfa', 'rfa'),
                                                         ('sensitivity', 'sensitivity')
                                                         ])
    project = models.CharField(max_length=100, null=True, blank=True)
    project_version = models.DateTimeField(null=True, blank=True)
    properties = models.CharField(max_length=100, null=True, blank=True)
    properties_version = models.DateTimeField(null=True, blank=True)
    framework = models.CharField(max_length=100, null=True, blank=True)
    framework_version = models.DateTimeField(null=True, blank=True)
    schedule = models.CharField(max_length=100, null=True, blank=True)
    schedule_version = models.DateTimeField(null=True, blank=True)
    schedule_inputs_version = models.DateTimeField(null=True, blank=True)
    economics = models.CharField(max_length=100, null=True, blank=True)
    economics_version = models.DateTimeField(null=True, blank=True)
    forecast = models.CharField(max_length=100, null=True, blank=True)
    forecast_version = models.DateTimeField(null=True, blank=True)
    price_deck = models.CharField(max_length=100, null=True, blank=True)
    price_deck_version = models.DateTimeField(null=True, blank=True)
    probability = models.CharField(max_length=100, null=True, blank=True)
    probability_version = models.DateTimeField(null=True, blank=True)
    owner = models.CharField(max_length=100, null=True, blank=True)
    owner_version = models.DateTimeField(null=True, blank=True)
    version = models.DateTimeField()
    active = models.IntegerField()

    class Meta:
        unique_together = (('scenario', 'owner', 'version'),)


class output(models.Model):
    scenario = models.CharField(max_length=100)
    type = models.CharField(choices=[('deterministic', 'deterministic'), ('probabilistic', 'probabilistic')], max_length=25)
    idp = models.CharField(max_length=100)
    prod_date = models.DateField()
    budget_type = models.CharField(max_length=100)
    name = models.CharField(max_length=255)
    short_pad = models.CharField(max_length=100)
    pad = models.CharField(max_length=200)
    rig = models.CharField(max_length=100)
    field = models.CharField(max_length=50)
    time_on = models.IntegerField()
    gas_eur = models.FloatField()
    oil_eur = models.FloatField()
    gross_gas_mult = models.FloatField()
    gross_oil_mult = models.FloatField()
    gross_water_mult = models.FloatField()
    gross_gas = models.FloatField()
    gross_oil = models.FloatField()
    gross_water = models.FloatField()
    ngl_yield = models.FloatField()
    btu = models.FloatField()
    shrink = models.FloatField()
    wi = models.FloatField()
    nri = models.FloatField()
    royalty = models.FloatField()
    net_gas = models.FloatField()
    net_oil = models.FloatField()
    net_ngl = models.FloatField()
    net_mcfe = models.FloatField()
    royalty_gas = models.FloatField()
    royalty_oil = models.FloatField()
    royalty_ngl = models.FloatField()
    royalty_mcfe = models.FloatField()
    hh = models.FloatField()
    wti = models.FloatField()
    ngl = models.FloatField()
    cig = models.FloatField()
    nwr = models.FloatField()
    gas_basis = models.CharField(max_length=10)
    gas_price_adj = models.FloatField()
    gas_adj_unit = models.CharField(max_length=10)
    oil_price_adj = models.FloatField()
    oil_adj_unit = models.CharField(max_length=10)
    ngl_price_adj = models.FloatField()
    ngl_adj_unit = models.CharField(max_length=10)
    realized_gas_price = models.FloatField()
    realized_oil_price = models.FloatField()
    realized_ngl_price = models.FloatField()
    net_gas_rev = models.FloatField()
    net_oil_rev = models.FloatField()
    net_ngl_rev = models.FloatField()
    net_total_rev = models.FloatField()
    gross_drill_capex = models.FloatField()
    gross_compl_capex = models.FloatField()
    gross_misc_capex = models.FloatField()
    gross_aban_capex = models.FloatField()
    gross_total_capex = models.FloatField()
    net_drill_capex = models.FloatField()
    net_compl_capex = models.FloatField()
    net_misc_capex = models.FloatField()
    net_aban_capex = models.FloatField()
    net_total_capex = models.FloatField()
    fixed_cost = models.FloatField()
    alloc_fixed_cost = models.FloatField()
    var_gas_cost = models.FloatField()
    var_oil_cost = models.FloatField()
    var_water_cost = models.FloatField()
    doe = models.FloatField()
    gtp = models.FloatField()
    taxes = models.FloatField()
    loe = models.FloatField()
    cf = models.FloatField()
    fcf = models.FloatField()
    pv1 = models.FloatField()
    pv1_rate = models.FloatField()
    pv2 = models.FloatField()
    pv2_rate = models.FloatField()
    pv3 = models.FloatField()
    pv3_rate = models.FloatField()
    pv4 = models.FloatField()
    pv4_rate = models.FloatField()
    pv5 = models.FloatField()
    pv5_rate = models.FloatField()
    pv6 = models.FloatField()
    pv6_rate = models.FloatField()
    pv7 = models.FloatField()
    pv7_rate = models.FloatField()
    pv8 = models.FloatField()
    pv8_rate = models.FloatField()
    pv9 = models.FloatField()
    pv9_rate = models.FloatField()
    pv10 = models.FloatField()
    pv10_rate = models.FloatField()
    active = models.IntegerField()
    owner = models.CharField(max_length=255)
    version = models.DateTimeField()
    simulation = models.IntegerField()

    class Meta:
        unique_together = (('scenario', 'type', 'idp', 'prod_date', 'owner', 'version', 'simulation'),)


class output_onelines(models.Model):
    scenario = models.CharField(max_length=100)
    type = models.CharField(choices=[('deterministic', 'deterministic'), ('probabilistic', 'probabilistic')], max_length=25)
    idp = models.CharField(max_length=100)
    budget_type = models.CharField(max_length=100)
    name = models.CharField(max_length=255)
    short_pad = models.CharField(max_length=100)
    pad = models.CharField(max_length=200)
    rig = models.CharField(max_length=100)
    field = models.CharField(max_length=50)
    irr = models.FloatField(null=True, blank=True)
    payout = models.FloatField(null=True, blank=True)
    year_1_roic = models.FloatField(null=True, blank=True)
    year_1_fcf = models.FloatField(null=True, blank=True)
    year_2_roic = models.FloatField(null=True, blank=True)
    year_2_fcf = models.FloatField(null=True, blank=True)
    pvr = models.FloatField(null=True, blank=True)
    gas_eur = models.FloatField(null=True, blank=True)
    oil_eur = models.FloatField(null=True, blank=True)
    ip90 = models.FloatField(null=True, blank=True)
    gross_gas_mult = models.FloatField(null=True, blank=True)
    gross_oil_mult = models.FloatField(null=True, blank=True)
    gross_water_mult = models.FloatField(null=True, blank=True)
    gross_gas = models.FloatField(null=True, blank=True)
    gross_oil = models.FloatField(null=True, blank=True)
    gross_water = models.FloatField(null=True, blank=True)
    ngl_yield = models.FloatField(null=True, blank=True)
    btu = models.FloatField(null=True, blank=True)
    shrink = models.FloatField(null=True, blank=True)
    wi = models.FloatField(null=True, blank=True)
    nri = models.FloatField(null=True, blank=True)
    royalty = models.FloatField(null=True, blank=True)
    net_gas = models.FloatField(null=True, blank=True)
    net_oil = models.FloatField(null=True, blank=True)
    net_ngl = models.FloatField(null=True, blank=True)
    net_mcfe = models.FloatField(null=True, blank=True)
    royalty_gas = models.FloatField(null=True, blank=True)
    royalty_oil = models.FloatField(null=True, blank=True)
    royalty_ngl = models.FloatField(null=True, blank=True)
    royalty_mcfe = models.FloatField(null=True, blank=True)
    hh = models.FloatField(null=True, blank=True)
    wti = models.FloatField(null=True, blank=True)
    ngl = models.FloatField(null=True, blank=True)
    cig = models.FloatField(null=True, blank=True)
    nwr = models.FloatField(null=True, blank=True)
    gas_basis = models.CharField(max_length=10, null=True, blank=True)
    gas_price_adj = models.FloatField(null=True, blank=True)
    oil_price_adj = models.FloatField(null=True, blank=True)
    ngl_price_adj = models.FloatField(null=True, blank=True)
    realized_gas_price = models.FloatField(null=True, blank=True)
    realized_oil_price = models.FloatField(null=True, blank=True)
    realized_ngl_price = models.FloatField(null=True, blank=True)
    net_gas_rev = models.FloatField(null=True, blank=True)
    net_oil_rev = models.FloatField(null=True, blank=True)
    net_ngl_rev = models.FloatField(null=True, blank=True)
    net_total_rev = models.FloatField(null=True, blank=True)
    gross_drill_capex = models.FloatField(null=True, blank=True)
    gross_compl_capex = models.FloatField(null=True, blank=True)
    gross_misc_capex = models.FloatField(null=True, blank=True)
    gross_aban_capex = models.FloatField(null=True, blank=True)
    gross_total_capex = models.FloatField(null=True, blank=True)
    net_drill_capex = models.FloatField(null=True, blank=True)
    net_compl_capex = models.FloatField(null=True, blank=True)
    net_misc_capex = models.FloatField(null=True, blank=True)
    net_aban_capex = models.FloatField(null=True, blank=True)
    net_total_capex = models.FloatField(null=True, blank=True)
    fixed_cost = models.FloatField(null=True, blank=True)
    alloc_fixed_cost = models.FloatField(null=True, blank=True)
    var_gas_cost = models.FloatField(null=True, blank=True)
    var_oil_cost = models.FloatField(null=True, blank=True)
    var_water_cost = models.FloatField(null=True, blank=True)
    doe = models.FloatField(null=True, blank=True)
    gtp = models.FloatField(null=True, blank=True)
    taxes = models.FloatField(null=True, blank=True)
    loe = models.FloatField(null=True, blank=True)
    cf = models.FloatField(null=True, blank=True)
    fcf = models.FloatField(null=True, blank=True)
    pv1 = models.FloatField(null=True, blank=True)
    pv1_rate = models.FloatField(null=True, blank=True)
    pv2 = models.FloatField(null=True, blank=True)
    pv2_rate = models.FloatField(null=True, blank=True)
    pv3 = models.FloatField(null=True, blank=True)
    pv3_rate = models.FloatField(null=True, blank=True)
    pv4 = models.FloatField(null=True, blank=True)
    pv4_rate = models.FloatField(null=True, blank=True)
    pv5 = models.FloatField(null=True, blank=True)
    pv5_rate = models.FloatField(null=True, blank=True)
    pv6 = models.FloatField(null=True, blank=True)
    pv6_rate = models.FloatField(null=True, blank=True)
    pv7 = models.FloatField(null=True, blank=True)
    pv7_rate = models.FloatField(null=True, blank=True)
    pv8 = models.FloatField(null=True, blank=True)
    pv8_rate = models.FloatField(null=True, blank=True)
    pv9 = models.FloatField(null=True, blank=True)
    pv9_rate = models.FloatField(null=True, blank=True)
    pv10 = models.FloatField(null=True, blank=True)
    pv10_rate = models.FloatField(null=True, blank=True)
    owner = models.CharField(max_length=255, null=True, blank=True)
    version = models.DateTimeField(null=True, blank=True)
    simulation = models.IntegerField(null=True, blank=True)

    class Meta:
        unique_together = (('scenario', 'type', 'idp', 'owner', 'version', 'simulation'),)


class output_log(models.Model):
    output_scenario = models.CharField(max_length=100)
    output_owner = models.CharField(max_length=255)
    output_version = models.DateTimeField()
    output_type = models.CharField(choices=[('deterministic', 'deterministic'), ('probabilistic', 'probabilistic')], max_length=25)
    scenario = models.CharField(max_length=100)
    scenario_owner = models.CharField(max_length=255)
    scenario_version = models.DateTimeField()

    class Meta:
        unique_together = (('output_scenario', 'output_owner', 'output_version', 'output_type'),)