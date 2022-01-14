from django.contrib import admin
from olive.models import scenarios, properties, forecasts, properties, frameworks, economics, projects

admin.site.register(scenarios)
admin.site.register(properties)
admin.site.register(forecasts)
admin.site.register(frameworks)
admin.site.register(economics)
admin.site.register(projects)