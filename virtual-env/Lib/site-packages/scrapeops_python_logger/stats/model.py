
class BaseStatsModel(object):

    def __init__(self):
        pass

    def get_value(self, stats, key, default=None):
        return stats.get(key, default)

    def set_value(self, stats, key, value):
        stats[key] = value

    def inc_value(self, stats, key, count=1, start=0, spider=None):
        d = stats
        d[key] = d.setdefault(key, start) + count

    def max_value(self, stats, key, value, spider=None):
        stats[key] = max(stats.setdefault(key, value), value)

    def min_value(self, stats, key, value, spider=None):
        stats[key] = min(stats.setdefault(key, value), value)

    def print_stats(self, statsType, stats):
        print(f'#### SCRAPEOPS {statsType.upper()} STATS ####')
        print('{')
        for key, value in stats.items():
            if key[0] != '_':
                print(f"    '{key}': {value},")
        print('}')


class PeriodicStatsModel(BaseStatsModel):
    _periodic_stats = {}
    _periodic_errors = 0
    _periodic_warnings = 0
    _periodic_criticals = 0

    def get_periodic_stats(self):
        return self._periodic_stats

    def reset_periodic_stats(self):
        self._periodic_stats = {}

    def display_periodic_stats(self):
        stats = self.get_periodic_stats()
        self.print_stats('periodic', stats)


class OverallStatsModel(BaseStatsModel):
    _overall_stats = {}
    _overall_errors = 0
    _overall_warnings = 0
    _overall_criticals = 0

    def get_overall_stats(self):
        return self._overall_stats

    def display_overall_stats(self):
        stats = self.get_overall_stats()
        self.print_stats('overall', stats)

