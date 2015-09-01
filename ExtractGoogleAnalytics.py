from __future__ import print_function

import luigi, get_ga
import MySQLdb
import time, argparse, csv
import json, datetime, time
# Important to note - luigi does not like parameters with underscores _ 

class ExtractGoogleAnalytics(luigi.Task):
    viewid = luigi.Parameter()
    email = luigi.Parameter()
    p12file = luigi.Parameter()

    one_day_ago = datetime.date.today() - datetime.timedelta(days=1)
    two_days_ago = datetime.date.today() - datetime.timedelta(days=2)

    start = luigi.Parameter(default=two_days_ago)
    end = luigi.Parameter(default=one_day_ago)

    def run(self):        
        file_location = 'p12/' + str(self.p12file)
        scope = ['https://www.googleapis.com/auth/analytics.readonly']
        service = get_ga.get_service('analytics', 'v3', scope, file_location, self.email)
        
        print(self.start)
        print('start date string is ' + str(self.start))
        ga_query = service.data().ga().get(
          ids='ga:' + self.viewid,
          start_date=str(self.start), 
          end_date=str(self.end), 
          metrics=str(self.metrics),
          dimensions=str(self.dimensions),
          sort=str(self.sort),
          start_index='1').execute()
        
        writer = csv.writer(open('dict.csv', 'wb'))
        for rows in ga_query:
            if rows == "rows":
                with open("output.csv", "wb") as f:
                    writer = csv.writer(f)
                    writer.writerows(ga_query[rows])
        f.close()

    def output(self):
        temp_name = 'temp_%s_%s.csv' % (self.viewid, unicode(time.strftime('%Y%m%d')))
        return luigi.LocalTarget(temp_name)

if __name__ == '__main__':
    luigi.run()